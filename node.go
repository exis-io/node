package node

// Consider for data structures: http://arslan.io/thread-safe-set-data-structure-in-go

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Node interface {
	Accept(Peer) error
	Listen(*Session)
	Close() error
	GetLocalPeer(URI, map[string]interface{}) (Peer, error)
}

type node struct {
	closing   bool
	closeLock sync.Mutex
	Authen
	Broker
	Dealer
	Agent
	agent       *Client
	sessions    map[string]Session
	sessionLock sync.RWMutex
	stats       *NodeStats
	PermMode    string
	Config      *NodeConfig
}

// NewDefaultNode creates a very basic WAMP Node.
func NewNode(config *NodeConfig) Node {
	node := &node{
		sessions: make(map[string]Session, 0),
		Broker:   NewDefaultBroker(),
		Dealer:   NewDefaultDealer(),
		Agent:    NewAgent(),
		stats:    NewNodeStats(),
		PermMode: os.Getenv("EXIS_PERMISSIONS"),
		Config:   config,
	}

	// Open a file for logging messages.
	// Note: this must come before we set up the local agent.
	if config.MessageLogFile != "" {
		node.stats.OpenMessageLog(config.MessageLogFile, config.MessageLogMaxLines)
	}

	// For the startup phase, we will hold calls without a registered procedure.
	if config.HoldCalls > 0 {
		go func() {
			time.Sleep(time.Duration(config.HoldCalls) * time.Second)
			node.Dealer.ClearBlockedCalls()
		}()
	}

	node.agent = node.localClient(config.Agent)
	node.Authen = NewAuthen(node)

	node.RegisterNodeMethods()

	return node
}

func (node *node) Close() error {
	node.closeLock.Lock()

	if node.closing {
		node.closeLock.Unlock()
		return fmt.Errorf("already closed")
	}

	node.closing = true
	node.closeLock.Unlock()

	// Tell all sessions wer're going down
	// sessions must be locked before access, read is ok here
	node.sessionLock.RLock()
	for _, s := range node.sessions {
		s.kill <- ErrSystemShutdown
	}
	node.sessionLock.RUnlock()

	// Clear the map (might not be needed)
	node.sessionLock.Lock()
	node.sessions = make(map[string]Session, 0)
	node.sessionLock.Unlock()

	return nil
}

func (node *node) Accept(client Peer) error {
	sess, ok := node.Handshake(client)

	node.stats.LogEvent("SessionAccept")

	if ok != nil {
		return ok
	}

	// Start listening on the session
	// This will eventually move to the session
	go node.Listen(&sess)

	return nil
}

// Spin on a session, wait for messages to arrive. Method does not return
func (node *node) Listen(sess *Session) {
	c := sess.Receive()

	node.SendJoinNotification(sess)

	limit := node.Config.GetRequestLimit(sess.authid)
	limiter := NewBasicLimiter(limit)
	out.Debug("Request rate limit for %s: %d/s", sess, limit)

	for {
		var open bool
		var msg Message

		limiter.Acquire()

		select {
		case msg, open = <-c:
			if !open {
				//log.Println("lost session:", sess)

				node.SessionClose(sess)
				return
			}

		case reason := <-sess.kill:
			logErr(sess.Send(&Goodbye{Reason: reason, Details: make(map[string]interface{})}))
			//log.Printf("kill session %s: %v", sess, reason)

			//NEW: Exit the session!
			node.SessionClose(sess)
			return
		}

		node.Handle(&msg, sess)
	}
}

// Handle a new Peer, creating and returning a session
func (n *node) Handshake(client Peer) (Session, error) {
	handled := NewHandledMessage("Hello")

	sess := Session{
		Peer: client,
		messageCounts: make(map[string]int64),
		kill: make(chan URI, 1),
	}

	// Dont accept new sessions if the node is going down
	if n.closing {
		logErr(client.Send(&Abort{Reason: ErrSystemShutdown}))
		logErr(client.Close())
		return sess, fmt.Errorf("Node is closing, no new connections are allowed")
	}

	msg, err := GetMessageTimeout(client, 5*time.Second)
	if err != nil {
		return sess, err
	}

	hello, msgOk := msg.(*Hello)

	// Ensure the message is valid and well constructed
	if !msgOk {
		logErr(client.Send(&Abort{Reason: URI("wamp.error.protocol_violation")}))
		logErr(client.Close())

		return sess, fmt.Errorf("protocol violation: expected HELLO, received %s", msg.MessageType())
	}

	sess.pdid = hello.Realm
	sess.authid = string(hello.Realm)

	// Old implementation: the authentication must occur before fetching the realm
	welcome, err := n.Authen.handleAuth(&sess, hello)

	if err != nil {
		abort := &Abort{
			Reason:  ErrAuthorizationFailed, // TODO: should this be AuthenticationFailed?
			Details: map[string]interface{}{"error": err.Error()},
		}

		logErr(client.Send(abort))
		logErr(client.Close())
		return sess, AuthenticationError(err.Error())
	}

	welcome.Id = NewID()

	if welcome.Details == nil {
		welcome.Details = make(map[string]interface{})
	}

	// add default details to welcome message
	for k, v := range defaultWelcomeDetails {
		if _, ok := welcome.Details[k]; !ok {
			welcome.Details[k] = v
		}
	}

	if err := client.Send(welcome); err != nil {
		return sess, err
	}

	out.Notice("Session open: %s", string(hello.Realm))
	sess.Id = welcome.Id
	n.sessionLock.Lock()
	n.sessions[string(hello.Realm)] = sess
	n.sessionLock.Unlock()

	// Note: we are ignoring the CR exchange and just logging it as a
	// Hello-Welcome exchange.
	effect := NewMessageEffect("", "Welcome", sess.Id)
	n.stats.LogMessage(&sess, handled, effect)

	return sess, nil
}

// Called when a session is closed or closes itself
func (n *node) SessionClose(sess *Session) {
	sess.Close()
	out.Notice("Session close: %s", sess)

	n.Dealer.lostSession(sess)
	n.Broker.lostSession(sess)

	n.stats.LogEvent("SessionClose")

	n.SendLeaveNotification(sess)

	n.sessionLock.Lock()
	delete(n.sessions, string(sess.pdid))
	n.sessionLock.Unlock()

	// We log a special _Close message in case there was no Goodbye message
	// associated with this session closing.
	handled := NewHandledMessage("_Close")
	effect := NewMessageEffect("", "", sess.Id)
	n.stats.LogMessage(sess, handled, effect)
}

// Publish a notification that a session joined.
// If "xs.a.b" joins, the message is published to "x.a/sessionJoined".
func (n *node) SendJoinNotification(sess *Session) {
	args := []interface{}{}
	kwargs := map[string]interface{}{
		"id": sess.Id,
		"agent": string(sess.pdid),
	}

	endpoint := popDomain(string(sess.pdid)) + "/sessionJoined"

	// Note: we are not using the agent to publish these messages because the
	// agent itself triggers a sessionJoined message.
	msg := &Publish{
		Request: NewID(),
		Topic: URI(endpoint),
		Arguments: args,
		ArgumentsKw: kwargs,
	}
	n.Broker.Publish(nil, msg)
}

// Publish a notification that a session left.
// If "xs.a.b" leaves, the message is published to "x.a/sessionLeft".
func (n *node) SendLeaveNotification(sess *Session) {
	args := []interface{}{
        string(sess.pdid),
    }
    
	kwargs := map[string]interface{}{
		"id": sess.Id,
		"agent": string(sess.pdid),
	}

	endpoint := popDomain(string(sess.pdid)) + "/sessionLeft"

	msg := &Publish{
		Request: NewID(),
		Topic: URI(endpoint),
		Arguments: args,
		ArgumentsKw: kwargs,
	}
	n.Broker.Publish(nil, msg)
}

func (n *node) LogMessage(msg *Message, sess *Session) {
	// Extract the target domain from the message
	target, err := destination(msg)

	// Make errors nice and pretty. These are riffle error messages, not node errors
	m := *msg
	if m.MessageType() == ERROR {
		out.Warning("%s from %s: %v", m.MessageType(), *sess, *msg)
	} else if err == nil {
		out.Debug("%s %s from %s", m.MessageType(), string(target), *sess)
	} else {
		out.Debug("%s from %s", m.MessageType(), *sess)
	}

	typeName := messageTypeString(*msg)
	n.stats.LogEvent(typeName)
	sess.messageCounts[typeName]++
}

// Handle a new message
func (n *node) Handle(msg *Message, sess *Session) {
	// NOTE: there is a serious shortcoming here: How do we deal with WAMP messages with an
	// implicit destination? Many of them refer to sessions, but do we want to store the session
	// IDs with the ultimate PDID target, or just change the protocol?

	handled := NewHandledMessage(messageTypeString(*msg))
	var effect *MessageEffect

	n.LogMessage(msg, sess)


	// Extract the target domain from the message
	target, err := destination(msg)
	if err == nil {
		// Ensure the construction of the message is valid, extract the endpoint, domain, and action
		_, _, err := breakdownEndpoint(string(target))

		// Return a WAMP error to the user indicating a poorly constructed endpoint
		if err != nil {
			out.Error("Misconstructed endpoint: %s", msg)
			m := *msg

			err := &Error{
				Type:    m.MessageType(),
				Request: requestID(msg),
				Details: map[string]interface{}{"Invalid Endpoint": "Poorly constructed endpoint."},
				Error:   ErrInvalidUri,
			}

			sess.Peer.Send(err)

			effect = NewErrorMessageEffect("", ErrInvalidUri, 0)
			n.stats.LogMessage(sess, handled, effect)

			return
		}

		verb, ok := GetMessageVerb(*msg)

		// Downward domain action? That is, endpoint is a subdomain of the current agent?
		if !ok || !n.Permitted(target, sess, verb) {
			out.Warning("Action not allowed: %s:%s", sess.pdid, target)

			m := *msg
			err := &Error{
				Type:    m.MessageType(),
				Request: requestID(msg),
				Details: map[string]interface{}{"Not Permitted": "Action not permitted."},
				Error:   ErrNotAuthorized,
			}

			sess.Peer.Send(err)

			effect = NewErrorMessageEffect("", ErrNotAuthorized, 0)
			n.stats.LogMessage(sess, handled, effect)

			return
		}
	}


	switch msg := (*msg).(type) {
	case *Goodbye:
		logErr(sess.Send(&Goodbye{Reason: ErrGoodbyeAndOut, Details: make(map[string]interface{})}))
		effect = NewMessageEffect("", "Goodbye", sess.Id)
		// log.Printf("[%s] leaving: %v", sess, msg.Reason)

	// Broker messages
	case *Publish:
		effect = n.Broker.Publish(sess, msg)
	case *Subscribe:
		effect = n.Broker.Subscribe(sess, msg)
	case *Unsubscribe:
		effect = n.Broker.Unsubscribe(sess, msg)

	// Dealer messages
	case *Register:
		effect = n.Dealer.Register(sess, msg)
	case *Unregister:
		effect = n.Dealer.Unregister(sess, msg)
	case *Call:
		effect = n.Dealer.Call(sess, msg)
	case *Yield:
		effect = n.Dealer.Yield(sess, msg)

	// Error messages
	case *Error:
		if msg.Type == INVOCATION {
			// the only type of ERROR message the Node should receive
			effect = n.Dealer.Error(sess, msg)
		} else {
			out.Critical("invalid ERROR message received: %v", msg)
		}

	default:
		out.Critical("Unhandled message:", msg.MessageType())
	}

	// effect is nil in the case of messages we don't know how to handle.
	if effect != nil {
		n.stats.LogMessage(sess, handled, effect)
	}
}

// Return true or false based on the message and the session which sent the message
func (n *node) Permitted(endpoint URI, sess *Session, verb string) bool {
	// Permissions checking is turned off---only for testing, please!
	if n.PermMode == "off" {
		return true
	}

	// The node is always permitted to perform any action
	if sess.isLocal() {
		return true
	}

	// Always allow downward actions.
	if subdomain(string(sess.authid), string(endpoint)) {
		return true
	}

	// Look up auth level of receiver.  The action will not be permitted if the
	// receiver is more strongly authenticated than the caller.
	//
	// This code is only for testing interaction with authenticated agents
	// without breaking unauthenticated agents.
	//
	// TODO: Remove this code when all agents are authenticated.
	targetDomain, _ := extractDomain(string(endpoint))
	n.sessionLock.RLock()
	targetSession, ok := n.sessions[targetDomain]
	n.sessionLock.RUnlock()
	if ok && targetSession.authLevel > sess.authLevel {
		out.Warning("Communication with authenticated agent %s not permitted", targetSession.pdid)
		return false
	}

	// TODO Check permissions cache: if found, allow
	// TODO: save a permitted action in some flavor of cache

	return n.AskBouncer(string(sess.authid), string(endpoint), verb)

	// No bouncer approved it.
	return false
}

func (n *node) AskBouncer(authid string, target string, verb string) bool {
	// Check with bouncer(s) on permissions check.
	// At least one bouncer needs to approve a non-downward action.

	if n.Config.Bouncer == "" {
		return false
	}

	checkPerm := n.Config.Bouncer + "/checkPerm"

	bouncerActive := n.Dealer.hasRegistration(URI(checkPerm))
	if !bouncerActive {
		out.Warning("Bouncer (%s) not registered", checkPerm)
		return false
	}

	args := []interface{}{authid, target, verb}
	ret, err := n.agent.Call(checkPerm, args, nil)
	if err != nil {
		out.Critical("Error, returning false: %s", err)
		return false
	}

	permitted, ok := ret.Arguments[0].(bool)
	return ok && permitted
}

// returns the pdid of the next hop on the path for the given message
func (n *node) Route(msg *Message) string {
	// Is target a tenant?
	// Is target in forwarding tables?
	// Ask map for next hop

	return ""
}

func (node *node) EvictDomain(domain string) int {
	count := 0

	node.sessionLock.Lock()
	defer node.sessionLock.Unlock()

	for dom, sess := range node.sessions {
		if subdomain(domain, dom) {
			sess.kill <- ErrSessionEvicted
			count++
		}
	}

	return count
}

// GetLocalPeer returns an internal peer connected to the specified realm.
func (r *node) GetLocalPeer(realmURI URI, details map[string]interface{}) (Peer, error) {
	peerA, peerB := localPipe()
	sess := Session{Peer: peerA, Id: NewID(), kill: make(chan URI, 1)}
	out.Notice("Established internal session:", sess.Id)

	if details == nil {
		details = make(map[string]interface{})
	}

	go r.Listen(&sess)
	return peerB, nil
}

func (r *node) getTestPeer() Peer {
	peerA, peerB := localPipe()
	go r.Accept(peerA)
	return peerB
}

var defaultWelcomeDetails = map[string]interface{}{
	"roles": map[string]struct{}{
		"broker": {},
		"dealer": {},
	},
}

////////////////////////////////////////
// Misc and old
////////////////////////////////////////

func (n *node) localClient(s string) *Client {
	p := n.getTestPeer()

	client := NewClient(p)
	client.ReceiveTimeout = 1000 * time.Millisecond
	if _, err := client.JoinRealm(s, nil); err != nil {
		out.Error("Error when creating new client: ", err)
	}

	client.pdid = URI(s)

	return client
}
