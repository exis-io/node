package rabric

import (
	"fmt"
	"sync"
	"time"
)

type Node interface {
	Accept(Peer) error
	Close() error
	RegisterRealm(URI, Realm) error
	GetLocalPeer(URI, map[string]interface{}) (Peer, error)
	AddSessionOpenCallback(func(uint, string))
	AddSessionCloseCallback(func(uint, string))
	cb([]interface{}, map[string]interface{})
}

type node struct {
	closing               bool
	closeLock             sync.Mutex
	sessionOpenCallbacks  []func(uint, string)
	sessionCloseCallbacks []func(uint, string)
	realm                 Realm
	agent                 *Client
	// sessionPdid           map[string]string
	// nodes                 map[string]Session
	// forwarding            map[string]Session
	// permissions           map[string]string
	// agent                 *Client
}

// NewDefaultNode creates a very basic WAMP Node.
func NewNode() Node {
	node := &node{
		sessionOpenCallbacks:  []func(uint, string){},
		sessionCloseCallbacks: []func(uint, string){},
	}

	// Provisioning: this Node needs a name
	// Unhandled case: what to do with Nodes that start with nothing?
	// They still have to create a peer (self) and ask for an identity

	// Here we assume *one* Node as root and a default pd namespace
	// Node should identify itself based on its root certificate, for now
	// just set a constant name
	realm := Realm{URI: "pd"}
	realm.init()

	// Single realm handles all pubs and subs
	node.realm = realm
	node.agent = node.localClient("pd")

	// Subscribe to meta-level events here
	return node
}

// Testing method to ensure that structs can be receivers for Riffle traffic
func (n *node) cb(args []interface{}, kwargs map[string]interface{}) {
	out.Notice("Pub on local session!")
}

func (n *node) localClient(s string) *Client {
	p := n.getTestPeer()

	// p, _ := n.GetLocalPeer(URI(s), nil)

	client := NewClient(p)
	client.ReceiveTimeout = 1000 * time.Millisecond
	if _, err := client.JoinRealm(s, nil); err != nil {
		out.Error("Error when creating new client: ", err)
	}

	client.pdid = URI(s)

	return client
}

func (r *node) AddSessionOpenCallback(fn func(uint, string)) {
	r.sessionOpenCallbacks = append(r.sessionOpenCallbacks, fn)
}

func (r *node) AddSessionCloseCallback(fn func(uint, string)) {
	r.sessionCloseCallbacks = append(r.sessionCloseCallbacks, fn)
}

func (r *node) Close() error {
	r.closeLock.Lock()

	if r.closing {
		r.closeLock.Unlock()
		return fmt.Errorf("already closed")
	}

	r.closing = true
	r.closeLock.Unlock()
	r.realm.Close()

	return nil
}

// Shouldn't be called anymore
func (r *node) RegisterRealm(uri URI, realm Realm) error {
	return nil
}

func (r *node) Accept(client Peer) error {
	sess, ok := r.Handshake(client)

	if ok != nil {
		return ok
	}

	// Start listening on the session
	// This will eventually move to the session
	go Listen(r, sess)

	return nil
}

////////////////////////////////////////
// New Content
////////////////////////////////////////

// Spin on a session, wait for messages to arrive. Method does not return
// until session closes
// NOTE: realm and details are OLD CODE and should not be construed as permanent fixtures
func Listen(node *node, sess Session) {
	c := sess.Receive()

	for {
		var open bool
		var msg Message

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

		node.Handle(&msg, &sess)
	}
}

////////////////////////////////////////
// Very new code
////////////////////////////////////////

// Handle a new Peer, creating and returning a session
func (n *node) Handshake(client Peer) (Session, error) {
	sess := Session{}

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

	// Old implementation: the authentication must occur before fetching the realm
	welcome, err := n.realm.handleAuth(client, hello.Details)

	// Check to make sure PDID is not already registered

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

	out.Notice("Session open: [%s]", string(hello.Realm))

	sess = Session{Peer: client, Id: welcome.Id, pdid: hello.Realm, kill: make(chan URI, 1)}
	return sess, nil
}

// Called when a session is closed or closes itself
func (n *node) SessionClose(sess Session) {
	sess.Close()
	out.Notice("Session close: [%s]", sess)

	// Did these really not exist before? Doesn't seem likely, but can't find them
	n.realm.Dealer.lostSession(sess)
	n.realm.Broker.lostSession(sess)

	// Meta level events
	// for _, callback := range n.sessionCloseCallbacks {
	//     go callback(uint(sess.Id), string(hello.Realm))
	// }
}

// Handle a new message
func (n *node) Handle(msg *Message, sess *Session) {
	// NOTE: there is a serious shortcoming here: How do we deal with WAMP messages with an
	// implicit destination? Many of them refer to sessions, but do we want to store the session
	// IDs with the ultimate PDID target, or just change the protocol?

	// Make errors nice and pretty. These are riffle error messages, not node errors
	m := *msg
	if m.MessageType() == ERROR {
		out.Warning("[%s] %s: %+v", *sess, m.MessageType(), *msg)
	} else {
		out.Debug("[%s] %s: %+v", *sess, m.MessageType(), *msg)
	}

	// Extract the target domain from the message
	if uri, ok := destination(msg); ok == nil {
		// Ensure the construction of the message is valid, extract the endpoint, domain, and action
		_, _, err := breakdownEndpoint(string(uri))

		// Return a WAMP error to the user indicating a poorly constructed endpoint
		if err != nil {
			out.Error("Misconstructed endpoint: %s", msg)
			m := *msg
			err := &Error{
				Type:    m.MessageType(),
				Request: sess.Id,
				Details: map[string]interface{}{"Invalid Endpoint": "Poorly constructed endpoint."},
				Error:   ErrInvalidUri,
			}

			sess.Peer.Send(err)
			return
		}

		// Downward domain action? That is, endpoint is a subdomain of the current agent?
		if !n.Permitted(uri, sess) {
			out.Warning("Action not allowed: %s:%s", sess.pdid, uri)
			return
		}

	} else {
		out.Debug("Unable to determine destination from message: %+v", *msg)
		// n.realm.handleMessage(*msg, *sess)
	}

	n.realm.handleMessage(*msg, *sess)
}

// Return true or false based on the message and the session which sent the message
func (n *node) Permitted(endpoint URI, sess *Session) bool {
	// TODO: allow all core appliances to perform whatever they want
	if sess.pdid == "pd.bouncer" || sess.pdid == "pd.map" || sess.pdid == "pd.auth" {
		return true
	}

	// The node is always permitted to perform any action
	if sess.pdid == n.agent.pdid {
		return true
	}

	// Is downward action? allow
	// return true

	// Check permissions cache: if found, allow

	// Check with bouncer on permissions check
	if bouncerActive := n.realm.hasRegistration("pd.bouncer/checkPerm"); bouncerActive {
		args := []interface{}{string(sess.pdid), string(endpoint)}
		// args := []string{"a", "b", "c", "d"}

		ret, err := n.agent.Call("pd.bouncer/checkPerm", args, nil)

		if err != nil {
			out.Critical("Error, returning true: %s", err)
			return true
		}

		if permitted, ok := ret.Arguments[0].(bool); ok {
			// out.Debug("Bouncer returning %s", permitted)
			// TODO: save a permitted action in some flavor of cache
			return permitted
		} else {
			out.Critical("Could not extract permission from return val. Bouncer called and returnd: %s", ret.Arguments)
			return true
		}
	} else {
		out.Warning("No bouncer registered!")
	}

	// Action is not permitted
	return false
}

// returns the pdid of the next hop on the path for the given message
func (n *node) Route(msg *Message) string {
	// Is target a tenant?
	// Is target in forwarding tables?
	// Ask map for next hop

	return ""
}

// Returns true if core appliances connected
func (n *node) CoreReady() bool {
	out.Warning("Core status: ", n.realm)
	return true
}

// GetLocalPeer returns an internal peer connected to the specified realm.
func (r *node) GetLocalPeer(realmURI URI, details map[string]interface{}) (Peer, error) {
	peerA, peerB := localPipe()
	sess := Session{Peer: peerA, Id: NewID(), kill: make(chan URI, 1)}
	out.Notice("Established internal session:", sess.Id)

	// TODO: session open/close callbacks?
	if details == nil {
		details = make(map[string]interface{})
	}

	// go r.realm.handleSession(sess, details)
	go Listen(r, sess)
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

type RealmExistsError string

func (e RealmExistsError) Error() string {
	return "realm exists: " + string(e)
}

type NoSuchRealmError string

func (e NoSuchRealmError) Error() string {
	return "no such realm: " + string(e)
}

type AuthenticationError string

func (e AuthenticationError) Error() string {
	return "authentication error: " + string(e)
}

type InvalidURIError string

func (e InvalidURIError) Error() string {
	return "invalid URI: " + string(e)
}
