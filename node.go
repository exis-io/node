package rabric

import (
	"fmt"
	"sync"
	"time"
)

type node struct {
	closing               bool
	closeLock             sync.Mutex
	sessionOpenCallbacks  []func(uint, string)
	sessionCloseCallbacks []func(uint, string)
	realm                 Realm
	sessionPdid           map[string]string
	nodes                 map[string]Session
	forwarding            map[string]Session
	permissions           map[string]string
	agent                 *Client
}

// NewDefaultRouter creates a very basic WAMP router.
func NewNode() Router {
	node := &node{
		sessionOpenCallbacks:  []func(uint, string){},
		sessionCloseCallbacks: []func(uint, string){},
	}

	// Provisioning: this router needs a name
	// Unhandled case: what to do with routers that start with nothing?
	// They still have to create a peer (self) and ask for an identity

	// Here we assume *one* router as root and a default pd namespace
	realm := Realm{URI: "pd"}
	realm.init()

	// node.realms["pd"] = realm

	// Experimental single realm testing--- since we're handling the
	// pubs and subs to begin with
	node.realm = realm

	// peer, ok := node.GetLocalPeer("pd", nil)

	// if peer, ok := node.GetLocalPeer("pd", nil); ok != nil {
	//     //log.Println("Unable to create local session: ", ok)
	// } else {
	//     node.agent = peer
	// }

	node.agent = node.localClient("pd")

	h := func(args []interface{}, kwargs map[string]interface{}) {
		out.Warning("Got a pub on the local session!")
	}

	// NOTE: this works, but looks like an error with the extraction and parsing code
	// when published on this endpoint.
	node.agent.Subscribe("pd/hello", h)

	// What does a provisioning process look like? Where does the router get its name?
	// what is OUR name?

	return node
}

func (n *node) localClient(s string) *Client {
	p := n.getTestPeer()

	// p, _ := n.GetLocalPeer(URI(s), nil)

	client := NewClient(p)
	client.ReceiveTimeout = 100 * time.Millisecond
	if _, err := client.JoinRealm(s, nil); err != nil {
		out.Error("Error when creating new client: ", err)
	}

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
	// for _, realm := range r.realms {
	// 	realm.Close()
	// }

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

	// sess := Session{Peer: client, Id: welcome.Id, pdid: hello.Realm, kill: make(chan URI, 1)}
	out.Notice("Established session: ", sess.pdid)

	// Meta level start events
	// for _, callback := range r.sessionOpenCallbacks {
	// 	go callback(uint(sess.Id), string(hello.Realm))
	// }

	// OLD CODE: need the original realm to handle issues with default
	// realm := r.getDomain(sess.pdid)

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

// Handle a new Peer
func (n *node) Handshake(client Peer) (Session, error) {
	sess := Session{}

	// Dont accept new sessions if the node is going down
	if n.closing {
		logErr(client.Send(&Abort{Reason: ErrSystemShutdown}))
		logErr(client.Close())
		return sess, fmt.Errorf("Router is closing, no new connections are allowed")
	}

	msg, err := GetMessageTimeout(client, 5*time.Second)
	if err != nil {
		return sess, err
	}

	hello, _ := msg.(*Hello)

	// Ensure the message is valid and well constructed
	if _, ok := msg.(*Hello); !ok {
		logErr(client.Send(&Abort{Reason: URI("wamp.error.protocol_violation")}))
		logErr(client.Close())

		return sess, fmt.Errorf("protocol violation: expected HELLO, received %s", msg.MessageType())
	}

	// get the appropriate domain
	// realm := n.getDomain(hello.Realm)
	// realm :=

	// Old implementation: the authentication must occur before fetching the realm
	welcome, err := n.realm.handleAuth(client, hello.Details)

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

	out.Notice("Established session: ", hello.Realm)
	// //log.Println("Established session: ", welcome.Id)

	sess = Session{Peer: client, Id: welcome.Id, pdid: hello.Realm, kill: make(chan URI, 1)}
	return sess, nil
}

// Called when a session is closed or closes itself
func (n *node) SessionClose(sess Session) {
	sess.Close()
	out.Info("Session %s closed", sess)

	// Did these really not exist before? Doesn't seem likely, but can't find them
	n.realm.Dealer.lostSession(sess)
	n.realm.Broker.lostSession(sess)

	// Meta level events
	// for _, callback := range n.sessionCloseCallbacks {
	//     go callback(uint(sess.Id), string(hello.Realm))
	// }

	// Check if any realms need to be closed
	// Check if any registrations or pubs need to be purged
}

// Handle a new message
func (n *node) Handle(msg *Message, sess *Session) {
	// NOTE: there is a serious shortcoming here: How do we deal with WAMP messages with an
	// implicit destination? Many of them refer to sessions, but do we want to store the session
	// IDs with the ultimate PDID target, or just change the protocol?

	m := *msg
	out.Debug("[%s] %s: %+v", *sess, m.MessageType(), *msg)

	if uri, ok := destination(msg); ok == nil {
		// Ensure the construction of the message is valid, extract the endpoint, domain, and action
		// domain, action, err := breakdownEndpoint(string(uri))
		_, action, err := breakdownEndpoint(string(uri))

		// Return a WAMP error to the user indicating a poorly constructed endpoint
		if err != nil {
			out.Error("Misconstructed endpoint. Dont know what to do now!")
			return
		}

		// out.Debug("Extracted: %s %s \n", domain, action)

		if !n.Permitted(msg, sess) {
			out.Error("Operation not permitted! TODO: return an error here!")
			return
		}

		// Testing
		if action == "/ping" {
			// out.Critical("Trying session lookup...")

			// Try and check if the given endpoint is registered.

			// For now, dump the realm
			s := n.realm.dump()
			out.Critical(s)

			exists := n.realm.hasSubscription("pd/pong")
			out.Critical("Subscription for pd/ping exists: ", exists)

			if exists {
				out.Critical("Sending blind pub on pd/pong")

				n.agent.Publish("pd/pong", nil, nil)
			}
		}

		// Delivery (deferred)
		// route = n.Route(msg)

		// n.CoreReady()

	} else {
		// //log.Printf("Unable to determine destination from message: %+v", *msg)
		out.Debug("Unable to determine destination from message: %+v", *msg)
		// n.realm.handleMessage(*msg, *sess)
	}

	n.realm.handleMessage(*msg, *sess)
}

// Return true or false based on the message and the session which sent the messate
func (n *node) Permitted(msg *Message, sess *Session) bool {
	// Is downward action? allow
	// Check permissions cache: if found, allow
	// Check with bouncer
	return true
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

////////////////////////////////////////
// Old code, not sure if still useful
////////////////////////////////////////

// GetLocalPeer returns an internal peer connected to the specified realm.
func (r *node) GetLocalPeer(realmURI URI, details map[string]interface{}) (Peer, error) {
	peerA, peerB := localPipe()
	sess := Session{Peer: peerA, Id: NewID(), kill: make(chan URI, 1)}
	out.Notice("Established internal session:", sess.Id)

	// TODO: session open/close callbacks?
	if details == nil {
		details = make(map[string]interface{})
	}

	go r.realm.handleSession(sess, details)
	return peerB, nil
}

func (r *node) getTestPeer() Peer {
	peerA, peerB := localPipe()
	go r.Accept(peerA)
	return peerB
}
