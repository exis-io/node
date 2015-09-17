package rabric

import (
	"fmt"
	"time"
)

const (
	defaultAuthTimeout = 2 * time.Minute
)

type Realm struct {
	_   string
	URI URI
	Broker
	Dealer
	CRAuthenticators map[string]CRAuthenticator
	Authenticators   map[string]Authenticator
	AuthTimeout      time.Duration
	clients          map[string]Session
}

func (r *Realm) init() {
	r.clients = make(map[string]Session)
	if r.Broker == nil {
		r.Broker = NewDefaultBroker()
	}
	if r.Dealer == nil {
		r.Dealer = NewDefaultDealer()
	}
	if r.AuthTimeout == 0 {
		r.AuthTimeout = defaultAuthTimeout
	}
}

func (r Realm) Close() {
	for _, client := range r.clients {
		client.kill <- ErrSystemShutdown
	}
}

func (r *Realm) handleAuth(client Peer, details map[string]interface{}) (*Welcome, error) {
	msg, err := r.authenticate(details)

	if err != nil {
		return nil, err
	}
	// we should never get anything besides WELCOME and CHALLENGE
	if msg.MessageType() == WELCOME {
		return msg.(*Welcome), nil
	} else {
		// Challenge response
		challenge := msg.(*Challenge)
		if err := client.Send(challenge); err != nil {
			return nil, err
		}

		msg, err := GetMessageTimeout(client, r.AuthTimeout)
		if err != nil {
			return nil, err
		}
		//log.Printf("%s: %+v", msg.MessageType(), msg)
		if authenticate, ok := msg.(*Authenticate); !ok {
			return nil, fmt.Errorf("unexpected %s message received", msg.MessageType())
		} else {
			return r.checkResponse(challenge, authenticate)
		}
	}
}

// Authenticate either authenticates a client or returns a challenge message if
// challenge/response authentication is to be used.
func (r Realm) authenticate(details map[string]interface{}) (Message, error) {
	// pprint the incoming details

	// if b, err := json.MarshalIndent(details, "", "  "); err != nil {
	// 	fmt.Println("error:", err)
	// } else {
	// 	//log.Printf(string(b))
	// }

	// //log.Println("details:", details)

	if len(r.Authenticators) == 0 && len(r.CRAuthenticators) == 0 {
		return &Welcome{}, nil
	}

	// TODO: this might not always be a []interface{}. Using the JSON unmarshaller it will be,
	// but we may have serializations that preserve more of the original type.
	// For now, the tests just explicitly send a []interface{}
	_authmethods, ok := details["authmethods"].([]interface{})

	if !ok {
		return nil, fmt.Errorf("No authentication supplied")
	}

	authmethods := []string{}

	for _, method := range _authmethods {
		if m, ok := method.(string); ok {
			authmethods = append(authmethods, m)
		} else {
			//log.Printf("invalid authmethod value: %v", method)
		}
	}

	for _, method := range authmethods {
		if auth, ok := r.CRAuthenticators[method]; ok {
			if challenge, err := auth.Challenge(details); err != nil {
				return nil, err
			} else {
				return &Challenge{AuthMethod: method, Extra: challenge}, nil
			}
		}
		if auth, ok := r.Authenticators[method]; ok {
			if authDetails, err := auth.Authenticate(details); err != nil {
				return nil, err
			} else {
				return &Welcome{Details: addAuthMethod(authDetails, method)}, nil
			}
		}
	}

	// TODO: check default auth (special '*' auth?)
	return nil, fmt.Errorf("could not authenticate with any method")
}

// checkResponse determines whether the response to the challenge is sufficient to gain access to the Realm.
func (r Realm) checkResponse(challenge *Challenge, authenticate *Authenticate) (*Welcome, error) {
	if auth, ok := r.CRAuthenticators[challenge.AuthMethod]; !ok {
		return nil, fmt.Errorf("authentication method has been removed")
	} else {
		if details, err := auth.Authenticate(challenge.Extra, authenticate.Signature); err != nil {
			return nil, err
		} else {
			return &Welcome{Details: addAuthMethod(details, challenge.AuthMethod)}, nil
		}
	}
}

func addAuthMethod(details map[string]interface{}, method string) map[string]interface{} {
	if details == nil {
		details = make(map[string]interface{})
	}
	details["authmethod"] = method
	return details
}

func (r *Realm) handleMessage(msg Message, sess Session) {

	switch msg := msg.(type) {
	case *Goodbye:
		logErr(sess.Send(&Goodbye{Reason: ErrGoodbyeAndOut, Details: make(map[string]interface{})}))
		//log.Printf("[%s] leaving: %v", sess, msg.Reason)
		return

	// Broker messages
	case *Publish:
		r.Broker.Publish(sess.Peer, msg)
	case *Subscribe:
		r.Broker.Subscribe(sess.Peer, msg)
	case *Unsubscribe:
		r.Broker.Unsubscribe(sess.Peer, msg)

	// Dealer messages
	case *Register:
		r.Dealer.Register(sess.Peer, msg)
	case *Unregister:
		r.Dealer.Unregister(sess.Peer, msg)
	case *Call:
		r.Dealer.Call(sess.Peer, msg)
	case *Yield:
		r.Dealer.Yield(sess.Peer, msg)

	// Error messages
	case *Error:
		if msg.Type == INVOCATION {
			// the only type of ERROR message the Node should receive
			r.Dealer.Error(sess.Peer, msg)
		} else {
			out.Critical("invalid ERROR message received: %v", msg)
		}

	default:
		out.Critical("Unhandled message:", msg.MessageType())
	}
}

// Dump the contents of the realm
func (r *Realm) dump() string {
	ret := "\nBROKER:\n" + r.Broker.dump()
	ret += "\nDEALER:\n" + r.Dealer.dump()
	return ret
}
