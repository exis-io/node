package node

import (
	"fmt"
	"time"
)

const (
	defaultAuthTimeout = 2 * time.Minute
)

// Holds stored certificates, contacts the auth appliance, etc
type Authen struct {
	CRAuthenticators map[string]CRAuthenticator
	Authenticators   map[string]Authenticator
	AuthTimeout      time.Duration
}

func NewAuthen() Authen {
	return Authen{
		AuthTimeout: defaultAuthTimeout,
	}
}

// Move to authn
func (r *Authen) handleAuth(client Peer, details map[string]interface{}) (*Welcome, error) {
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
func (r Authen) authenticate(details map[string]interface{}) (Message, error) {
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
func (r Authen) checkResponse(challenge *Challenge, authenticate *Authenticate) (*Welcome, error) {
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

////////////////////////////////////////
// Misc and old
////////////////////////////////////////

// CRAuthenticator describes a type that can handle challenge/response authentication.
type CRAuthenticator interface {
	// accept HELLO details and returns a challenge map (which will be sent in a CHALLENGE message)
	Challenge(details map[string]interface{}) (map[string]interface{}, error)
	// accept a challenge map (same as was generated in Challenge) and a signature string, and
	// authenticates the signature string against the challenge. Returns a details map and error.
	Authenticate(challenge map[string]interface{}, signature string) (map[string]interface{}, error)
}

// Authenticator describes a type that can handle authentication based solely on the HELLO message.
//
// Use CRAuthenticator for more complex authentication schemes.
type Authenticator interface {
	// Authenticate takes the HELLO details and returns a (WELCOME) details map if the
	// authentication is successful, otherwise it returns an error
	Authenticate(details map[string]interface{}) (map[string]interface{}, error)
}

type basicTicketAuthenticator struct {
	tickets map[string]bool
}

func (t *basicTicketAuthenticator) Challenge(details map[string]interface{}) (map[string]interface{}, error) {
	return make(map[string]interface{}), nil
}

func (t *basicTicketAuthenticator) Authenticate(challenge map[string]interface{}, signature string) (map[string]interface{}, error) {
	if !t.tickets[signature] {
		return nil, fmt.Errorf("Invalid ticket")
	}
	return nil, nil
}

// NewBasicTicketAuthenticator creates a basic ticket authenticator from a static set of valid tickets.
//
// This method of ticket-based authentication is insecure, but useful for bootstrapping.
// Do not use this in production.
func NewBasicTicketAuthenticator(tickets ...string) CRAuthenticator {
	authenticator := &basicTicketAuthenticator{tickets: make(map[string]bool)}
	for _, ticket := range tickets {
		authenticator.tickets[ticket] = true
	}
	return authenticator
}
