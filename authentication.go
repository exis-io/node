package node

import (
	"encoding/json"
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

func NewAuthen(node *node) Authen {
	authen := Authen {
		CRAuthenticators: make(map[string]CRAuthenticator),
		AuthTimeout: defaultAuthTimeout,
	}

	authen.CRAuthenticators["token"] = NewTokenAuthenticator(node.agent)

	return authen
}

// Move to authn
func (r *Authen) handleAuth(session *Session, hello *Hello) (*Welcome, error) {
	msg, err := r.authenticate(session, hello)

	if err != nil {
		return nil, err
	}

	// we should never get anything besides WELCOME and CHALLENGE
	if msg.MessageType() == WELCOME {
		return msg.(*Welcome), nil
	} else {
		// Challenge response
		challenge := msg.(*Challenge)
		if err := session.Peer.Send(challenge); err != nil {
			return nil, err
		}

		msg, err := GetMessageTimeout(session.Peer, r.AuthTimeout)
		if err != nil {
			return nil, err
		}
		//log.Printf("%s: %+v", msg.MessageType(), msg)
		if authenticate, ok := msg.(*Authenticate); !ok {
			return nil, fmt.Errorf("unexpected %s message received", msg.MessageType())
		} else {
			return r.checkResponse(session, challenge, authenticate)
		}
	}
}

// Authenticate either authenticates a client or returns a challenge message if
// challenge/response authentication is to be used.
func (r Authen) authenticate(session *Session, hello *Hello) (Message, error) {
	// pprint the incoming details

	// if b, err := json.MarshalIndent(details, "", "  "); err != nil {
	// 	fmt.Println("error:", err)
	// } else {
	// 	//log.Printf(string(b))
	// }

	// If client is a local peer, allow it without authentication.
	_, ok := session.Peer.(*localPeer)
	if ok {
		session.authLevel = AUTH_HIGH
		return &Welcome{}, nil
	}

	_authmethods, ok := hello.Details["authmethods"].([]interface{})
	if !ok {
		session.authLevel = AUTH_LOW
		return &Welcome{}, nil
	}

	authmethods := []string{}
	for _, method := range _authmethods {
		if m, ok := method.(string); ok {
			authmethods = append(authmethods, m)
		} else {
			//log.Printf("invalid authmethod value: %v", method)
		}
	}

	details := make(map[string]interface{})
	details["name"] = string(session.pdid)

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
func (r Authen) checkResponse(session *Session, challenge *Challenge, authenticate *Authenticate) (*Welcome, error) {
	if auth, ok := r.CRAuthenticators[challenge.AuthMethod]; !ok {
		return nil, fmt.Errorf("authentication method has been removed")
	} else {
		if details, err := auth.Authenticate(challenge.Extra, authenticate); err != nil {
			return nil, err
		} else {
			session.authLevel = AUTH_HIGH
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
	Authenticate(challenge map[string]interface{}, authenticate *Authenticate) (map[string]interface{}, error)
}

// Authenticator describes a type that can handle authentication based solely on the HELLO message.
//
// Use CRAuthenticator for more complex authentication schemes.
type Authenticator interface {
	// Authenticate takes the HELLO details and returns a (WELCOME) details map if the
	// authentication is successful, otherwise it returns an error
	Authenticate(details map[string]interface{}) (map[string]interface{}, error)
}


//
// Token Authenticator
//
// 1. Through some means, the agent acquires a token.
// 2. During challenge-response, the agent presents his name, the issuing auth
// appliance, and the token.
// 3. We verify the validity token with the auth appliance.
//

type TokenAuthResponse struct {
	Name string
	Auth string
	Token string
}

type TokenAuthenticator struct {
	agent *Client
}

func (ta *TokenAuthenticator) Challenge(details map[string]interface{}) (map[string]interface{}, error) {
	return details, nil
}

func (ta *TokenAuthenticator) Authenticate(challenge map[string]interface{}, authenticate *Authenticate) (map[string]interface{}, error) {
	var response TokenAuthResponse

	err := json.Unmarshal([]byte(authenticate.Signature), &response)
	if err != nil {
		fmt.Println(err)
		return nil, fmt.Errorf("Error parsing authentication response")
	}

	// TODO: Before we can allow arbitrary auth appliances, we need to be able
	// to verify that an auth appliance has authority over a domain.
	//
	// For example, the highest level auth appliance (pd.auth/pd.admin.auth)
	// can grant tokens for anything under pd, but lower level auth appliances
	// need to be restricted.
	if response.Auth != "pd.auth" {
		return nil, fmt.Errorf("Arbitrary auth appliances are not supported yet")
	}

	// The agent is doing something funny here if he presents a token for pd.A
	// but tries to set his pdid to pd.B.  We will allow downward name changes.
	if !subdomain(response.Name, challenge["name"].(string)) {
		return nil, fmt.Errorf("Requested name not a permitted subdomain")
	}

	authEndpoint := response.Auth + "/check_token_1"

	// Verify the token with auth.
	args := []interface{}{response.Name, response.Token}
	ret, err := ta.agent.Call(authEndpoint, args, nil)
	if err != nil {
		return nil, fmt.Errorf("Unable to verify token with auth")
	}

	permitted, ok := ret.Arguments[0].(bool)
	if ok && permitted {
		return nil, nil
	} else {
		return nil, fmt.Errorf("Token not valid")
	}
}

func NewTokenAuthenticator(agent *Client) *TokenAuthenticator {
	authenticator := &TokenAuthenticator{
		agent: agent,
	}
	return authenticator
}
