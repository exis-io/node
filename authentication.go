package node

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"
)

const (
	defaultAuthTimeout = 2 * time.Minute
	defaultNonceSize = 32
	defaultHashMethod = "sha512"
)

// Holds stored certificates, contacts the auth appliance, etc
type Authen struct {
	CRAuthenticators map[string]CRAuthenticator
	Authenticators   map[string]Authenticator
	AuthTimeout      time.Duration
	PubKeys          map[string]*rsa.PublicKey
}

func NewAuthen(node *node) Authen {
	authen := Authen {
		CRAuthenticators: make(map[string]CRAuthenticator),
		AuthTimeout: defaultAuthTimeout,
		PubKeys: make(map[string]*rsa.PublicKey),
	}

	authen.LoadPubKeys()

	authen.CRAuthenticators["token"] = NewTokenAuthenticator(node.agent)
	authen.CRAuthenticators["signature"] = NewSignatureAuthenticator(node.agent, authen.PubKeys)

	return authen
}

// Read a public key from a PEM file.
//
// PEM files are the ones that look like this:
// -----BEGIN PUBLIC KEY-----
// Base64 encoded data...
// -----END PUBLIC KEY-----
func ReadPublicKey(path string) (*rsa.PublicKey, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println("read")
		return nil, err
	}

	// Decode the PEM public key
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("Error decoding PEM file")
	}

	// Parse the public key.
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	// Type assertion: want an rsa.PublicKey.
	pubkey, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("Error loading RSA public key")
	}

	return pubkey, nil
}

// Load public keys from a directory.
//
// The directory is specified by the environment variable PUBKEYS.  Each file
// should be a PEM-encoded public key, and the file name will be used as the
// authorized pdid.
//
// Example: A file named "pd.auth" authorizes the owner of that public key to
// authenticate as "pd.auth".
//
// This feature should only be used for loading core appliances, particularly
// auth.  Everything else should register with auth, and we will query auth.
func (r *Authen) LoadPubKeys() {
	dirname := os.Getenv("PUBKEYS")
	if dirname == "" {
		dirname = "."
	}

	files, _ := ioutil.ReadDir(dirname)
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		path := path.Join(dirname, f.Name())

		pubkey, err := ReadPublicKey(path)
		if err == nil {
			fmt.Println("Loaded public key for:", f.Name())
			r.PubKeys[f.Name()] = pubkey
		}
	}
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

//
// Signature Authenticator
//
// This is the more secure approach to authentication.
// 1. The agent holds a private key, and the knows the corresponding public key.
// 2. During challenge, we send a random string.
// 3. The agent signs the hash of the challenge string and sends it back.
// 4. The node verifies the signature against the public key.
//
// TODO: We are missing authentication of the node.  The agent should
// send a challenge to the node, and the node should send back a signed hash.
//

type SignatureAuthenticator struct {
	agent *Client
	PublicKeys map[string]*rsa.PublicKey
}

func (ta *SignatureAuthenticator) Challenge(details map[string]interface{}) (map[string]interface{}, error) {
	data := make([]byte, defaultNonceSize)
	_, err := rand.Read(data)
	if err != nil {
		return nil, fmt.Errorf("Error generating random nonce")
	}

	nonce := hex.EncodeToString(data)

	details["challenge"] = nonce

	// Tell the agent what hash method to use.  This gives us a path to upgrade.
	details["hash"] = defaultHashMethod

	return details, nil
}

func (ta *SignatureAuthenticator) Authenticate(challenge map[string]interface{}, authenticate *Authenticate) (map[string]interface{}, error) {
	pdid := challenge["name"].(string)

	// This is the random nonce that was sent to the agent.
	nonce := []byte(challenge["challenge"].(string))

	// If we want to support different hash functions, here is where we need to
	// do it.
	if challenge["hash"] != "sha512" {
		fmt.Printf("Warning: hash method %s not supported.\n", challenge["hash"])
		return nil, fmt.Errorf("Node error: hash method not supported")
	}
	hashed := sha512.Sum512(nonce)

	// Decode the base64 encoded signature from the agent.
	signature, err := base64.StdEncoding.DecodeString(authenticate.Signature)
	if err != nil {
		return nil, fmt.Errorf("Error decoding signature")
	}

	pubkey, _ := ta.PublicKeys[pdid]
	if pubkey == nil {
		// TODO: Go ask auth for the public key.
		return nil, fmt.Errorf("Public key not found")
	}

	err = rsa.VerifyPKCS1v15(pubkey, crypto.SHA512, hashed[:], signature)
	if err != nil {
		return nil, fmt.Errorf("Signature is not correct: %s", err)
	}

	return nil, nil
}

func NewSignatureAuthenticator(agent *Client, pubkeys map[string]*rsa.PublicKey) *SignatureAuthenticator {
	authenticator := &SignatureAuthenticator{
		agent: agent,
		PublicKeys: pubkeys,
	}
	return authenticator
}
