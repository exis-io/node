package node

import (
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
)

const (
	jsonWebsocketProtocol    = "wamp.2.json"
	msgpackWebsocketProtocol = "wamp.2.msgpack"
)

type invalidPayload byte

func (e invalidPayload) Error() string {
	return fmt.Sprintf("Invalid payloadType: %d", e)
}

type protocolExists string

func (e protocolExists) Error() string {
	return "This protocol has already been registered: " + string(e)
}

type protocol struct {
	payloadType int
	serializer  Serializer
}

// WebsocketServer handles websocket connections.
type WebsocketServer struct {
	Node
	Upgrader *websocket.Upgrader

	protocols map[string]protocol

	// The serializer to use for text frames. Defaults to JSONSerializer.
	TextSerializer Serializer

	// The serializer to use for binary frames. Defaults to JSONSerializer.
	BinarySerializer Serializer
}

func CreateNode(config *NodeConfig) *WebsocketServer {
	r := NewNode(config)
	s := newWebsocketServer(r)
	return s
}

func newWebsocketServer(r Node) *WebsocketServer {
	s := &WebsocketServer{
		Node:      r,
		protocols: make(map[string]protocol),
	}

	s.Upgrader = &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	s.RegisterProtocol(jsonWebsocketProtocol, websocket.TextMessage, new(JSONSerializer))
	s.RegisterProtocol(msgpackWebsocketProtocol, websocket.BinaryMessage, new(MessagePackSerializer))

	return s
}

// RegisterProtocol registers a serializer that should be used for a given protocol string and payload type.
func (s *WebsocketServer) RegisterProtocol(proto string, payloadType int, serializer Serializer) error {
	//out.Debug("RegisterProtocol:", proto)

	if payloadType != websocket.TextMessage && payloadType != websocket.BinaryMessage {
		return invalidPayload(payloadType)
	}

	if _, ok := s.protocols[proto]; ok {
		return protocolExists(proto)
	}

	s.protocols[proto] = protocol{payloadType, serializer}
	s.Upgrader.Subprotocols = append(s.Upgrader.Subprotocols, proto)
	return nil
}

// GetLocalClient returns a client connected to the specified realm
func (s *WebsocketServer) GetLocalClient(realm string, details map[string]interface{}) (*Client, error) {
	//out.Debug("Request for local client for realm: %s", realm)
	if peer, err := s.Node.GetLocalPeer(URI(realm), details); err != nil {
		return nil, err
	} else {
		c := NewClient(peer)
		go c.Receive()
		return c, nil
	}
}

// ServeHTTP handles a new HTTP connection.
func (s *WebsocketServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//out.Debug("WebsocketServer.ServeHTTP", r.Method, r.RequestURI)

	// TODO: subprotocol?
	conn, err := s.Upgrader.Upgrade(w, r, nil)

	if err != nil {
		out.Critical("Error upgrading to websocket connection:", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.handleWebsocket(conn)
}

func (s *WebsocketServer) handleWebsocket(conn *websocket.Conn) {
	//out.Debug("New WS connection: %s", conn)
	var serializer Serializer
	var payloadType int
	if proto, ok := s.protocols[conn.Subprotocol()]; ok {
		serializer = proto.serializer
		payloadType = proto.payloadType
	} else {
		// TODO: this will not currently ever be hit because
		//       gorilla/websocket will reject the conncetion
		//       if the subprotocol isn't registered
		switch conn.Subprotocol() {
		case jsonWebsocketProtocol:
			serializer = new(JSONSerializer)
			payloadType = websocket.TextMessage
		case msgpackWebsocketProtocol:
			serializer = new(MessagePackSerializer)
			payloadType = websocket.BinaryMessage
		default:
			conn.Close()
			return
		}
	}

	peer := websocketPeer{
		conn:        conn,
		serializer:  serializer,
		messages:    make(chan Message, 10),
		payloadType: payloadType,
	}
	go peer.run()

	logErr(s.Node.Accept(&peer))
}
