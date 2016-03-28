package node

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"sync"
	"time"
)

// A broker handles routing EVENTS from Publishers to Subscribers.
type Broker interface {
	// Publishes a message to all Subscribers.
	Publish(Sender, *Publish) *MessageEffect
	// Subscribes to messages on a URI.
	Subscribe(*Session, *Subscribe) *MessageEffect
	// Unsubscribes from messages on a URI.
	Unsubscribe(*Session, *Unsubscribe) *MessageEffect

	lostSession(*Session)
}

// A super simple broker that matches URIs to Subscribers.
type defaultBroker struct {
	node *node

	routes map[URI]map[ID]Sender

	// Keep track of subscriptions by session, so that we can clean up when the
	// session closes.  For each session, we have a map[ID]URI, which maps
	// subscription ID to the endpoint.
	subscriptions map[Sender]map[ID]URI

	// Use this mutex to protect all accesses to the routes and subscriptions
	// maps.
	subMutex sync.Mutex
}

// NewDefaultBroker initializes and returns a simple broker that matches URIs to Subscribers.
func NewDefaultBroker(node *node) Broker {
	return &defaultBroker{
		node:          node,
		routes:        make(map[URI]map[ID]Sender),
		subscriptions: make(map[Sender]map[ID]URI),
	}
}

// Publish sends a message to all subscribed clients except for the sender.
//
// If msg.Options["acknowledge"] == true, the publisher receives a Published event
// after the message has been sent to all subscribers.
func (br *defaultBroker) Publish(pub Sender, msg *Publish) *MessageEffect {
	pubId := NewID()

	evtTemplate := Event{
		Publication: pubId,
		Arguments:   msg.Arguments,
		ArgumentsKw: msg.ArgumentsKw,
		Details:     make(map[string]interface{}),
	}

	// Make a copy of the subscriber list so we don't hold the lock during the
	// send calls.
	subs := make(map[ID]Sender)
	br.subMutex.Lock()
	for id, sub := range br.routes[msg.Topic] {
		// don't send event to publisher
		if sub != pub {
			subs[id] = sub
		}
	}
	br.subMutex.Unlock()

	for id, sub := range subs {
		// shallow-copy the template
		event := evtTemplate
		event.Subscription = id
		sub.Send(&event)
	}

	result := NewMessageEffect(msg.Topic, "", pubId)

	// only send published message if acknowledge is present and set to true
	if doPub, _ := msg.Options["acknowledge"].(bool); doPub {
		result.Response = "Published"
		pub.Send(&Published{Request: msg.Request, Publication: pubId})
	}

	return result
}

// Subscribe subscribes the client to the given topic.
func (br *defaultBroker) Subscribe(sub *Session, msg *Subscribe) *MessageEffect {
	id := NewID()

	br.subMutex.Lock()

	if _, ok := br.routes[msg.Topic]; !ok {
		br.routes[msg.Topic] = make(map[ID]Sender)
	}

	br.routes[msg.Topic][id] = sub

	if br.subscriptions[sub] == nil {
		br.subscriptions[sub] = make(map[ID]URI)
	}
	br.subscriptions[sub][id] = msg.Topic

	br.subMutex.Unlock()

	sub.Send(&Subscribed{Request: msg.Request, Subscription: id})

	return NewMessageEffect(msg.Topic, "Subscribed", id)
}

func (br *defaultBroker) Unsubscribe(sub *Session, msg *Unsubscribe) *MessageEffect {
	var topic URI

	br.subMutex.Lock()

	topic, ok := br.subscriptions[sub][msg.Subscription]
	if !ok {
		br.subMutex.Unlock()

		err := &Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Error:   ErrNoSuchSubscription,
		}
		sub.Send(err)
		//log.Printf("Error unsubscribing: no such subscription %v", msg.Subscription)

		return NewErrorMessageEffect("", ErrNoSuchSubscription, msg.Subscription)
	}

	delete(br.subscriptions[sub], msg.Subscription)

	if r, ok := br.routes[topic]; !ok {
		//log.Printf("Error unsubscribing: unable to find routes for %s topic", topic)
	} else if _, ok := r[msg.Subscription]; !ok {
		//log.Printf("Error unsubscribing: %s route does not exist for %v subscription", topic, msg.Subscription)
	} else {
		delete(r, msg.Subscription)
		if len(r) == 0 {
			delete(br.routes, topic)
		}
	}

	br.subMutex.Unlock()

	sub.Send(&Unsubscribed{Request: msg.Request})

	return NewMessageEffect(topic, "Unsubscribed", msg.Subscription)
}

// Remove all the subs for a session that has disconected
func (br *defaultBroker) lostSession(sess *Session) {
	br.subMutex.Lock()

	for id, topic := range br.subscriptions[sess] {
		out.Debug("Unsubscribe: %s from %s", sess, string(topic))
		delete(br.subscriptions[sess], id)
		delete(br.routes[topic], id)
	}

	delete(br.subscriptions, sess)

	br.subMutex.Unlock()
}

//
// Redis data structures:
//
// subscription:<session_id>:<subscription_id> -> hash of subscriptionInfo
// subscribers:<endpoint> -> set of subscription:<session_id>:<subscription_id>
// subscribed:<session_id> -> set of subscription:<session_id>:<subscription_id>
//

type heldEvent struct {
	event   *Event
	expires int64
}

type eventQueueKey struct {
	session  ID
	endpoint URI
}

type redisBroker struct {
	node *node

	heldEvents  map[eventQueueKey][]heldEvent
	eventsMutex sync.Mutex
}

func NewRedisBroker(node *node) Broker {
	return &redisBroker{
		node:       node,
		heldEvents: make(map[eventQueueKey][]heldEvent),
	}
}

type subscriptionInfo struct {
	ID        int64  `redis:"id"`
	SessionID int64  `redis:"sessionid"`
	Endpoint  string `redis:"endpoint"`
}

type subscriptionIterator struct {
	conn   redis.Conn
	key    string
	cursor int
	values []string
}

func (iter *subscriptionIterator) Value() *subscriptionInfo {
	sub := &subscriptionInfo{}
	reply, _ := redis.Values(iter.conn.Do("HGETALL", iter.values[0]))
	redis.ScanStruct(reply, sub)

	return sub
}

func (iter *subscriptionIterator) Next() bool {
	if len(iter.values) > 0 {
		iter.values = iter.values[1:]
	}

	for len(iter.values) == 0 && iter.cursor != 0 {
		// Special initialization case: -1 means this is the first query.
		if iter.cursor == -1 {
			iter.cursor = 0
		}

		arr, err := redis.Values(iter.conn.Do("SSCAN", iter.key, iter.cursor))
		if err != nil {
			return false
		}

		iter.cursor, _ = redis.Int(arr[0], nil)
		iter.values, _ = redis.Strings(arr[1], nil)
	}

	return (len(iter.values) > 0)
}

func (iter *subscriptionIterator) Close() {
	if iter.conn != nil {
		iter.conn.Close()
		iter.conn = nil
	}
}

// Returns an iterator.  Be sure to call Close when done with the iterator!
func GetSubscriptions(pool *redis.Pool, endpoint URI) *subscriptionIterator {
	iter := &subscriptionIterator{
		conn:   pool.Get(),
		key:    fmt.Sprintf("subscribers:%s", endpoint),
		cursor: -1,
	}
	return iter
}

func StoreSubscription(pool *redis.Pool, endpoint URI, session ID, subscription ID) error {
	conn := pool.Get()
	defer conn.Close()

	var result error = nil

	subscriptionKey := fmt.Sprintf("subscription:%x:%x", session, subscription)
	args := []interface{}{
		subscriptionKey,
		"id", int64(subscription),
		"sessionid", int64(session),
		"endpoint", string(endpoint),
	}

	_, err := conn.Do("HMSET", args...)
	if err != nil {
		fmt.Println(err)
		result = err
	}

	subscribersKey := fmt.Sprintf("subscribers:%s", endpoint)
	_, err = conn.Do("SADD", subscribersKey, subscriptionKey)
	if err != nil {
		fmt.Println(err)
		result = err
	}

	subscribedKey := fmt.Sprintf("subscribed:%x", session)
	_, err = conn.Do("SADD", subscribedKey, subscriptionKey)
	if err != nil {
		fmt.Println(err)
		result = err
	}

	return result
}

func RemoveSubscription(pool *redis.Pool, session ID, subscription ID) error {
	conn := pool.Get()
	defer conn.Close()

	var result error = nil

	subscriptionKey := fmt.Sprintf("subscription:%x:%x", session, subscription)

	endpoint, err := redis.String(conn.Do("HGET", subscriptionKey, "endpoint"))
	if err != nil {
		fmt.Println(err)
		return err
	} else if endpoint == "" {
		return fmt.Errorf("Subscription not found")
	}

	subscribersKey := fmt.Sprintf("subscribers:%s", endpoint)
	_, err = conn.Do("SREM", subscribersKey, subscriptionKey)
	if err != nil {
		fmt.Println(err)
		result = err
	}

	subscribedKey := fmt.Sprintf("subscribed:%x", session)
	_, err = conn.Do("SREM", subscribedKey, subscriptionKey)
	if err != nil {
		fmt.Println(err)
		result = err
	}

	_, err = conn.Do("DEL", subscriptionKey)
	if err != nil {
		fmt.Println(err)
		result = err
	}

	return result
}

func (br *redisBroker) purgeEvent(key eventQueueKey) {
	timeout := int64(br.node.Config.HoldCalls)

	// As long as there are entries remaining in the endpoint's heldEvents
	// queue, this routine will walk through them and delete them one by one as
	// they expire.
	remaining := true
	for remaining {
		if timeout > 0 {
			time.Sleep(time.Duration(timeout) * time.Second)
		}

		br.eventsMutex.Lock()
		if len(br.heldEvents[key]) > 0 {
			br.heldEvents[key] = br.heldEvents[key][1:]
		}
		if len(br.heldEvents[key]) > 0 {
			// Compute the time until next expiration.
			timeout = br.heldEvents[key][0].expires - time.Now().Unix()
		} else {
			remaining = false
		}
		br.eventsMutex.Unlock()
	}
}

func (br *redisBroker) holdEvent(event *Event, session ID, endpoint URI) {
	br.eventsMutex.Lock()
	defer br.eventsMutex.Unlock()

	key := eventQueueKey{
		session:  session,
		endpoint: endpoint,
	}

	if len(br.heldEvents[key]) == 0 {
		// This is the first held event for the endpoint,
		// so schedule a worker to delete it later.
		go br.purgeEvent(key)
	}

	hevent := heldEvent{
		event:   event,
		expires: time.Now().Unix() + int64(br.node.Config.HoldCalls),
	}

	br.heldEvents[key] = append(br.heldEvents[key], hevent)
}

// Publish sends a message to all subscribed clients except for the sender.
//
// If msg.Options["acknowledge"] == true, the publisher receives a Published event
// after the message has been sent to all subscribers.
func (br *redisBroker) Publish(pub Sender, msg *Publish) *MessageEffect {
	pubId := NewID()

	evtTemplate := Event{
		Publication: pubId,
		Arguments:   msg.Arguments,
		ArgumentsKw: msg.ArgumentsKw,
		Details:     make(map[string]interface{}),
	}

	iter := GetSubscriptions(br.node.RedisPool, msg.Topic)
	for iter.Next() {
		sub := iter.Value()

		receiver, ok := br.node.GetSession(ID(sub.SessionID))
		if ok {
			// shallow-copy the template
			event := evtTemplate
			event.Subscription = ID(sub.ID)
			receiver.Send(&event)
		} else {
			br.holdEvent(&evtTemplate, ID(sub.SessionID), msg.Topic)
			go ThawSession(br.node.RedisPool, br.node.agent, ID(sub.SessionID))
		}
	}
	iter.Close()

	result := NewMessageEffect(msg.Topic, "", pubId)

	// only send published message if acknowledge is present and set to true
	if doPub, _ := msg.Options["acknowledge"].(bool); doPub {
		result.Response = "Published"
		pub.Send(&Published{Request: msg.Request, Publication: pubId})
	}

	return result
}

// Subscribe subscribes the client to the given topic.
func (br *redisBroker) Subscribe(sub *Session, msg *Subscribe) *MessageEffect {
	id := NewID()

	err := StoreSubscription(br.node.RedisPool, msg.Topic, sub.Id, id)
	if err != nil {
		err := &Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Error:   ErrInternalError,
		}
		sub.Send(err)
		return NewErrorMessageEffect("", ErrInternalError, msg.Request)
	}

	sub.Send(&Subscribed{Request: msg.Request, Subscription: id})

	if sub.resumeFrom != ID(0) {
		// If there are any held events, send them now.
		key := eventQueueKey{
			session:  sub.resumeFrom,
			endpoint: msg.Topic,
		}

		br.eventsMutex.Lock()
		defer br.eventsMutex.Unlock()

		for _, hevent := range br.heldEvents[key] {
			event := hevent.event
			event.Subscription = id
			sub.Send(event)
		}

		delete(br.heldEvents, key)
	}

	return NewMessageEffect(msg.Topic, "Subscribed", id)
}

func (br *redisBroker) Unsubscribe(sub *Session, msg *Unsubscribe) *MessageEffect {
	err := RemoveSubscription(br.node.RedisPool, sub.Id, msg.Subscription)
	if err != nil {
		err := &Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Error:   ErrInternalError,
		}
		sub.Send(err)
		return NewErrorMessageEffect("", ErrInternalError, msg.Request)
	}

	sub.Send(&Unsubscribed{Request: msg.Request})

	return NewMessageEffect("", "Unsubscribed", msg.Subscription)
}

// Remove all the subs for a session that has disconected
func (br *redisBroker) lostSession(sess *Session) {
}
