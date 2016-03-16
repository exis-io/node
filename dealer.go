package node

import (
	// "reflect"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"sync"
	"time"
)

// A Dealer routes and manages RPC calls to callees.
type Dealer interface {
	// Register a procedure on an endpoint
	Register(*Session, *Register) *MessageEffect
	// Unregister a procedure on an endpoint
	Unregister(*Session, *Unregister) *MessageEffect
	// Call a procedure on an endpoint
	Call(Sender, *Call) *MessageEffect
	// Return the result of a procedure call
	Yield(Sender, *Yield) *MessageEffect
	// Handle an ERROR message from an invocation
	Error(Sender, *Error) *MessageEffect

	UnregisterAll(URI) int
	SetCallHolding(bool)

	hasRegistration(URI) bool
	lostSession(*Session)
}

type RemoteProcedure struct {
	Receiver    Sender
	Procedure   URI
	PassDetails bool
}

type OutstandingRequest struct {
	request   ID
	procedure URI
	caller    Sender
}

func NewDefaultDealer(node *node) Dealer {
	return &defaultDealer{
		node:                 node,
		procedures:           make(map[ID]RemoteProcedure),
		registrations:        make(map[URI]RegistrationHolder),
		requests:             make(map[ID]OutstandingRequest),
		sessionRegistrations: make(map[Sender]map[ID]URI),
		blockedCalls:         make(map[URI]map[Sender]chan bool),
		waitForRegistration:  true,
		holdTimeout:          time.Duration(node.Config.HoldCalls) * time.Second,
	}
}

func NewRemoteProcedure(endpoint Sender, procedure URI, tags map[string]bool) RemoteProcedure {
	proc := RemoteProcedure{
		Receiver:    endpoint,
		Procedure:   procedure,
		PassDetails: false,
	}

	proc.PassDetails = tags["details"]

	return proc
}

type defaultDealer struct {
	node *node

	//
	// Registrations
	//

	// map registration IDs to procedures
	procedures map[ID]RemoteProcedure

	// map procedure URIs to registration holders
	registrations map[URI]RegistrationHolder

	// Keep track of registrations by session, so that we can clean up when the
	// session closes.  For each session, we have a map[ID]URI, which we are
	// using as a set of registration IDs.
	sessionRegistrations map[Sender]map[ID]URI

	// waitForRegistration specifies how to treat calls for procedures that do
	// not exist.  If false, we send back an error immediately.  If true, we
	// queue the call and block until the appropriate registration comes in.
	//
	// This is intended to solve out-of-order connections of core appliances
	// during the startup phase on the node.
	//
	// This is lumped in with the registrations fields because it makes sense
	// to read this flag with the registrationMutex read lock held.  We check
	// this flag at the same time as checking whether a registration exists.
	waitForRegistration bool

	// Protect procedures and registrations maps.  Assume there will be more
	// reads than writes (agents making calls vs.  registrations).
	registrationMutex sync.RWMutex

	//
	// Requests
	//

	// Map InvocationID to RequestID so we can send the RequestID with the
	// result (lets caller know what request the result is for).
	requests map[ID]OutstandingRequest

	// Map procedure URIs to lists of channels that are blocked waiting for
	// registrations on those URIs.  See the comment for the
	// waitForRegistration flag.
	blockedCalls map[URI]map[Sender]chan bool

	// Protect requests and blockedCalls maps.
	requestMutex sync.Mutex

	holdTimeout time.Duration
}

func (d *defaultDealer) Register(callee *Session, msg *Register) *MessageEffect {
	// Endpoint may contain a # sign to pass comma-separated tags.
	// Example: pd.agent/function#details
	parts := strings.SplitN(string(msg.Procedure), "#", 2)
	endpoint := URI(parts[0])

	tags := make(map[string]bool)
	if len(parts) > 1 {
		for _, tag := range strings.Split(parts[1], ",") {
			tags[tag] = true
		}
	}

	d.registrationMutex.Lock()

	holder, ok := d.registrations[endpoint]
	if ok {
		// Return ErrProcedureAlreadyExists under two cases:
		// 1. Agent asked for multi-registration, but existing holder will not
		// allow it.
		// 2. Agent did not ask for multi-registration, and existing holder
		// already has a handler.
		if (tags["multi"] && !holder.CanAdd()) ||
			(!tags["multi"] && holder.HasHandler()) {
			// Unlock before calling Send.
			d.registrationMutex.Unlock()

			//log.Println("error: procedure already exists:", msg.Procedure, id)
			out.Error("error: procedure already exists:", endpoint)
			callee.Send(&Error{
				Type:    msg.MessageType(),
				Request: msg.Request,
				Details: make(map[string]interface{}),
				Error:   ErrProcedureAlreadyExists,
			})
			return NewErrorMessageEffect(endpoint, ErrProcedureAlreadyExists, 0)
		}
	} else {
		if tags["multi"] {
			holder = NewMultiRegistrationHolder()
		} else {
			holder = NewSingleRegistrationHolder()
		}

		d.registrations[endpoint] = holder
	}

	reg := NewID()

	holder.Add(reg)
	d.procedures[reg] = NewRemoteProcedure(callee, endpoint, tags)

	if d.sessionRegistrations[callee] == nil {
		d.sessionRegistrations[callee] = make(map[ID]URI)
	}
	d.sessionRegistrations[callee][reg] = endpoint
	d.registrationMutex.Unlock()

	d.requestMutex.Lock()
	for _, waiting := range d.blockedCalls[endpoint] {
		waiting <- true
	}
	delete(d.blockedCalls, endpoint)
	d.requestMutex.Unlock()

	//log.Printf("registered procedure %v [%v]", reg, msg.Procedure)
	callee.Send(&Registered{
		Request:      msg.Request,
		Registration: reg,
	})

	return NewMessageEffect(endpoint, "Registered", reg)
}

func (d *defaultDealer) Unregister(callee *Session, msg *Unregister) *MessageEffect {
	d.registrationMutex.Lock()
	if procedure, ok := d.procedures[msg.Registration]; !ok {
		d.registrationMutex.Unlock()

		// the registration doesn't exist
		//log.Println("error: no such registration:", msg.Registration)
		callee.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrNoSuchRegistration,
		})
		return NewErrorMessageEffect("", ErrNoSuchRegistration, msg.Registration)
	} else {
		delete(d.sessionRegistrations[callee], msg.Registration)
		delete(d.procedures, msg.Registration)

		holder, ok := d.registrations[procedure.Procedure]
		if ok {
			holder.Remove(msg.Registration)
			if !holder.HasHandler() {
				delete(d.registrations, procedure.Procedure)
			}
		}

		d.registrationMutex.Unlock()

		//log.Printf("unregistered procedure %v [%v]", procedure.Procedure, msg.Registration)
		callee.Send(&Unregistered{
			Request: msg.Request,
		})
		return NewMessageEffect(procedure.Procedure, "Unregistered", msg.Registration)
	}
}

// Call with registrationMutex read lock held.
func (d *defaultDealer) shouldHoldCall(procedure URI) bool {
	holder, exists := d.registrations[procedure]
	if exists && holder.HasHandler() {
		return false
	}

	// Did not find a handler anywhere.  Are we in startup mode?
	return d.waitForRegistration
}

// Call with a read lock on d.registrationMutex
// This function releases the lock before returning.
func (d *defaultDealer) resolveCall(caller Sender, msg *Call) *MessageEffect {
	holder, ok := d.registrations[msg.Procedure]
	if !ok || !holder.HasHandler() {
		d.registrationMutex.RUnlock()
		caller.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrNoSuchProcedure,
		})
		return NewErrorMessageEffect(msg.Procedure, ErrNoSuchProcedure, 0)
	} else {
		reg, _ := holder.GetHandler()
		if rproc, ok := d.procedures[reg]; !ok {
			d.registrationMutex.RUnlock()
			// found a registration id, but doesn't match any remote procedure
			caller.Send(&Error{
				Type:    msg.MessageType(),
				Request: msg.Request,
				Details: make(map[string]interface{}),
				// TODO: what should this error be?
				Error: ErrInternalError,
			})
			return NewErrorMessageEffect(msg.Procedure, ErrInternalError, 0)
		} else {
			// everything checks out, make the invocation request
			args := msg.Arguments
			kwargs := msg.ArgumentsKw

			// Remote procedures with the PassDetails flag set will receive a
			// special first argument set by the node.
			if rproc.PassDetails {
				details := make(map[string]interface{})

				// Make sure the argument list exists first.
				if args == nil {
					args = make([]interface{}, 0)
				}

				// Does the caller want to be disclosed?
				// We default to true unless he explicitly says otherwise.
				disclose_caller, ok := msg.Options["disclose_me"].(bool)
				if !ok {
					disclose_caller = true
				}

				if disclose_caller {
					sess := caller.(*Session)
					if sess != nil {
						details["caller"] = sess.pdid
					}
				}

				// Insert as the first positional argument.
				args = append(args, nil)
				copy(args[1:], args[:])
				args[0] = details
			}

			d.registrationMutex.RUnlock()

			invocationID := NewID()

			d.requestMutex.Lock()
			d.requests[invocationID] = OutstandingRequest{
				request:   msg.Request,
				procedure: msg.Procedure,
				caller:    caller,
			}
			d.requestMutex.Unlock()

			rproc.Receiver.Send(&Invocation{
				Request:      invocationID,
				Registration: reg,
				Details:      msg.Options,
				Arguments:    args,
				ArgumentsKw:  kwargs,
			})
			return NewMessageEffect(msg.Procedure, "", invocationID)
		}
	}
}

func (d *defaultDealer) Call(caller Sender, msg *Call) *MessageEffect {
	d.registrationMutex.RLock()

	if d.shouldHoldCall(msg.Procedure) {
		out.Debug("Hold CALL %s from %s", msg.Procedure, caller)

		ready := make(chan bool)

		d.requestMutex.Lock()
		if d.blockedCalls[msg.Procedure] == nil {
			d.blockedCalls[msg.Procedure] = make(map[Sender]chan bool)
		} else if d.blockedCalls[msg.Procedure][caller] != nil {
			// If this section executes, a session somehow triggered more than
			// one blocked call.  It means my assumption that there can be at
			// most one was wrong, and some restructuring of the code will be
			// necessary to handle more than one blocked call correctly.
			d.requestMutex.Unlock()
			d.registrationMutex.RUnlock()

			out.Warning("Hold already in progress for %s from %s", msg.Procedure,
				caller)

			caller.Send(&Error{
				Type:    msg.MessageType(),
				Request: msg.Request,
				Details: make(map[string]interface{}),
				Error:   ErrInternalError,
			})

			return NewErrorMessageEffect(msg.Procedure, ErrInternalError, 0)
		}
		d.blockedCalls[msg.Procedure][caller] = ready
		d.requestMutex.Unlock()

		// Unlock while we block on the channel.
		d.registrationMutex.RUnlock()

		select {
		case <-time.After(d.holdTimeout):
			out.Debug("Timed out waiting for %s", msg.Procedure)

			// We timed out waiting for a registration to come in.  Delete the
			// information from the blocked calls table and send an error to
			// the caller.
			d.requestMutex.Lock()
			delete(d.blockedCalls[msg.Procedure], caller)
			d.requestMutex.Unlock()

			caller.Send(&Error{
				Type:    msg.MessageType(),
				Request: msg.Request,
				Details: make(map[string]interface{}),
				Error:   ErrNoSuchProcedure,
			})

			return NewErrorMessageEffect(msg.Procedure, ErrNoSuchProcedure, 0)
		case <-ready:
			// A registration occurred while we were waiting, so we are ready
			// to proceed to make the call.
		}

		d.registrationMutex.RLock()
	}

	// Releases the read lock on registrationMutex.
	return d.resolveCall(caller, msg)
}

func (d *defaultDealer) Yield(callee Sender, msg *Yield) *MessageEffect {
	d.requestMutex.Lock()

	request, ok := d.requests[msg.Request]
	if !ok {
		d.requestMutex.Unlock()

		// WAMP spec doesn't allow sending an error in response to a YIELD message
		//log.Println("received YIELD message with invalid invocation request ID:", msg.Request)
		return NewErrorMessageEffect("", ErrNoSuchRegistration, msg.Request)
	}

	// If the progress option is set, it is a progressive result.  There should
	// be more YIELDs coming until the final YIELD which does not have the
	// progress option set.
	is_progress, ok := msg.Options["progress"].(bool)
	if !ok || !is_progress {
		delete(d.requests, msg.Request)
	}

	d.requestMutex.Unlock()

	// return the result to the caller
	request.caller.Send(&Result{
		Request:     request.request,
		Details:     msg.Options,
		Arguments:   msg.Arguments,
		ArgumentsKw: msg.ArgumentsKw,
	})

	return NewMessageEffect(request.procedure, "Result", msg.Request)
}

func (d *defaultDealer) Error(peer Sender, msg *Error) *MessageEffect {
	d.requestMutex.Lock()

	request, ok := d.requests[msg.Request]
	if !ok {
		d.requestMutex.Unlock()

		// WAMP spec doesn't allow sending an error in response to a YIELD message
		//log.Println("received YIELD message with invalid invocation request ID:", msg.Request)
		return NewErrorMessageEffect("", ErrNoSuchRegistration, msg.Request)
	}

	delete(d.requests, msg.Request)

	d.requestMutex.Unlock()

	// return an error to the caller
	request.caller.Send(&Error{
		Type:        CALL,
		Request:     request.request,
		Details:     make(map[string]interface{}),
		Arguments:   msg.Arguments,
		ArgumentsKw: msg.ArgumentsKw,
		Error:       msg.Error,
	})

	return NewErrorMessageEffect(request.procedure, msg.Error, msg.Request)
}

// Unregister all handlers for a given endpoint.  Use with caution!  The
// agent(s) who registered the endpoint will not be informed that their
// registration has been removed from the node.
func (d *defaultDealer) UnregisterAll(endpoint URI) int {
	var unregistered = 0

	d.registrationMutex.Lock()
	defer d.registrationMutex.Unlock()

	holder, ok := d.registrations[endpoint]
	if !ok {
		return unregistered
	}

	for {
		regid, ok := holder.GetHandler()
		if !ok {
			break
		}

		rproc, ok := d.procedures[regid]
		if ok {
			// TODO: Should send a message to the agent who registered this
			// endpoint, but the Unregistered message can only be sent in
			// response to an Unregister request.
			out.Debug("UNREGISTER %s from %s", endpoint, rproc.Receiver)
			delete(d.sessionRegistrations[rproc.Receiver], regid)
			delete(d.procedures, regid)
		}

		holder.Remove(regid)

		unregistered++
	}

	delete(d.registrations, endpoint)

	return unregistered
}

func (d *defaultDealer) SetCallHolding(enable bool) {
	// Make sure no one is reading this flag at the time that we change it.
	d.registrationMutex.Lock()
	d.waitForRegistration = enable
	d.registrationMutex.Unlock()
}

// Remove all the registrations for a session that has disconected
func (d *defaultDealer) lostSession(sess *Session) {
	// TODO: Do something about outstanding requests

	d.registrationMutex.Lock()
	defer d.registrationMutex.Unlock()

	for id, uri := range d.sessionRegistrations[sess] {
		out.Debug("Unregister: %s", string(uri))

		holder, ok := d.registrations[uri]
		if ok {
			holder.Remove(id)
			if !holder.HasHandler() {
				delete(d.registrations, uri)
			}
		}

		delete(d.procedures, id)
	}
	delete(d.sessionRegistrations, sess)
}

// Testing. Not sure if this works 100 or not
func (d *defaultDealer) hasRegistration(s URI) bool {
	d.registrationMutex.RLock()
	defer d.registrationMutex.RUnlock()

	holder, exists := d.registrations[s]
	return exists && holder.HasHandler()
}

//
// Redis data structures:
//
// registration:<session_id>:<registration_id> -> hash of registrationInfo
// procedures:<endpoint> -> ordered set
// registered:<session_id> -> set
//

type redisDealer struct {
	node *node

	// waitForRegistration specifies how to treat calls for procedures that do
	// not exist.  If false, we send back an error immediately.  If true, we
	// queue the call and block until the appropriate registration comes in.
	//
	// This is intended to solve out-of-order connections of core appliances
	// during the startup phase on the node.
	//
	// This is lumped in with the registrations fields because it makes sense
	// to read this flag with the registrationMutex read lock held.  We check
	// this flag at the same time as checking whether a registration exists.
	waitForRegistration bool

	// Map InvocationID to RequestID so we can send the RequestID with the
	// result (lets caller know what request the result is for).
	requests map[ID]OutstandingRequest

	// Map procedure URIs to lists of channels that are blocked waiting for
	// registrations on those URIs.  See the comment for the
	// waitForRegistration flag.
	blockedCalls map[URI]map[Sender]chan bool

	// Protect requests and blockedCalls maps.
	requestMutex sync.Mutex

	holdTimeout time.Duration
}

func NewRedisDealer(node *node) Dealer {
	return &redisDealer{
		node:                node,
		waitForRegistration: true,
		requests:            make(map[ID]OutstandingRequest),
		blockedCalls:        make(map[URI]map[Sender]chan bool),
		holdTimeout:         time.Duration(node.Config.HoldCalls) * time.Second,
	}
}

type registrationInfo struct {
	ID          int64  `redis:"id"`
	SessionID   int64  `redis:"sessionid"`
	Endpoint    string `redis:"endpoint"`
	PassDetails bool   `redis:"details"`
}

type registrationIterator struct {
	conn   redis.Conn
	key    string
	cursor int
	values []string
}

func storeRegistration(pool *redis.Pool, endpoint URI, session ID, registration ID, details bool) error {
	conn := pool.Get()
	defer conn.Close()

	var result error = nil

	registrationKey := fmt.Sprintf("registration:%x:%x", session, registration)
	args := []interface{}{
		registrationKey,
		"id", int64(registration),
		"sessionid", int64(session),
		"endpoint", string(endpoint),
		"details", details,
	}

	_, err := conn.Do("HMSET", args...)
	if err != nil {
		out.Debug("Redis error on key %s: %v", registrationKey, err)
		result = err
	}

	// For now, set priority to registration time so that newer registrations
	// override older ones.
	// TODO: allow caller to set a priority, and put those in the higher bits
	// so that time still breaks ties.
	priority := time.Now().Unix()

	proceduresKey := fmt.Sprintf("procedures:%s", endpoint)
	_, err = conn.Do("ZADD", proceduresKey, priority, registrationKey)
	if err != nil {
		out.Debug("Redis error on key %s: %v", proceduresKey, err)
		result = err
	}

	registeredKey := fmt.Sprintf("registered:%x", session)
	_, err = conn.Do("SADD", registeredKey, registrationKey)
	if err != nil {
		out.Debug("Redis error on key %s: %v", registeredKey, err)
		result = err
	}

	return result
}

func removeRegistration(pool *redis.Pool, session ID, registration ID) error {
	conn := pool.Get()
	defer conn.Close()

	var result error = nil

	registrationKey := fmt.Sprintf("registration:%x:%x", session, registration)

	endpoint, err := redis.String(conn.Do("HGET", registrationKey, "endpoint"))
	if err != nil {
		out.Debug("Redis error on key %s: %v", registrationKey, err)
		return err
	} else if endpoint == "" {
		return fmt.Errorf("Registration not found")
	}

	proceduresKey := fmt.Sprintf("procedures:%s", endpoint)
	_, err = conn.Do("ZREM", proceduresKey, registrationKey)
	if err != nil {
		out.Debug("Redis error on key %s: %v", proceduresKey, err)
		result = err
	}

	registeredKey := fmt.Sprintf("registered:%x", session)
	_, err = conn.Do("SREM", registeredKey, registrationKey)
	if err != nil {
		out.Debug("Redis error on key %s: %v", registeredKey, err)
		result = err
	}

	_, err = conn.Do("DEL", registrationKey)
	if err != nil {
		out.Debug("Redis error on key %s: %v", registrationKey, err)
		result = err
	}

	return result
}

func getProcedure(pool *redis.Pool, endpoint URI) *registrationInfo {
	conn := pool.Get()
	defer conn.Close()

	proceduresKey := fmt.Sprintf("procedures:%s", endpoint)
	procedures, err := redis.Strings(conn.Do("ZREVRANGE", proceduresKey, 0, 1))
	if err != nil {
		out.Debug("Redis error on key %s: %v", proceduresKey, err)
		return nil
	}

	if len(procedures) == 0 {
		return nil
	}

	info := registrationInfo{}

	reply, err := redis.Values(conn.Do("HGETALL", procedures[0]))
	if err != nil {
		out.Debug("Redis error on key %s: %v", procedures[0], err)
		return nil
	}

	err = redis.ScanStruct(reply, &info)
	if err != nil {
		out.Debug("Redis error on key %s: %v", procedures[0], err)
		return nil
	}

	return &info
}

func (d *redisDealer) Register(callee *Session, msg *Register) *MessageEffect {
	// Endpoint may contain a # sign to pass comma-separated tags.
	// Example: pd.agent/function#details
	parts := strings.SplitN(string(msg.Procedure), "#", 2)
	endpoint := URI(parts[0])

	tags := make(map[string]bool)
	if len(parts) > 1 {
		for _, tag := range strings.Split(parts[1], ",") {
			tags[tag] = true
		}
	}

	reg := NewID()

	storeRegistration(d.node.RedisPool, endpoint, callee.Id, reg, tags["details"])

	d.requestMutex.Lock()
	for _, waiting := range d.blockedCalls[endpoint] {
		waiting <- true
	}
	delete(d.blockedCalls, endpoint)
	d.requestMutex.Unlock()

	//log.Printf("registered procedure %v [%v]", reg, msg.Procedure)
	callee.Send(&Registered{
		Request:      msg.Request,
		Registration: reg,
	})

	return NewMessageEffect(endpoint, "Registered", reg)
}

func (d *redisDealer) Unregister(callee *Session, msg *Unregister) *MessageEffect {
	err := removeRegistration(d.node.RedisPool, callee.Id, msg.Registration)
	if err == nil {
		callee.Send(&Unregistered{
			Request: msg.Request,
		})
		return NewMessageEffect("", "Unregistered", msg.Registration)
	} else {
		callee.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrNoSuchRegistration,
		})
		return NewErrorMessageEffect("", ErrNoSuchRegistration, msg.Registration)
	}
}

func (d *redisDealer) finishCall(caller Sender, msg *Call, rproc *registrationInfo, receiver Sender) *MessageEffect {
	args := msg.Arguments
	kwargs := msg.ArgumentsKw

	// Remote procedures with the PassDetails flag set will receive a
	// special first argument set by the node.
	if rproc.PassDetails {
		details := make(map[string]interface{})

		// Make sure the argument list exists first.
		if args == nil {
			args = make([]interface{}, 0)
		}

		// Does the caller want to be disclosed?
		// We default to true unless he explicitly says otherwise.
		disclose_caller, ok := msg.Options["disclose_me"].(bool)
		if !ok {
			disclose_caller = true
		}

		if disclose_caller {
			sess := caller.(*Session)
			if sess != nil {
				details["caller"] = sess.pdid
			}
		}

		// Insert as the first positional argument.
		args = append(args, nil)
		copy(args[1:], args[:])
		args[0] = details
	}

	invocationID := NewID()

	d.requestMutex.Lock()
	d.requests[invocationID] = OutstandingRequest{
		request:   msg.Request,
		procedure: msg.Procedure,
		caller:    caller,
	}
	d.requestMutex.Unlock()

	receiver.Send(&Invocation{
		Request:      invocationID,
		Registration: ID(rproc.ID),
		Details:      msg.Options,
		Arguments:    args,
		ArgumentsKw:  kwargs,
	})
	return NewMessageEffect(msg.Procedure, "", invocationID)
}

func (d *redisDealer) Call(caller Sender, msg *Call) *MessageEffect {
	var receiver *Session

	proc := getProcedure(d.node.RedisPool, msg.Procedure)
	if proc == nil {
		// No registration found at all--hold the call if we are in startup
		// mode or return an error right away.
		if !d.waitForRegistration {
			caller.Send(&Error{
				Type:    msg.MessageType(),
				Request: msg.Request,
				Details: make(map[string]interface{}),
				Error:   ErrNoSuchProcedure,
			})
			return NewErrorMessageEffect(msg.Procedure, ErrNoSuchProcedure, 0)
		}
	} else {
		var ok bool

		// We found a registration, now check if the session is available.
		// If not, hold the call while we attempt to thaw it.
		receiver, ok = d.node.GetSession(ID(proc.SessionID))
		if !ok {
			go ThawSession(d.node.RedisPool, d.node, ID(proc.SessionID))
		}
	}

	if receiver == nil {
		out.Debug("Hold CALL %s from %s", msg.Procedure, caller)

		ready := make(chan bool)

		d.requestMutex.Lock()
		if d.blockedCalls[msg.Procedure] == nil {
			d.blockedCalls[msg.Procedure] = make(map[Sender]chan bool)
		} else if d.blockedCalls[msg.Procedure][caller] != nil {
			// If this section executes, a session somehow triggered more than
			// one blocked call.  It means my assumption that there can be at
			// most one was wrong, and some restructuring of the code will be
			// necessary to handle more than one blocked call correctly.
			d.requestMutex.Unlock()

			out.Warning("Hold already in progress for %s from %s", msg.Procedure,
				caller)

			caller.Send(&Error{
				Type:    msg.MessageType(),
				Request: msg.Request,
				Details: make(map[string]interface{}),
				Error:   ErrInternalError,
			})

			return NewErrorMessageEffect(msg.Procedure, ErrInternalError, 0)
		}
		d.blockedCalls[msg.Procedure][caller] = ready
		d.requestMutex.Unlock()

		select {
		case <-time.After(d.holdTimeout):
			out.Debug("Timed out waiting for %s", msg.Procedure)

			// We timed out waiting for a registration to come in.  Delete the
			// information from the blocked calls table and send an error to
			// the caller.
			d.requestMutex.Lock()
			delete(d.blockedCalls[msg.Procedure], caller)
			d.requestMutex.Unlock()

			caller.Send(&Error{
				Type:    msg.MessageType(),
				Request: msg.Request,
				Details: make(map[string]interface{}),
				Error:   ErrNoSuchProcedure,
			})

			return NewErrorMessageEffect(msg.Procedure, ErrNoSuchProcedure, 0)
		case <-ready:
			// A registration occurred while we were waiting, so we are ready
			// to proceed to make the call.
		}

		// Try again to find a registration.
		proc = getProcedure(d.node.RedisPool, msg.Procedure)
		if proc != nil {
			receiver, _ = d.node.GetSession(ID(proc.SessionID))
		}

		// No luck--send an error.
		if proc == nil || receiver == nil {
			caller.Send(&Error{
				Type:    msg.MessageType(),
				Request: msg.Request,
				Details: make(map[string]interface{}),
				Error:   ErrNoSuchProcedure,
			})
			return NewErrorMessageEffect(msg.Procedure, ErrNoSuchProcedure, 0)
		}
	}

	return d.finishCall(caller, msg, proc, receiver)
}

func (d *redisDealer) Yield(callee Sender, msg *Yield) *MessageEffect {
	d.requestMutex.Lock()

	request, ok := d.requests[msg.Request]
	if !ok {
		d.requestMutex.Unlock()

		// WAMP spec doesn't allow sending an error in response to a YIELD message
		//log.Println("received YIELD message with invalid invocation request ID:", msg.Request)
		return NewErrorMessageEffect("", ErrNoSuchRegistration, msg.Request)
	}

	// If the progress option is set, it is a progressive result.  There should
	// be more YIELDs coming until the final YIELD which does not have the
	// progress option set.
	is_progress, ok := msg.Options["progress"].(bool)
	if !ok || !is_progress {
		delete(d.requests, msg.Request)
	}

	d.requestMutex.Unlock()

	// return the result to the caller
	request.caller.Send(&Result{
		Request:     request.request,
		Details:     msg.Options,
		Arguments:   msg.Arguments,
		ArgumentsKw: msg.ArgumentsKw,
	})

	return NewMessageEffect(request.procedure, "Result", msg.Request)
}

func (d *redisDealer) Error(peer Sender, msg *Error) *MessageEffect {
	d.requestMutex.Lock()

	request, ok := d.requests[msg.Request]
	if !ok {
		d.requestMutex.Unlock()

		// WAMP spec doesn't allow sending an error in response to a YIELD message
		//log.Println("received YIELD message with invalid invocation request ID:", msg.Request)
		return NewErrorMessageEffect("", ErrNoSuchRegistration, msg.Request)
	}

	delete(d.requests, msg.Request)

	d.requestMutex.Unlock()

	// return an error to the caller
	request.caller.Send(&Error{
		Type:        CALL,
		Request:     request.request,
		Details:     make(map[string]interface{}),
		Arguments:   msg.Arguments,
		ArgumentsKw: msg.ArgumentsKw,
		Error:       msg.Error,
	})

	return NewErrorMessageEffect(request.procedure, msg.Error, msg.Request)
}

// Unregister all handlers for a given endpoint.  Use with caution!  The
// agent(s) who registered the endpoint will not be informed that their
// registration has been removed from the node.
func (d *redisDealer) UnregisterAll(endpoint URI) int {
	conn := d.node.RedisPool.Get()
	defer conn.Close()

	proceduresKey := fmt.Sprintf("procedures:%s", endpoint)
	procedures, err := redis.Strings(conn.Do("ZREVRANGE", proceduresKey, 0, -1))
	if err != nil {
		out.Debug("Redis error on key %s: %v", proceduresKey, err)
		return 0
	}

	for _, registrationKey := range procedures {
		_, err = conn.Do("DEL", registrationKey)
		if err != nil {
			out.Debug("Redis error on key %s: %v", registrationKey, err)
		}

		_, err = conn.Do("ZREM", proceduresKey, registrationKey)
		if err != nil {
			out.Debug("Redis error on key %s: %v", proceduresKey, err)
		}

		sessionid, err := redis.Int64(conn.Do("HGET", registrationKey, "sessionid"))
		if err != nil {
			out.Debug("Redis error on key %s: %v", registrationKey, err)
			continue
		}

		registeredKey := fmt.Sprintf("registered:%x", sessionid)
		_, err = conn.Do("SREM", registeredKey, registrationKey)
		if err != nil {
			out.Debug("Redis error on key %s: %v", registeredKey, err)
		}
	}

	return len(procedures)
}

func (d *redisDealer) SetCallHolding(enable bool) {
	// Make sure no one is reading this flag at the time that we change it.
	d.requestMutex.Lock()
	d.waitForRegistration = enable
	d.requestMutex.Unlock()
}

// Remove all the registrations for a session that has disconected
func (d *redisDealer) lostSession(sess *Session) {
	// TODO: Do something about outstanding requests
}

// Testing. Not sure if this works 100 or not
func (d *redisDealer) hasRegistration(s URI) bool {
	proc := getProcedure(d.node.RedisPool, s)
	return (proc != nil)
}
