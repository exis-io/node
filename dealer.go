package node

import (
	// "reflect"
	"strconv"
	"strings"
	"sync"
)

// A Dealer routes and manages RPC calls to callees.
type Dealer interface {
	// Register a procedure on an endpoint
	Register(Sender, *Register) *MessageEffect
	// Unregister a procedure on an endpoint
	Unregister(Sender, *Unregister) *MessageEffect
	// Call a procedure on an endpoint
	Call(Sender, *Call) *MessageEffect
	// Return the result of a procedure call
	Yield(Sender, *Yield) *MessageEffect
	// Handle an ERROR message from an invocation
	Error(Sender, *Error) *MessageEffect

	// Timeout any calls that were waiting for registrations.
	ClearBlockedCalls()

	dump() string
	hasRegistration(URI) bool
	lostSession(*Session)
}

type RemoteProcedure struct {
	Endpoint    Sender
	Procedure   URI
	PassDetails bool
}

type OutstandingRequest struct {
	request   ID
	procedure URI
	caller    Sender
}

func NewRemoteProcedure(endpoint Sender, procedure URI, tags []string) RemoteProcedure {
	proc := RemoteProcedure{
		Endpoint:    endpoint,
		Procedure:   procedure,
		PassDetails: false,
	}

	for _, tag := range tags {
		switch {
		case tag == "details":
			proc.PassDetails = true
		}
	}

	return proc
}

type defaultDealer struct {
	//
	// Registrations
	//

	// map registration IDs to procedures
	procedures map[ID]RemoteProcedure

	// map procedure URIs to registration IDs
	registrations map[URI]ID

	// Keep track of registrations by session, so that we can clean up when the
	// session closes.  For each session, we have a map[URI]bool, which we are
	// using as a set of registrations (store true for register, delete for
	// unregister).
	sessionRegistrations map[Sender]map[URI]bool

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
	blockedCalls        map[URI][]chan bool

	// Protect requests and blockedCalls maps.
	requestMutex        sync.Mutex
}

func NewDefaultDealer() Dealer {
	return &defaultDealer{
		procedures:           make(map[ID]RemoteProcedure),
		registrations:        make(map[URI]ID),
		requests:             make(map[ID]OutstandingRequest),
		sessionRegistrations: make(map[Sender]map[URI]bool),
		blockedCalls:         make(map[URI][]chan bool),
		waitForRegistration:  true,
	}
}

func (d *defaultDealer) Register(callee Sender, msg *Register) *MessageEffect {
	// Endpoint may contain a # sign to pass comma-separated tags.
	// Example: pd.agent/function#details
	parts := strings.SplitN(string(msg.Procedure), "#", 2)
	endpoint := URI(parts[0])

	var tags []string
	if len(parts) > 1 {
		tags = strings.Split(parts[1], ",")
	}

	d.registrationMutex.Lock()
	if id, ok := d.registrations[endpoint]; ok {
		// Unlock before calling Send.
		d.registrationMutex.Unlock()

		//log.Println("error: procedure already exists:", msg.Procedure, id)
		out.Error("error: procedure already exists:", endpoint, id)
		callee.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrProcedureAlreadyExists,
		})
		return NewErrorMessageEffect(endpoint, ErrProcedureAlreadyExists, 0)
	}

	reg := NewID()

	d.procedures[reg] = NewRemoteProcedure(callee, endpoint, tags)
	d.registrations[endpoint] = reg

	if d.sessionRegistrations[callee] == nil {
		d.sessionRegistrations[callee] = make(map[URI]bool)
	}
	d.sessionRegistrations[callee][endpoint] = true
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

func (d *defaultDealer) Unregister(callee Sender, msg *Unregister) *MessageEffect {
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
		delete(d.sessionRegistrations[callee], procedure.Procedure)
		delete(d.registrations, procedure.Procedure)
		delete(d.procedures, msg.Registration)

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
	_, exists := d.registrations[procedure]
	return (!exists && d.waitForRegistration)
}

func (d *defaultDealer) Call(caller Sender, msg *Call) *MessageEffect {
	d.registrationMutex.RLock()

	if d.shouldHoldCall(msg.Procedure) {
		ready := make(chan bool)

		d.requestMutex.Lock()
		d.blockedCalls[msg.Procedure] = append(d.blockedCalls[msg.Procedure], ready)
		d.requestMutex.Unlock()

		// Unlock while we block on the channel.
		d.registrationMutex.RUnlock()
		<-ready
		d.registrationMutex.RLock()
	}

	if reg, ok := d.registrations[msg.Procedure]; !ok {
		d.registrationMutex.RUnlock()
		caller.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrNoSuchProcedure,
		})
		return NewErrorMessageEffect(msg.Procedure, ErrNoSuchProcedure, 0)
	} else {
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

			rproc.Endpoint.Send(&Invocation{
				Request:      invocationID,
				Registration: reg,
				Details:      map[string]interface{}{},
				Arguments:    args,
				ArgumentsKw:  kwargs,
			})
			return NewMessageEffect(msg.Procedure, "", invocationID)
		}
	}
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

	delete(d.requests, msg.Request)

	d.requestMutex.Unlock()

	// return the result to the caller
	request.caller.Send(&Result{
		Request:     request.request,
		Details:     map[string]interface{}{},
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

func (d *defaultDealer) ClearBlockedCalls() {
	// Make sure no one is reading this flag at the time that we change it.
	d.registrationMutex.Lock()
	d.waitForRegistration = false
	d.registrationMutex.Unlock()

	d.requestMutex.Lock()
	for procedure, callers := range d.blockedCalls {
		for _, caller := range callers {
			caller <- true
		}
		delete(d.blockedCalls, procedure)
	}
	d.requestMutex.Unlock()
}

// Remove all the registrations for a session that has disconected
func (d *defaultDealer) lostSession(sess *Session) {
	// TODO: Do something about outstanding requests

	d.registrationMutex.Lock()
	defer d.registrationMutex.Unlock()

	for uri, _ := range d.sessionRegistrations[sess] {
		out.Debug("Unregister: %s", string(uri))
		delete(d.procedures, d.registrations[uri])
		delete(d.registrations, uri)
	}
	delete(d.sessionRegistrations, sess)
}

func (d *defaultDealer) dump() string {
	ret := "  functions:"

	for k, v := range d.procedures {
		ret += "\n\t" + strconv.FormatUint(uint64(k), 16) + ": " + string(v.Procedure)
	}

	ret += "\n  registrations:"

	for k, v := range d.registrations {
		ret += "\n\t" + string(k) + ": " + strconv.FormatUint(uint64(v), 16)
	}

	//	ret += "\n  requests:"
	//
	//	for k, v := range d.requests {
	//		ret += "\n\t" + strconv.FormatUint(uint64(k), 16) + ": " + strconv.FormatUint(uint64(v), 16)
	//	}

	return ret
}

// Testing. Not sure if this works 100 or not
func (d *defaultDealer) hasRegistration(s URI) bool {
	d.registrationMutex.RLock()
	defer d.registrationMutex.RUnlock()

	_, exists := d.registrations[s]
	return exists
}
