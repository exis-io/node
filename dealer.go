package rabric

import (
	// "reflect"
	"strconv"
	"strings"
)

// A Dealer routes and manages RPC calls to callees.
type Dealer interface {
	// Register a procedure on an endpoint
	Register(Sender, *Register)
	// Unregister a procedure on an endpoint
	Unregister(Sender, *Unregister)
	// Call a procedure on an endpoint
	Call(Sender, *Call)
	// Return the result of a procedure call
	Yield(Sender, *Yield)
	// Handle an ERROR message from an invocation
	Error(Sender, *Error)
	dump() string
	hasRegistration(string) bool
	lostSession(Session)
}

type RemoteProcedure struct {
	Endpoint  Sender
	Procedure URI
}

type defaultDealer struct {
	// map registration IDs to procedures
	procedures map[ID]RemoteProcedure
	// map procedure URIs to registration IDs
	registrations map[URI]ID
	// keep track of call IDs so we can send the response to the caller
	calls map[ID]Sender
	// link the invocation ID to the call ID
	invocations map[ID]ID
}

func NewDefaultDealer() Dealer {
	return &defaultDealer{
		procedures:    make(map[ID]RemoteProcedure),
		registrations: make(map[URI]ID),
		calls:         make(map[ID]Sender),
		invocations:   make(map[ID]ID),
	}
}

func (d *defaultDealer) Register(callee Sender, msg *Register) {
	if id, ok := d.registrations[msg.Procedure]; ok {
		//log.Println("error: procedure already exists:", msg.Procedure, id)
		out.Error("error: procedure already exists:", msg.Procedure, id)
		callee.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrProcedureAlreadyExists,
		})
		return
	}
	reg := NewID()
	d.procedures[reg] = RemoteProcedure{callee, msg.Procedure}
	d.registrations[msg.Procedure] = reg
	//log.Printf("registered procedure %v [%v]", reg, msg.Procedure)
	callee.Send(&Registered{
		Request:      msg.Request,
		Registration: reg,
	})
}

func (d *defaultDealer) Unregister(callee Sender, msg *Unregister) {
	if procedure, ok := d.procedures[msg.Registration]; !ok {
		// the registration doesn't exist
		//log.Println("error: no such registration:", msg.Registration)
		callee.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrNoSuchRegistration,
		})
	} else {
		delete(d.registrations, procedure.Procedure)
		delete(d.procedures, msg.Registration)
		//log.Printf("unregistered procedure %v [%v]", procedure.Procedure, msg.Registration)
		callee.Send(&Unregistered{
			Request: msg.Request,
		})
	}
}

func (d *defaultDealer) Call(caller Sender, msg *Call) {
	if reg, ok := d.registrations[msg.Procedure]; !ok {
		caller.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrNoSuchProcedure,
		})
	} else {
		if rproc, ok := d.procedures[reg]; !ok {
			// found a registration id, but doesn't match any remote procedure
			caller.Send(&Error{
				Type:    msg.MessageType(),
				Request: msg.Request,
				Details: make(map[string]interface{}),
				// TODO: what should this error be?
				Error: URI("wamp.error.internal_error"),
			})
		} else {
			// everything checks out, make the invocation request
			// TODO: make the Request ID specific to the caller
			d.calls[msg.Request] = caller
			invocationID := NewID()
			d.invocations[invocationID] = msg.Request
			rproc.Endpoint.Send(&Invocation{
				Request:      invocationID,
				Registration: reg,
				Details:      map[string]interface{}{},
				Arguments:    msg.Arguments,
				ArgumentsKw:  msg.ArgumentsKw,
			})
			//log.Printf("dispatched CALL %v [%v] to callee as INVOCATION %v",
			// msg.Request, msg.Procedure, invocationID,
			// )
		}
	}
}

func (d *defaultDealer) Yield(callee Sender, msg *Yield) {
	if callID, ok := d.invocations[msg.Request]; !ok {
		// WAMP spec doesn't allow sending an error in response to a YIELD message
		//log.Println("received YIELD message with invalid invocation request ID:", msg.Request)
	} else {
		delete(d.invocations, msg.Request)
		if caller, ok := d.calls[callID]; !ok {
			// found the invocation id, but doesn't match any call id
			// WAMP spec doesn't allow sending an error in response to a YIELD message
			//log.Printf("received YIELD message, but unable to match it (%v) to a CALL ID", msg.Request)
		} else {
			delete(d.calls, callID)
			// return the result to the caller
			caller.Send(&Result{
				Request:     callID,
				Details:     map[string]interface{}{},
				Arguments:   msg.Arguments,
				ArgumentsKw: msg.ArgumentsKw,
			})
			//log.Printf("returned YIELD %v to caller as RESULT %v", msg.Request, callID)
		}
	}
}

func (d *defaultDealer) Error(peer Sender, msg *Error) {
	if callID, ok := d.invocations[msg.Request]; !ok {
		//log.Println("received ERROR (INVOCATION) message with invalid invocation request ID:", msg.Request)
	} else {
		delete(d.invocations, msg.Request)
		if caller, ok := d.calls[callID]; !ok {
			//log.Printf("received ERROR (INVOCATION) message, but unable to match it (%v) to a CALL ID", msg.Request)
		} else {
			delete(d.calls, callID)
			// return an error to the caller
			caller.Send(&Error{
				Type:        CALL,
				Request:     callID,
				Details:     make(map[string]interface{}),
				Arguments:   msg.Arguments,
				ArgumentsKw: msg.ArgumentsKw,
			})
			//log.Printf("returned ERROR %v to caller as ERROR %v", msg.Request, callID)
		}
	}
}

// Remove all the registrations for a session that has disconected
func (d *defaultDealer) lostSession(sess Session) {
	rem := make([]URI, 0)

	for k, v := range d.registrations {
		if strings.Contains(string(k), string(sess.pdid)+"/") {
			out.Debug("Deleting %s %s", k, v)
			rem = append(rem, k)
		}
	}

	for _, x := range rem {
		delete(d.procedures, d.registrations[x])
		delete(d.registrations, x)
	}
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

	ret += "\n  calls:"

	for k, _ := range d.calls {
		ret += "\n\t" + strconv.FormatUint(uint64(k), 16) + ": (sender)"
	}

	ret += "\n  invocations:"

	for k, v := range d.invocations {
		ret += "\n\t" + strconv.FormatUint(uint64(k), 16) + ": " + strconv.FormatUint(uint64(v), 16)
	}

	return ret
}

// Testing. Not sure if this works 100 or not
func (d *defaultDealer) hasRegistration(s string) bool {
	_, exists := d.registrations[URI(s)]
	return exists
}
