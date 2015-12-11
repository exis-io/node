package node

import (
	"math/rand"
)

type RegistrationHolder interface {
	Add(ID) bool
	Remove(ID) bool
	CanAdd() bool
	HasHandler() bool
	GetHandler() (ID, bool)
}

//
// SingleRegistrationHolder allows only one registration at a time.
//

type SingleRegistrationHolder struct {
	handler ID
	isSet bool
}

func NewSingleRegistrationHolder() *SingleRegistrationHolder {
	return &SingleRegistrationHolder{
		handler: 0,
		isSet: false,
	}
}

func (reg *SingleRegistrationHolder) Add(id ID) bool {
	if reg.isSet {
		return false
	} else {
		reg.handler = id
		reg.isSet = true
		return true
	}
}

func (reg *SingleRegistrationHolder) Remove(id ID) bool {
	if reg.isSet && reg.handler == id {
		reg.isSet = false
		return true
	} else {
		return false
	}
}

func (reg *SingleRegistrationHolder) CanAdd() bool {
	return !reg.isSet
}

func (reg *SingleRegistrationHolder) HasHandler() bool {
	return reg.isSet
}

func (reg *SingleRegistrationHolder) GetHandler() (ID, bool) {
	return reg.handler, reg.isSet
}

//
// MultiRegistrationHolder allows multiple agents to register the same endpoint.
// When asked for a handler, it will randomly select from the registered
// handlers.
//

type MultiRegistrationHolder struct {
	handlerList []ID
	handlerSet map[ID]bool
}

func NewMultiRegistrationHolder() *MultiRegistrationHolder {
	return &MultiRegistrationHolder{
		handlerList: make([]ID, 0),
		handlerSet: make(map[ID]bool),
	}
}

func (reg *MultiRegistrationHolder) Add(id ID) bool {
	_, exists := reg.handlerSet[id]
	if exists {
		return false
	}

	reg.handlerList = append(reg.handlerList, id)
	reg.handlerSet[id] = true
	return true
}

func (reg *MultiRegistrationHolder) Remove(id ID) bool {
	_, exists := reg.handlerSet[id]
	if !exists {
		return false
	}

	delete(reg.handlerSet, id)

	i := 0
	reg.handlerList = make([]ID, len(reg.handlerSet))
	for k, _ := range reg.handlerSet {
		reg.handlerList[i] = k
		i++
	}

	return true
}

func (reg *MultiRegistrationHolder) CanAdd() bool {
	return true
}

func (reg *MultiRegistrationHolder) HasHandler() bool {
	return len(reg.handlerList) > 0
}

func (reg *MultiRegistrationHolder) GetHandler() (ID, bool) {
	n := len(reg.handlerList)
	if n == 0 {
		return 0, false
	}

	i := rand.Intn(n)
	return reg.handlerList[i], true
}
