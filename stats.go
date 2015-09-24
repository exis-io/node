package node

import (
	"reflect"
	"sync"
)

type NodeStats struct {
	lock          sync.Mutex
	messages      chan string
	messageCounts map[string]int64
}

func NewNodeStats() *NodeStats {
	stats := &NodeStats{
		messages: make(chan string, 1024),
		messageCounts: make(map[string]int64, 0),
	}
	go stats.CountEventsRoutine()
	return stats
}

func (stats *NodeStats) CountEventsRoutine() {
	for msg := range stats.messages {
		stats.lock.Lock()
		stats.messageCounts[msg]++
		stats.lock.Unlock()
	}
}

// LogMessage puts a message on the queue for consumption by CountEventsRoutine.
// We are just using reflection here to log different message types.
func (stats *NodeStats) LogMessage(msg *Message) {
	typeName := reflect.TypeOf(*msg).Elem().Name()
	stats.messages <- typeName
}

// LogMessage puts a message on the queue for consumption by CountEventsRoutine.
// We are counting the occurences of each unique string.
func (stats *NodeStats) LogEvent(msg string) {
	stats.messages <- msg
}

// Register the getUsage method on the node's internal agent.
func (node *node) RegisterGetUsage() {
	options := make(map[string]interface{}, 0)
	endpoint := string(node.agent.pdid + "/getUsage")
	node.agent.Register(endpoint, node.stats.GetUsage, options)
}

// GetUsage is meant to be registered as an RPC function.  Returns a map of
// strings to counters.
func (stats *NodeStats) GetUsage(args []interface{}, kwargs map[string]interface{}, details map[string]interface{}) (result *CallResult) {
	counts := make(map[string]int64, 0)

	stats.lock.Lock()
	for k, v := range stats.messageCounts {
		counts[k] = v
	}
	stats.lock.Unlock()

	// Set up the return value: a one element slice containing the map of
	// strings to counts.
	result = &CallResult {
		Args: []interface{}{counts},
	}
	return
}
