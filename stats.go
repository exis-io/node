package node

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type NodeStats struct {
	startTime     int64
	lock          sync.Mutex
	messages      chan string
	messageCounts map[string]int64
	messageLog    *os.File
}

func NewNodeStats() *NodeStats {
	stats := &NodeStats{
		startTime: int64(time.Now().Unix()),
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

// CountMessage puts a message on the queue for consumption by CountEventsRoutine.
// We are just using reflection here to log different message types.
func (stats *NodeStats) CountMessage(msg *Message) {
	typeName := messageTypeString(*msg)
	stats.messages <- typeName
}

// CountMessage puts a message on the queue for consumption by CountEventsRoutine.
// We are counting the occurences of each unique string.
func (stats *NodeStats) LogEvent(msg string) {
	stats.messages <- msg
}

func (stats *NodeStats) OpenMessageLog(path string) error {
	var err error
	stats.messageLog, err = os.Create(path)
	if err == nil {
		stats.messageLog.WriteString("time,type,authid,agent,endpoint,exchange,response,error\n")
	}
	return err
}

func (stats *NodeStats) LogMessage(sess *Session, msg *HandledMessage, effect *MessageEffect) {
	if stats.messageLog != nil {
		event := fmt.Sprintf("%d.%09d,%s,%s,%s,%s,%x,%s,%s\n",
				msg.Time.Unix(), msg.Time.Nanosecond(),
				msg.Type,
				sess.authid,
				sess.pdid,
				effect.Endpoint,
				effect.InternalID,
				effect.Response,
				effect.Error)
		stats.messageLog.WriteString(event)
	}
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

	// The start time tells the receiver when our counters were initialized.
	counts["start"] = stats.startTime
	stats.lock.Unlock()

	// Set up the return value: a one element slice containing the map of
	// strings to counts.
	result = &CallResult {
		Args: []interface{}{counts},
	}
	return
}
