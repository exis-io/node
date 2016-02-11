package node

import (
	"fmt"
	"os"
	"sync"
	"time"
)

const messageLogHeader string = "time,type,authid,agent,endpoint,exchange,response,error\n"

type NodeStats struct {
	startTime     int64
	lock          sync.Mutex
	messages      chan string
	messageCounts map[string]int64

	MessageLog
}

type MessageLog struct {
	path  string
	file  *os.File

	write chan string

	lines    int
	maxLines int
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

func (stats *NodeStats) OpenMessageLog(path string, maxLines int) error {
	var err error

	stats.MessageLog.path = path
	stats.MessageLog.lines = 0
	stats.MessageLog.maxLines = maxLines

	stats.MessageLog.file, err = os.Create(path)
	if err == nil {
		stats.MessageLog.write = make(chan string)
		stats.MessageLog.file.WriteString(messageLogHeader)
		go stats.MessageLog.writeMessages()
	}
	return err
}

// Write the log messages out to a file and periodically rotate it.
// This is run in a go routine by OpenMessageLog.
func (log *MessageLog) writeMessages() {
	for msg := range log.write {
		log.file.WriteString(msg)
		log.lines++

		if log.lines >= log.maxLines {
			log.rotate()
		}
	}
}

func (log *MessageLog) rotate() error {
	var err error

	log.file.Close()

	newPath := log.path + time.Now().Format(".20060102_150405")
	os.Rename(log.path, newPath)

	log.lines = 0
	log.file, err = os.Create(log.path)
	if err == nil {
		log.file.WriteString(messageLogHeader)
	}
	return err
}

func (stats *NodeStats) LogMessage(sess *Session, msg *HandledMessage, effect *MessageEffect) {
	event := fmt.Sprintf("%d.%09d,%s,%s,%s,%s,%x,%s,%s\n",
			msg.Time.Unix(), msg.Time.Nanosecond(),
			msg.Type,
			sess.authid,
			sess.pdid,
			effect.Endpoint,
			effect.InternalID,
			effect.Response,
			effect.Error)

	if stats.MessageLog.write != nil {
		stats.MessageLog.write <- event
	}
}

// Returns a map of strings to counters.
func (stats *NodeStats) GetUsage() map[string]int64 {
	counts := make(map[string]int64, 0)

	stats.lock.Lock()
	for k, v := range stats.messageCounts {
		counts[k] = v
	}

	// The start time tells the receiver when our counters were initialized.
	counts["start"] = stats.startTime
	stats.lock.Unlock()

	return counts
}
