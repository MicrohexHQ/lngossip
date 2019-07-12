package main

import (
	"log"
	"time"
)

type Message interface {
	UUID() int64
	// Return the ID used to uniquely identify message in the protocol
	// Short channel ID, node ID etc
	ID() string
	// Nodes that originally creates the message
	OriginNodes() []string
	// Timestamp of message
	TimeStamp() time.Time
}

type floodManager struct {
	// Buckets of messages based on tick index
	messages   map[int][]Message
	lastBucket int
}

type MessageManager interface {
	// get messages returns an array of messages and a 'done' bool indicating
	// if message list has been exhausted; if true, you should stop getting messages
	GetNewMessages(tick int) ([]Message, bool)
}

func NewFloodMessageManager() MessageManager {
	// TODO(carla): read in messages from DB here

	return &floodManager{
		messages: make(map[int][]Message),
	}
}

type ChannelUpdate struct {
	id   int64
	Node string
	ts time.Time
	chanID string
}

func (c *ChannelUpdate) UUID() int64 {
	return c.id
}

func (c *ChannelUpdate) OriginNodes() []string {
	return []string{c.Node}
}

func (c * ChannelUpdate)TimeStamp()time.Time{
	return c.ts
}

func (c * ChannelUpdate)ID()string{
	return c.chanID
}

func (f *floodManager) GetNewMessages(tick int) ([]Message, bool) {
	m, ok := f.messages[tick]
	if !ok {
		log.Printf("No more new messages for tick: %v", tick)
		return []Message{}, tick >= f.lastBucket
	}

	return m, tick >= f.lastBucket
}

func ReportMessage(dbc *labelledDB, msg Message, nodeID string, tick int) {
	if nodeCount == 0 {
		log.Fatal("Node count is 0, has not been initialized, " +
			"see comment on var")
	}

	WriteMessageSeen(dbc, msg.UUID(),nodeID, tick )
}
