package main

import "log"

type Message interface {
	UUID() int64
	OriginNodes() []string
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
}

func (c *ChannelUpdate) UUID() int64 {
	return c.id
}

func (c *ChannelUpdate) OriginNodes() []string {
	return []string{c.Node}
}

func (f *floodManager) GetNewMessages(tick int) ([]Message, bool) {
	m, ok := f.messages[tick]
	if !ok {
		log.Printf("No more new messages for tick: %v", tick)
		return []Message{}, tick >= f.lastBucket
	}

	return m, tick >= f.lastBucket
}

var reportedMessage = make(map[int64]int)

func ReportMessage(uuid int64, nodeID string, tick, seenCount int) {
	if nodeCount == 0 {
		log.Fatal("Node count is 0, has not been initialized, " +
			"see comment on var")
	}

	// TODO(carla): persist all message sightings by nodes;
	// latency for node = node tick - lowest tick for message uuid
	// duplicates for node = count of ticks per node and message
	log.Printf("Message: %v seen by: %v at tick: %v", uuid, nodeID, tick)

	// message has already been seen by this node, we do not need
	// to add it to the total count of messages in the network
	if seenCount != 0 {
		return
	}

	totalSightings := reportedMessage[uuid]
	reportedMessage[uuid] = totalSightings + 1

	// TODO(carla): persist point when whole network has message
	if reportedMessage[uuid] == nodeCount {
		log.Printf("Message: %v seen by entire network of %v nodes", uuid, nodeCount)
	}
}
