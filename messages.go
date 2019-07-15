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

func NewFloodMessageManager(startTime time.Time, duration time.Duration) (MessageManager, error) {
	dbc, err := connectWithURI("mysql://root@unix(" + SockFile + ")/wirewatcher?")
	if err != nil {
		return nil, err
	}

	messages := make(map[int][]Message)
	endTime := startTime.Add(duration)
	// get unique messages from the DB over the given period
	rows, err := dbc.Query("select ANY_VALUE(channel_updates.uuid) as "+
		"`uuid`, channel_updates.chan_id,  ANY_VALUE(`timestamp`) as "+
		"`timestamp`, ANY_VALUE(byte_len)  as `byte_len`, ANY_VALUE(node_1) "+
		"as `node_1`, ANY_VALUE(node_2) as `node_2`, channel_updates.channel_flags "+
		" from channel_updates,  ln_messages, channel_announcements where "+
		"channel_updates.uuid =  ln_messages.uuid and channel_announcements.chan_id = "+
		"channel_updates.chan_id and `timestamp`>=? and `timestamp`<=? "+
		"group by channel_updates.chan_id, base_fee, fee_rate, channel_flags, "+
		"max_htlc, min_htlc, timelock_delta", startTime, endTime)
	if err != nil {
		return nil, err
	}

	var lastBucket int

	defer rows.Close()
	for rows.Next() {
		var flags int
		var node1, node2 string

		var msg ChannelUpdate
		err := rows.Scan(&msg.id, &msg.chanID, &msg.ts, &msg.byteLen, &node1, &node2, &flags)
		if err != nil {
			return nil, err
		}

		msg.Node = node1
		if flags == 1 {
			msg.Node = node2
		}

		bucket := int(msg.ts.Sub(startTime).Seconds() / 90)
		if bucket >= lastBucket {
			lastBucket = bucket
		}
		messages[bucket] = append(messages[bucket], &msg)
	}

	for bucket, ms := range messages {
		log.Printf("Bucket: %v, count: %v", bucket, len(ms))
	}

	return &floodManager{
		messages:   messages,
		lastBucket: lastBucket,
	}, nil
}

type ChannelUpdate struct {
	id      int64
	Node    string
	ts      time.Time
	chanID  string
	byteLen int
}

func (c *ChannelUpdate) UUID() int64 {
	return c.id
}

func (c *ChannelUpdate) OriginNodes() []string {
	return []string{c.Node}
}

func (c *ChannelUpdate) TimeStamp() time.Time {
	return c.ts
}

func (c *ChannelUpdate) ID() string {
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

	WriteMessageSeen(dbc, msg.UUID(), nodeID, tick)
}
