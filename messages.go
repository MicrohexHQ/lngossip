package main

import (
	"database/sql"
	"flag"
	"log"
	"math"
	"time"
)

var wirewatcher = flag.String("wirewatcher_db",
	"mysql://root@unix("+SockFile+")/wirewatcher?",
	"uri for wirewatcher DB")

type Message interface {
	// UUID is a unique ID given to the message during data gathering
	// to allow for easy correlation of LN messages and underlying detail.
	// It should only be used for DB level operations.
	UUID() int64

	// Return the ID used to uniquely identify message in the protocol
	// Short channel ID, node ID etc.
	ID() string

	// Node(s) that originally created the message.
	OriginNodes() []string

	// Timestamp of message.
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

type dbChanUpdate struct {
	id        int64
	chanID    string
	ts        time.Time
	baseFee   int
	feeRate   int
	chanFlags int
	maxHTLC   int
	minHTLC   int
	timeLock  int
}

func (u *dbChanUpdate) isDuplicate(update dbChanUpdate) bool {
	// if updates are more than 5 minutes apart we do not consider them duplicates
	if math.Abs(u.ts.Sub(update.ts).Seconds()) > (time.Minute * 5).Seconds() {
		return false
	}

	// if updates differ on any dimension they are not duplicates
	if u.chanID != update.chanID ||
		u.baseFee != update.baseFee ||
		u.feeRate != update.feeRate ||
		u.chanFlags != update.chanFlags ||
		u.maxHTLC != update.maxHTLC ||
		u.minHTLC != update.minHTLC ||
		u.timeLock != update.timeLock {
		return false
	}

	return true
}

func NewFloodMessageManager(startTime time.Time, duration time.Duration) (MessageManager, error) {
	dbc, err := connectWithURI(*wirewatcher)
	if err != nil {
		return nil, err
	}
	endTime := startTime.Add(duration)

	rows, err := dbc.Query("select uuid, chan_id, `timestamp`, base_fee,"+
		"fee_rate, channel_flags, max_htlc, min_htlc, timelock_delta"+
		" from channel_updates where `timestamp`>=? and `timestamp`<=? "+
		"order by timestamp",
		startTime, endTime)
	if err != nil {
		return nil, err
	}

	recentUpdates := make(map[string]dbChanUpdate)
	var uniqueUpdates []dbChanUpdate

	var count, uniqueCount int

	defer rows.Close()
	for rows.Next() {
		count++

		var msg dbChanUpdate
		err := rows.Scan(&msg.id, &msg.chanID, &msg.ts, &msg.baseFee,
			&msg.feeRate, &msg.chanFlags, &msg.maxHTLC, &msg.minHTLC,
			&msg.timeLock)
		if err != nil {
			return nil, err
		}

		// if we have no updates for this channel, it is not a duplicate and
		// can be added to the set of
		recent, ok := recentUpdates[msg.chanID]
		if !ok {
			uniqueCount++
			uniqueUpdates = append(uniqueUpdates, msg)
			recentUpdates[msg.chanID] = msg
			continue
		}

		// if we have an update for this channel already, only add the update
		// to our unique list of updates if it is not a duplicate.
		if !recent.isDuplicate(msg) {
			uniqueCount++
			uniqueUpdates = append(uniqueUpdates, msg)

			// if the update is more recent than the one we have on record,
			// replace it in the recentUpdates map which we use for duplicates.
			if recent.ts.Before(msg.ts) {
				recentUpdates[msg.chanID] = msg
			}
		}

	}

	log.Printf("Read in %v unique messages from %v messages", uniqueCount, count)

	var lastBucket int
	count = 0
	messages := make(map[int][]Message)

	for _, m := range uniqueUpdates {
		var byteLen int

		err := dbc.QueryRow("select byte_len from ln_messages where "+
			"uuid=?", m.id).Scan(&byteLen)
		if err != nil {
			return nil, err
		}

		var node1, node2 string
		err = dbc.QueryRow("select node_1, node_2 from "+
			"channel_announcements where chan_id=? limit 1", m.chanID).Scan(&node1, &node2)
		if err == sql.ErrNoRows {
			// we can reasonably expect that we do not have the announcement for
			// very old channels, so just skip message
			log.Printf("Cannot find channel announcment for message: %v", m.id)
			continue
		} else if err != nil {
			return nil, err
		}

		msg := &ChannelUpdate{
			id:      m.id,
			Node:    node1,
			ts:      m.ts,
			chanID:  m.chanID,
			byteLen: byteLen,
		}

		if m.chanFlags == 1 {
			msg.Node = node2
		}

		bucket := int(msg.ts.Sub(startTime).Seconds() / 90)
		if bucket >= lastBucket {
			lastBucket = bucket
		}
		messages[bucket] = append(messages[bucket], msg)

		count++
	}

	log.Printf("Read in flood manager with: %v buckets containing"+
		" %v messages", len(messages), count)

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

func ReportMessage(dbc *labelledDB, msg Message, nodeID string, tick int) error {
	return WriteMessageSeen(dbc, msg.UUID(), nodeID, tick)
}
