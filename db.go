package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var SockFile = getSocketFile()

var dbURI = flag.String("db", "mysql://root@unix("+SockFile+")/lngossip?",
	"Database URI")

func getSocketFile() string {
	var sock = "/tmp/mysql.sock"
	if _, err := os.Stat(sock); os.IsNotExist(err) {
		// try common linux/Ubuntu socket file location
		return "/var/run/mysqld/mysqld.sock"
	}
	return sock
}

// labelledDB adds a label to each line of data to distinguish between simulations
type labelledDB struct {
	dbc   *sql.DB
	label string
}

func Connect(label string) (*labelledDB, error) {
	dbc, err := connectWithURI(*dbURI)
	if err != nil {
		return nil, err
	}

	var labelCount int
	dbc.QueryRow("select count(*) from received_messages where label=?", label).Scan(&labelCount)

	if labelCount != 0 {
		return nil, errors.New("must have unique label for simulation")
	}

	return &labelledDB{
		dbc:   dbc,
		label: label,
	}, nil
}

func connectWithURI(connectStr string) (*sql.DB, error) {
	const prefix = "mysql://"
	if !strings.HasPrefix(connectStr, prefix) {
		return nil, errors.New("db: URI is missing mysql:// prefix")
	}
	connectStr = connectStr[len(prefix):]

	if connectStr[len(connectStr)-1] != '?' {
		connectStr += "&"
	}
	connectStr += "parseTime=true&collation=utf8mb4_general_ci"

	dbc, err := sql.Open("mysql", connectStr)
	if err != nil {
		return nil, err
	}

	dbc.SetMaxOpenConns(100)
	dbc.SetMaxIdleConns(50)
	dbc.SetConnMaxLifetime(time.Minute)

	return dbc, nil
}

// WriteMessageSeen logs the tick at which a message was seen by a node.
// It may be called multiple times for a given node and message.
func WriteMessageSeen(db *labelledDB, uuid int64, nodeID string, tick int) error {
	var newRecord bool

	var firstSeen, lastSeen, seenCount int
	err := db.dbc.QueryRow("select first_seen, last_seen, "+
		"seen_count from received_messages where uuid=? and node_id=? and label=?",
		uuid, nodeID, db.label).Scan(&firstSeen, &lastSeen, &seenCount)
	// if there is not an entry of the node and uuid, this is a new record in the DB.
	if err == sql.ErrNoRows {
		newRecord = true
	} else if err != nil {
		return err
	}

	// if we have seen the node has seen the message before, update the last
	// seen and count.
	query := fmt.Sprintf("update received_messages set last_seen=%v, "+
		"seen_count=%v where uuid=%v and node_id=\"%v\" and label=\"%v\"", tick, seenCount+1,
		uuid, nodeID, db.label)
	// if this is the first time the message has been seen, create a new record.
	if newRecord {
		query = fmt.Sprintf("insert into received_messages "+
			"(uuid, node_id, first_seen, last_seen, seen_count, label) "+
			"values (%v,\"%v\",%v,%v,%v,\"%v\")", uuid, nodeID, tick, tick, 1, db.label)
	}

	res, err := db.dbc.Exec(query)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n != 1 {
		return fmt.Errorf("expected 1 row affected, got %v", n)
	}

	return nil
}

var (
	errUnexpectedFirstSeen = errors.New("first record of message earlier than expected")
	errNegativeLatency     = errors.New("negative latency calculated")
)

// GetMessageLatency gets the number of ticks it took a message to propagate fully.
func GetMessageLatency(db *labelledDB, messageID int64) (int, error) {
	var firstSeen, lastSeen int

	err := db.dbc.QueryRow("select min(first_seen), max(first_seen) from received_messages "+
		"where uuid=? and label=?", messageID, db.label).Scan(&firstSeen, &lastSeen)
	if err != nil {
		return 0, err
	}

	return lastSeen - firstSeen, nil
}

func GetAverageLatency(db *labelledDB, messageID int64) (float64, error) {
	var firstSeen int

	err := db.dbc.QueryRow("select min(first_seen) from received_messages "+
		"where uuid=? and label=?", messageID, db.label).Scan(&firstSeen)
	if err != nil {
		return 0, err
	}

	rows, err := db.dbc.Query("select first_seen from received_messages "+
		"where uuid=? and label=?", messageID, db.label)
	if err != nil {
		return 0, err
	}

	var total, n int

	defer rows.Close()
	for rows.Next() {
		var lastSeen int
		err := rows.Scan(&lastSeen)
		if err != nil {
			return 0, err
		}

		latency := lastSeen - firstSeen

		// sanity check
		if latency < 0 {
			return 0, errNegativeLatency

		}

		total += latency
		n++
	}

	// there is a single entry for the message (it did not propagate)
	if n == 1 {
		return float64(total), nil
	}

	// The original entry is included in total, so the result will be off
	// by one unless we account for it
	return float64(total) / float64(n-1), nil
}

// GetDuplicateCount returns the number of times a message was received by a
// node which already has it.
func GetDuplicateCount(db *labelledDB, messageID int64) (int, error) {
	rows, err := db.dbc.Query("select seen_count from received_messages "+
		"where uuid=? and label=? and seen_count>1", messageID, db.label)
	if err != nil {
		return 0, err
	}

	var total int

	defer rows.Close()
	for rows.Next() {
		var count int
		err := rows.Scan(&count)
		if err != nil {
			return 0, err
		}

		total = total + (count - 1)
	}

	return total, nil
}

// GetDuplicateBucket returns the number of nodes which received a message
// duplicateCount times (special case 0 returns the total number of recipients).
func GetDuplicateBucket(db *labelledDB, messageID int64, duplicateCount int) (int, error) {
	var total int

	err := db.dbc.QueryRow("select count(*) from received_messages "+
		"where uuid=? and label=? and seen_count>?", messageID, db.label,
		duplicateCount).Scan(&total)
	if err != nil {
		return 0, err
	}

	return total, nil
}

type summary struct {
	messageID        int64
	latency          int
	duplicateBuckets map[int]int
	averageLatency   float64
}

func (s *summary) print() {
	log.Printf("Summary for message: %v, latency: %v, average ticks: %v",
		s.messageID, s.latency, s.averageLatency)

	for k, v := range s.duplicateBuckets {
		log.Printf("Nodes that received message more than %v times: %v", k, v)
	}
	log.Println()
}

// Return a summary for every message sent during the simulation. This includes
// the latency for the message to propagate and the duplicate count.
func GetSummary(db *labelledDB) ([]summary, error) {
	rows, err := db.dbc.Query("select distinct uuid from received_messages")
	if err != nil {
		return nil, err
	}

	var summaries []summary

	defer rows.Close()
	for rows.Next() {
		var uuid int64
		err := rows.Scan(&uuid)
		if err != nil {
			return nil, err
		}

		// latency is the difference between the node that first saw a a message
		// and the node that last saw a message
		latency, err := GetMessageLatency(db, uuid)
		if err != nil {
			return nil, err
		}

		averageLatency, err := GetAverageLatency(db, uuid)
		if err != nil {
			return nil, err
		}

		summary := summary{
			messageID:      uuid,
			latency:        latency,
			averageLatency: averageLatency,
		}

		buckets := make(map[int]int)
		// get count of messages that have more than x reciepts of the message
		for _, i := range []int{0, 1, 5, 10, 100} {
			bucket, err := GetDuplicateBucket(db, uuid, i)
			if err != nil {
				return nil, err
			}

			buckets[i] = bucket
		}
		summary.duplicateBuckets = buckets
		summaries = append(summaries, summary)
	}

	return summaries, nil
}
