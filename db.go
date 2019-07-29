package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
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

func WriteMessageSeen(db *labelledDB, uuid int64, nodeID string, tick int) error {
	res, err := db.dbc.Exec("insert into received_messages (uuid, node_id, tick, label) "+
		"values (?,?,?,?)", uuid, nodeID, tick, db.label)
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

// GetMessageLatency returns the average number of ticks a message took to propagate,
// expectedTick is the point at which we first expected to see the message, and is used
// as a sanity check to ensure 0-default values don't skew results
func GetMessageLatency(db *labelledDB, messageID int64, expectedFirstTick int) (int, error) {
	var firstSeen int
	db.dbc.QueryRow("select min(tick) from received_messages where uuid=? and "+
		"label=?", messageID, db.label).Scan(&firstSeen)

	if firstSeen < expectedFirstTick {
		return 0, errUnexpectedFirstSeen
	}

	rows, err := db.dbc.Query("select max(tick) from received_messages where uuid=? and "+
		"label=? group by node_id", messageID, db.label)
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

	// if only 1 row was found, it is the firstSeen value and the message
	// was not propagated at all
	if n == 1 {
		return 0, nil
	}

	return total / (n - 1), nil
}

func GetDuplicateCount(db *labelledDB, messageID int64) (int, error) {
	rows, err := db.dbc.Query("select count(*) as count from received_messages "+
		"where uuid=? and label=? group by node_id having count>1", messageID, db.label)
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
