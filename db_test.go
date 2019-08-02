package main

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var schema = `
create table received_messages(
	uuid bigint, 
	node_id varchar(255), 
	first_seen int,
	last_seen int,
	seen_count int, 
	label varchar(100),

	primary key(uuid, node_id)
);
`

func connectAndResetForTesting(t *testing.T) *labelledDB {

	uri := os.Getenv("DB_TEST_BASE")
	if uri == "" {
		uri = "mysql://root@unix(" + SockFile + ")/test?"
	}

	dbc, err := connectWithURI(uri)
	if err != nil {
		t.Fatalf("connect error: %v", err)
		return nil
	}

	// Multiple connections are problematic for unit tests since they
	// introduce concurrency issues.
	dbc.SetMaxOpenConns(1)

	if _, err := dbc.Exec("set time_zone='+00:00';"); err != nil {
		t.Errorf("Error setting time_zone: %v", err)
	}
	_, err = dbc.Exec("set sql_mode=if(@@version<'5.7', 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION', @@sql_mode);")
	if err != nil {
		t.Errorf("Error setting strict mode: %v", err)
	}

	for _, q := range strings.Split(schema, ";") {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}

		q = strings.Replace(
			q, "create table", "create temporary table", 1)

		// Temporary tables don't support fulltext indexes.
		q = strings.Replace(
			q, "fulltext", "index", -1)

		_, err = dbc.Exec(q)
		if err != nil {
			t.Fatalf("Error executing %s: %s", q, err.Error())
			return nil
		}
	}

	return &labelledDB{
		dbc:   dbc,
		label: "test",
	}
}

func TestWriteMessageSeen(t *testing.T) {
	dbc := connectAndResetForTesting(t)

	uuid := int64(432)
	nodeID := "node 12"

	err := WriteMessageSeen(dbc, uuid, nodeID, 3)
	require.NoError(t, err)

	// Write same value, ok
	err = WriteMessageSeen(dbc, uuid, nodeID, 4)
	require.NoError(t, err)

	var firstSeen, lastSeen, seenCount int
	err = dbc.dbc.QueryRow("select first_seen, last_seen, seen_count from "+
		"received_messages where uuid=? and node_id=?", uuid, nodeID).Scan(&firstSeen,
		&lastSeen, &seenCount)
	require.NoError(t, err)

	require.Equal(t, 2, seenCount)
	require.Equal(t, 4, lastSeen)
	require.Equal(t, 3, firstSeen)
}

type entry struct {
	node string
	tick int
}

func TestGetAverageLatency(t *testing.T) {
	tests := []struct {
		name            string
		node            string
		firstSeen       int
		uuid            int64
		ticks           []entry
		expectedLatency float64
	}{
		{
			name:      "No entries, zero latency",
			node:      "node1",
			firstSeen: 1,
		},
		{
			name:      "Latency of 1, one entry",
			node:      "node1",
			firstSeen: 1,
			ticks: []entry{
				{"node2", 2},
			},
			expectedLatency: 1,
		},
		{
			name:      "Multiple entries",
			node:      "node1",
			firstSeen: 0,
			ticks: []entry{
				{"node2", 2},
				{"node3", 4},
			},
			expectedLatency: 3,
		},
		{
			name:      "Multiple entries, same node",
			node:      "node1",
			firstSeen: 0,
			ticks: []entry{
				{"node2", 2},
				{"node2", 4},
			},
			expectedLatency: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbc := connectAndResetForTesting(t)

			err := WriteMessageSeen(dbc, test.uuid, test.node, test.firstSeen)
			require.NoError(t, err)

			for _, e := range test.ticks {
				err = WriteMessageSeen(dbc, test.uuid, e.node, e.tick)
				require.NoError(t, err)
			}

			latency, err := GetAverageLatency(dbc, test.uuid)
			require.NoError(t, err)
			require.Equal(t, test.expectedLatency, latency)
		})
	}
}

func TestGetDuplicateCount(t *testing.T) {
	tests := []struct {
		name               string
		uuid               int64
		ticks              []entry
		expectedDuplicates int
	}{
		{
			name: "No duplicates",
			uuid: 10,
			ticks: []entry{
				{"node1", 1},
				{"node2", 2},
				{"node3", 3},
			},
		},
		{
			name: "No duplicates",
			uuid: 10,
			ticks: []entry{
				{"node1", 1},
				{"node1", 2},
				{"node1", 3},
			},
			expectedDuplicates: 2,
		},
		{
			name: "Duplicates across multiple nodes",
			uuid: 10,
			ticks: []entry{
				{"node1", 1},
				{"node1", 2},
				{"node1", 3},
				{"node2", 3},
				{"node3", 2},
				{"node3", 3},
			},
			expectedDuplicates: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbc := connectAndResetForTesting(t)

			for _, e := range test.ticks {
				err := WriteMessageSeen(dbc, test.uuid, e.node, e.tick)
				require.NoError(t, err)
			}

			duplicates, err := GetDuplicateCount(dbc, test.uuid)
			require.NoError(t, err)
			assert.Equal(t, test.expectedDuplicates, duplicates)
		})
	}
}

func TestGetDuplicateBucket(t *testing.T) {
	dbc := connectAndResetForTesting(t)

	uuid := int64(432)
	nodeID := "node 12"

	count, err := GetDuplicateBucket(dbc, uuid, 0)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	err = WriteMessageSeen(dbc, uuid, nodeID, 3)
	require.NoError(t, err)

	count, err = GetDuplicateBucket(dbc, uuid, 0)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	for i := 0; i < 5; i++ {
		err = WriteMessageSeen(dbc, uuid, nodeID, 3)
		require.NoError(t, err)
	}

	count, err = GetDuplicateBucket(dbc, uuid, 5)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	count, err = GetDuplicateBucket(dbc, uuid, 10)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}
