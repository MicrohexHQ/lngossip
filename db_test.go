package main

import (
	"database/sql"
	"github.com/stretchr/testify/require"
	"os"
	"strings"

	"testing"
)

var schema = `
create table received_messages(
	uuid bigint, 
	node_id varchar(255), 
	tick int
);
`

func connectAndResetForTesting(
	t *testing.T) *sql.DB {

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

	return dbc
}

func TestWriteMessageSeen(t *testing.T){
	dbc:=connectAndResetForTesting(t)

	err:= WriteMessageSeen(dbc, 4321, "node", 3)
	require.NoError(t, err)
}

func TestGetMessageLatency(t *testing.T){
	type entry struct{
		node string
		tick int
	}
	tests := []struct{
		name string
		node string
		expectedFirstTick int
		uuid int64
		ticks []entry
		expectedLatency int
		error error
	}{
		{
			name:"Unexpected first seen",
			node: "node1",
			expectedFirstTick: 1,
			ticks: []entry{
				{"node2",0},
			},
			error:errUnexpectedFirstSeen,
		},
		{
			name:"No entries, zero latency",
			node: "node1",
			expectedFirstTick: 1,
		},
		{
			name:"Latency of 1, one entry",
			node: "node1",
			expectedFirstTick: 1,
			ticks: []entry{
				{"node2",2},
			},
			expectedLatency:1,
		},
		{
			name:"Multiple entries",
			node: "node1",
			expectedFirstTick: 0,
			ticks:  []entry{
				{"node2",2},
				{"node3",4},
			},
			expectedLatency:3,
		},
		{
			name:"Multiple entries, same node",
			node: "node1",
			expectedFirstTick: 0,
			ticks:  []entry{
				{"node2",2},
				{"node2",4},
			},
			expectedLatency:4,
		},
	}

	for _, test:= range tests{
		t.Run(test.name, func(t *testing.T) {
			dbc:= connectAndResetForTesting(t)

			err:= WriteMessageSeen(dbc, test.uuid, test.node, test.expectedFirstTick)
			require.NoError(t, err)

			for _, e := range test.ticks{
				err= WriteMessageSeen(dbc, test.uuid, e.node, e.tick)
				require.NoError(t, err)
			}

			latency, err:= GetMessageLatency(dbc, test.uuid, test.expectedFirstTick)
			require.Equal(t, test.error, err)
			require.Equal(t, test.expectedLatency, latency)
		})
	}
}
