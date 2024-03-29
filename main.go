package main

import (
	"flag"
	"log"
	"time"
)

// dbLabel flag is used to make sure that data from separate runs of the
// simulation do not interfere with each other. It should be set to a unique
// value, or the data should be cleared per run.
var (
	dbLabel = flag.String("db_label", "label",
		"value to label simulation data with to uniquely identify it with")

	startTime = flag.String("start_time", "2019-07-10 14:00:00",
		"start time in your dataset, must be expressed in format provided")

	duration = flag.Int("duration_minutes", 60,
		"amount of messages to load (specified in time)")
)

func main() {
	flag.Parse()

	dbc, err := Connect(*dbLabel)
	if err != nil {
		log.Fatalf("could not connect to DB: %v", err)
	}

	log.Println("Reading in channel graph")
	nodes, err := readChanGraph()
	if err != nil {
		log.Fatalf("cannot parse channel graph: %v", err)
	}

	startTime, err := time.Parse("2006-01-02 15:04:05", *startTime)
	if err != nil {
		log.Fatalf("cannot parse time: %v", err)
	}

	log.Println("Reading in messages")
	duration := time.Duration(*duration)
	mgr, err := NewFloodMessageManager(startTime, time.Minute*duration)
	if err != nil {
		log.Fatalf("could not load messages: %v", err)
	}

	simulate(dbc, mgr, nodes)

	// print out latency and duplicate summaries for nodes.
	summaries, err := GetSummary(dbc)
	if err != nil {
		log.Fatalf("could not get summary: %v", err)
	}
	for _, s := range summaries {
		s.print()
	}
}

func simulate(dbc *labelledDB, mMgr MessageManager, nodes map[string]Node) {
	start := time.Now()
	log.Printf("Stating simulation at %v", start)
	chanGraph := NewChannelGraph(nodes)

	// track the number of peers that we could not find in the chan graph
	// to relay messages to and the number of nodes we could not find in
	// the graph that have messages originating from them in the dataset
	var unknownPeers, knownPeers, unknownNodes, knownNodes int

	// get the new messages for this tick and send them to their origin
	// nodes to simulate creation of messages.
	for {
		result, err := chanGraph.Tick(dbc, mMgr)
		if err != nil {
			log.Fatal(err)
		}

		unknownPeers += result.peerUnknown
		knownPeers += result.peerKnown
		unknownNodes += result.nodeUnknown
		knownNodes += result.nodesKnown

		if result.done {
			break
		}
	}

	log.Printf("Ending simulation at %v, Runtime: %v, unknown peers: %v, "+
		"unknown nodes: %v", time.Now(), time.Now().Sub(start),
		float32(unknownPeers)/float32(knownPeers+unknownPeers),
		float32(unknownNodes)/float32(knownNodes+unknownNodes))
}
