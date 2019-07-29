package main

import (
	"flag"
	"log"
	"time"
)

// dbLabel flag is used to make sure that data from separate runs of the
// simulation do not interfere with each other. It should be set to a unique
// value, or the data should be cleared per run.
var dbLabel = flag.String("db_label", "label",
	"value to label simulation data with to uniquely identify it with")

func main() {
	flag.Parse()

	dbc, err := Connect("label")
	if err != nil {
		log.Fatalf("could not connect to DB: %v", err)
	}

	log.Println("Reading in channel graph")
	nodes, err := readChanGraph()
	if err != nil {
		log.Fatalf("cannot parse channel graph: %v", err)
	}

	// TODO(carla): do for whole data range
	startTime, err := time.Parse("2006-01-02 15:04:05", "2019-07-10 14:00:00")
	if err != nil {
		log.Fatalf("cannot parse time: %v", err)
	}

	log.Println("Reading in messages")
	// TODO(carla): make this more efficient, takes aaages
	mgr, err := NewFloodMessageManager(startTime, time.Hour)
	if err != nil {
		log.Fatalf("could not load messages: %v", err)
	}

	simulate(dbc, mgr, nodes)
}

func simulate(dbc *labelledDB, mMgr MessageManager, nodes map[string]Node) {
	log.Printf("Stating simulation at %v", time.Now())
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

	log.Printf("Ending simulation at %v, unknown peers: %v, "+
		"unknown nodes: %v", time.Now(),
		float32(unknownPeers)/float32(knownPeers+unknownPeers),
		float32(unknownNodes)/float32(knownNodes+unknownNodes))
}
