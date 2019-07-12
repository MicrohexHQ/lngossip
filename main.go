package main

import (
	"flag"
	"log"
	"time"
)

var dbLabel = flag.String("db_label", "label", "value to label simulation data with to uniquely identify it with")

func main() {
	flag.Parse()

	dbc, err := Connect("label")
	if err != nil {
		log.Fatalf("could not connect to DB: %v", err)
	}

	simulate(dbc, NewFloodMessageManager(), []Node{})
}

func simulate(dbc *labelledDB,mMgr MessageManager, nodes []Node) {
	log.Printf("Stating simulation at %v", time.Now())
	chanGraph := NewChannelGraph(nodes)

	// get the new messages for this tick and send them to their origin
	// nodes to simulate creation of messages.
	for {
		_, done := chanGraph.Tick(dbc,mMgr)

		if done {
			break
		}
	}

	log.Printf("Ending simulation at %v", time.Now())
}
