package main

import (
	"log"
	"time"
)

func main() {
	log.Printf("Stating simulation at %v", time.Now())

	simulate(NewFloodMessageManager(), []Node{})
	log.Printf("Ending simulation at %v", time.Now())
}

func simulate(mMgr MessageManager, nodes []Node) {
	chanGraph := NewChannelGraph(nodes)

	// get the new messages for this tick and send them to their origin
	// nodes to simulate creation of messages.

	for {
		tickCount, done := chanGraph.Tick(mMgr)
		log.Printf("Running simulation for tick: %v", tickCount)

		if done {
			break
		}
	}

}
