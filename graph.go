package main

import (
	"log"
)

func NewChannelGraph(nodes map[string]Node) *ChannelGraph {
	return &ChannelGraph{
		Nodes:     nodes,
		NodeCount: len(nodes),
	}
}

type ChannelGraph struct {
	// map of pubkey to node implementation
	Nodes     map[string]Node
	TickCount int
	NodeCount int
}

type tickResult struct {
	tickCount   int
	nodeUnknown int
	nodesKnown  int
	peerUnknown int
	peerKnown   int
	done        bool
}

// Tick advances the network by one period, where a period represents
// the exchange of one wire message between peers.
func (c *ChannelGraph) Tick(dbc *labelledDB, mMgr MessageManager) (*tickResult, error) {
	log.Printf("Running simulation for tick: %v", c.TickCount)
	result := &tickResult{}

	// Read in messages and "receive" them at origin nodes. This will be the first
	// record of the message that the simulation sees.
	messages, noMessages := mMgr.GetNewMessages(c.TickCount)
	for _, m := range messages {
		for _, node := range m.OriginNodes() {
			// nodes that messages were collected for may not have been online when
			// the network graph was collected samples, just do not propagate these messages
			n, ok := c.Nodes[node]
			if !ok {
				log.Printf("Tick: cannot find node: %v to originate "+
					"message: %v", node, m.UUID())
				result.nodeUnknown++
				continue
			}
			result.nodesKnown++

			// prompt node to receive message so that it queues it for relay
			// and reports its first sighting for latency measures
			if err := n.ReceiveMessage(dbc, m, c.TickCount, n.GetPubkey()); err != nil {
				return nil, err
			}
		}
	}

	log.Printf("Added %v messages for propagation", len(messages))

	// queuedItems monitors whether any messages were sent this round,
	// it is used to determine whether we should end the simulation or not
	var queuedItems int

	var nodeProgress int
	for pubkey, node := range c.Nodes {
		log.Printf("Processed sends for %v of %v nodes", nodeProgress, len(c.Nodes))
		// Get the queue of peer -> message list and send messages to each peer.
		for peer, messages := range node.GetQueue() {
			//log.Printf("Node: %v sending: %v messages to %v", pubkey, len(messages), peer)

			receivingPeer, ok := c.Nodes[peer]
			if !ok {
				log.Printf("Tick: could not find %v's peer %v in graph", pubkey, peer)
				result.peerUnknown++
				continue
			}
			result.peerKnown++

			for _, msg := range messages {
				// track the number of items sent. if there are no items
				// queued and we are out of messages, then we do not need to continue
				// the simulation
				queuedItems++

				// send message to peer
				if err := receivingPeer.ReceiveMessage(dbc, msg, c.TickCount, node.GetPubkey()); err != nil {
					return nil, err
				}

			}

		}
		nodeProgress++
	}

	log.Printf("Propagated %v messages", queuedItems)

	// progress each node's queue, this is done by clearing the relay queue and
	// moving the messages received into the relay queue for propagation
	for _, n := range c.Nodes {
		n.ProgressQueue()
	}

	c.TickCount++

	// if no items were relayed this tick, and we are out of network messages,
	// then we have finished relaying messages on the network
	result.done = queuedItems == 0 && noMessages
	result.tickCount = c.TickCount

	return result, nil
}
