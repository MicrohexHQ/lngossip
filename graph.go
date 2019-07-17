package main

import (
	"log"
)

type Node interface {
	GetPubkey() string
	GetPeers() []string
	// Get the queue of receiving peer -> messages to be relayed in this round
	GetQueue() map[string][]Message
	// Simulate a node receiving a message, record metrics
	ReceiveMessage(dbc *labelledDB, msg Message, tick int, from string) error
	// Move received messages from the round into relay queue
	ProgressQueue()
	// Add peer to a given node
	AddPeer(peer string)
}

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

	// Read in messages
	messages, noMessages := mMgr.GetNewMessages(c.TickCount)
	for _, m := range messages {
		for _, node := range m.OriginNodes() {
			// nodes that messages were collected for may not have been online when
			// the network graph was collected samples, just do not propagate these messages
			n, ok := c.Nodes[node]
			if !ok {
				log.Printf("Tick: cannot find node: %v to originate message: %v", node, m.UUID())
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

	for pubkey, node := range c.Nodes {
		queue := node.GetQueue()
		peers := node.GetPeers()

		for _, peer := range peers {
			p, ok := c.Nodes[peer]
			if !ok {
				log.Printf("Tick: could not find %v's peer %v in graph", pubkey, p)
				result.peerUnknown++
				continue
			}
			result.peerKnown++

			log.Printf("Node: %v has a queue with %v items in it",
				node.GetPubkey(), len(queue))

			for sendingPeer, messages := range queue {
				log.Printf("Queue has: %v messages in it ", messages)
				// must not send to peer that sent to us
				if peer == sendingPeer {
					continue
				}

				for _, m := range messages {
					// track the number of items sent. if there are not items
					// queued and we are out of messages, then we do not need to continue
					// the simulation
					queuedItems++

					// send message to peer
					if err := p.ReceiveMessage(dbc, m, c.TickCount, node.GetPubkey()); err != nil {
						return nil, err
					}
				}
			}

		}

	}

	log.Printf("Propageted %v messages", queuedItems)

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

func MakeFloodNode(pubkey string, peers []string) Node {
	return &FloodNode{
		Pubkey:         pubkey,
		RelayQueue:     make(map[string][]Message),
		ReceiveQueue:   make(map[string][]Message),
		Peers:          peers,
		CachedMessages: make(map[string]Message),
	}
}

type FloodNode struct {
	// PubKey of the node being represented
	Pubkey string
	// Queue of messages to be relayed
	RelayQueue map[string][]Message
	// Queue of messages that have just been received
	ReceiveQueue map[string][]Message
	// Pubkeys of peers
	Peers []string
	// Map protocol ID to message
	CachedMessages map[string]Message
}

func (n *FloodNode) GetPubkey() string {
	return n.Pubkey
}

func (n *FloodNode) AddPeer(peer string) {
	// do not add duplicate peers
	for _, p := range n.Peers {
		if p == peer {
			return
		}
	}
	n.Peers = append(n.Peers, peer)
}

func (n *FloodNode) GetPeers() []string {
	return n.Peers
}

func (n *FloodNode) ReceiveMessage(dbc *labelledDB, msg Message, tick int, from string) error {
	if err := ReportMessage(dbc, msg, n.Pubkey, tick); err != nil {
		return err
	}

	cached, alreadySeen := n.CachedMessages[msg.ID()]

	//log.Printf("Node %v receiving message %v from %v",
	//	n.Pubkey, msg.UUID(), from)

	// if we have never seen a message with this ID before,
	// or the message we stored is out of date, add to queue of things
	// to be sent
	if !alreadySeen || cached.TimeStamp().Before(msg.TimeStamp()) {
		n.ReceiveQueue[from] = append(n.ReceiveQueue[from], msg)
	}

	n.CachedMessages[msg.ID()] = msg

	return nil
}

func (n *FloodNode) GetQueue() map[string][]Message {
	return n.RelayQueue
}

func (n *FloodNode) ProgressQueue() {
	n.RelayQueue = n.ReceiveQueue

	// clear receive queue because we have moved these to our broadcast queue
	n.ReceiveQueue = make(map[string][]Message)
}
