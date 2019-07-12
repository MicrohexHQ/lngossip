package main

import (
	"log"
)

// var to check whether every node in the network has seen a message.
// set in NewChannelGraph, so the func must be called before using the var.
var nodeCount int

type Node interface {
	GetPubkey() string
	GetPeers() []string
	// Get the queue of receiving peer -> messages to be relayed in this round
	GetQueue() map[string][]Message
	// Simulate a node receiving a message, record metrics
	ReceiveMessage(dbc *labelledDB, msg Message, tick int, from string)
	// Move received messages from the round into relay queue
	ProgressQueue()
}

func NewChannelGraph(nodes []Node) *ChannelGraph {
	g := &ChannelGraph{
		Nodes:     make(map[string]Node),
		NodeCount: len(nodes),
	}

	for _, n := range nodes {
		g.Nodes[n.GetPubkey()] = n
	}

	nodeCount = len(nodes)

	return g
}

type ChannelGraph struct {
	// map of pubkey to node implementation
	Nodes     map[string]Node
	TickCount int
	NodeCount int
}

// Tick advances the network by one period, where a period represents
// the exchange of one wire message between peers.
func (c *ChannelGraph) Tick(dbc *labelledDB, mMgr MessageManager) (int, bool) {
	log.Printf("Running simulation for tick: %v", c.TickCount)

	// Read in messages
	messages, noMessages := mMgr.GetNewMessages(c.TickCount)
	for _, m := range messages {
		for _, node := range m.OriginNodes() {
			// nodes that messages were collected for may not have been online when
			// the network graph was collected samples, just do not propagate these messages
			n, ok := c.Nodes[node]
			if !ok {
				log.Printf("Tick: cannot find node: %v to originate message: %v", node, m.UUID())
				continue
			}

			// prompt node to receive message so that it queues it for relay
			// and reports its first sighting for latency measures
			n.ReceiveMessage(dbc, m, c.TickCount, n.GetPubkey())
		}
	}

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
				continue
			}

			for sendingPeer, messages := range queue {
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
					p.ReceiveMessage(dbc, m, c.TickCount, node.GetPubkey())
				}
			}

		}

	}

	// progress each node's queue, this is done by clearing the relay queue and
	// moving the messages received into the relay queue for propagation
	for _, n := range c.Nodes {
		n.ProgressQueue()
	}

	// if no items were relayed this tick, and we are out of network messages,
	// then we have finished relaying messages on the network
	done := queuedItems == 0 && noMessages
	c.TickCount++

	return c.TickCount, done
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

func (n *FloodNode) GetPeers() []string {
	return n.Peers
}

func (n *FloodNode) ReceiveMessage(dbc *labelledDB, msg Message, tick int, from string) {
	ReportMessage(dbc, msg, n.Pubkey, tick)

	cached, alreadySeen := n.CachedMessages[msg.ID()]

	// if we have never seen a message with this ID before,
	// or the message we stored is out of date, add to queue of things
	// to be sent
	if !alreadySeen || cached.TimeStamp().Before(msg.TimeStamp()) {
		n.ReceiveQueue[from] = append(n.ReceiveQueue[from], msg)
	}

	n.CachedMessages[msg.ID()] = msg
}

func (n *FloodNode) GetQueue() map[string][]Message {
	return n.RelayQueue
}

func (n *FloodNode) ProgressQueue() {
	n.RelayQueue = n.ReceiveQueue

	// clear receive queue because we have moved these to our broadcast queue
	n.ReceiveQueue = make(map[string][]Message)
}
