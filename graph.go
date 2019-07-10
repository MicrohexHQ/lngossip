package main

import "log"

// var to check whether every node in the network has seen a message.
// set in NewChannelGraph, so the func must be called before using the var.
var nodeCount int

type Node interface {
	GetPubkey() string
	GetPeers() []string
	// Get the queue of messages to be relayed in this round
	GetQueue() []Message
	// Progress the list of messages previously received for relay
	ProgressQueue()
	// Simulate a node receiving a message, record metrics
	ReceiveMessage(uuids Message, tick int)
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
func (c *ChannelGraph) Tick(mMgr MessageManager) (int, bool) {
	messages, noMessages := mMgr.GetNewMessages(c.TickCount)

	for _, m := range messages {
		for _, node := range m.OriginNodes() {
			n, ok := c.Nodes[node]
			if !ok {
				log.Printf("Tick: cannot find node: %v to originate message: %v", node, m.UUID())
				continue
			}

			// prompt node to receive message so that it queues it for relay
			// and reports its first sighting for latency measures
			n.ReceiveMessage(m, c.TickCount)
		}
	}

	var queuedItems int
	for pubkey, node := range c.Nodes {
		queue := node.GetQueue()
		peers := node.GetPeers()

		// track the number of items queued for sending. if there are not items
		// queued and we are out of messages, then we do not need to continue
		// the simulation
		queuedItems = queuedItems + len(queue)
		for _, peer := range peers {
			p, ok := c.Nodes[peer]
			if !ok {
				log.Printf("Tick: could not find %v's peer %v in graph", pubkey, p)
				continue
			}

			for _, m := range queue {
				// do not relay updates back to origin node
				// TODO(carla): do not relay messages to nodes who sent them to you
				var peerSentMessage bool
				for _, o := range m.OriginNodes() {
					if o == p.GetPubkey() {
						peerSentMessage = true
					}
				}
				// TODO(carla): check whether this is in spec
				if peerSentMessage {
					continue
				}

				p.ReceiveMessage(m, c.TickCount)
			}

		}

		node.ProgressQueue()
	}

	c.TickCount++
	if c.TickCount == 10 {
		return 10, true
	}
	// if no items were relayed this tick, and we are out of network messages,
	// then we have finished relaying messages on the network
	done := queuedItems == 0 && noMessages

	return c.TickCount, done
}

func MakeFloodNode(pubkey string, peers []string) Node {
	return &FloodNode{
		Pubkey:       pubkey,
		RelayQueue:   []Message{},
		ReceiveQueue: []Message{},
		SeenQueue:    make(map[int64]int),
		Peers:        peers,
	}
}

type FloodNode struct {
	// PubKey of the node being represented
	Pubkey string
	// Queue of messages to be relayed
	RelayQueue []Message
	// Queue of messages that have just been received
	ReceiveQueue []Message
	// Presistent way to track whether we've seen a message before
	SeenQueue map[int64]int
	// Nodes that we are peered with
	Peers []string
}

func (n *FloodNode) GetPubkey() string {
	return n.Pubkey
}

func (n *FloodNode) GetPeers() []string {
	return n.Peers
}

func (n *FloodNode) ReceiveMessage(msg Message, tick int) {
	uuid := msg.UUID()

	seenCount, ok := n.SeenQueue[uuid]
	if !ok {
		n.ReceiveQueue = append(n.ReceiveQueue, msg)
	}

	ReportMessage(uuid, n.Pubkey, tick, seenCount)
	n.SeenQueue[uuid] = seenCount + 1

}

func (n *FloodNode) ProgressQueue() {
	// queue up messages we have just received for relay in the next tick
	n.RelayQueue = n.ReceiveQueue

	// clear receive queue because we have moved these to our broadcast queue
	n.ReceiveQueue = []Message{}
}

func (n *FloodNode) GetQueue() []Message {
	// need to check the bolt specification
	// de-duplicate and send on a per channel basis
	return n.RelayQueue
}
