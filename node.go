package main

type Node interface {
	// GetPubKey returns the pubkey of this node
	GetPubkey() string

	// GetPeers returns the pubkeys of the nodes the node is connected to.
	GetPeers() []string

	// ProgressQueue *must* be called before GetQueue when simulating the sending
	// of gossip messages. It builds a node's set of messages to be relayed in the
	// next round of simulation based on what it has received from its peers.
	ProgressQueue()

	// GetQueue returns a map of peer pubkey to a list of messages that the
	// peer needs to relay to that peer. It should be used in conjunction
	// with ProgressQueue, which prepares the queue of messages for each peer.
	GetQueue() map[string][]Message

	// Simulate a node receiving a message, record metrics.
	ReceiveMessage(dbc *labelledDB, msg Message, tick int, from string) error

	// Add peer to a given node.
	AddPeer(peer string)
}

func MakeFloodNode(pubkey string, peers []string) Node {
	return &FloodNode{
		Pubkey:         pubkey,
		RelayQueue:     make(map[string][]Message),
		Peers:          peers,
		CachedMessages: make(map[string]cachedMessage),
	}
}

type cachedMessage struct {
	Message
	receivedFrom []string
}

type FloodNode struct {
	// PubKey of the node being represented
	Pubkey string

	// Queue of peer ID -> messages to send to peer
	RelayQueue map[string][]Message

	// Queue of messages that have just been received
	ReceiveQueue []Message

	// Pubkeys of peers
	Peers []string

	// Map protocol ID to message
	CachedMessages map[string]cachedMessage

	// ReceivedMessages maps a uuid to the list of peers who have sent us
	// a message, so we do not resend messages to them
	ReceivedMessages map[string][]string
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
		n.ReceiveQueue = append(n.ReceiveQueue, msg)
	}

	n.CachedMessages[msg.ID()] = cachedMessage{
		Message:      msg,
		receivedFrom: append(cached.receivedFrom, from),
	}

	return nil
}

func (n *FloodNode) GetQueue() map[string][]Message {
	return n.RelayQueue
}

func (n *FloodNode) ProgressQueue() {
	relay := make(map[string][]Message)

	for _, m := range n.ReceiveQueue {
		// get the message and the list of peers we have previously received
		// it from
		cached, ok := n.CachedMessages[m.ID()]
		if !ok {
			// check whether this would actually panic
			cached = cachedMessage{}
		}

		// assume we will send the message to all of our peers
		sendTo := n.Peers

		for _, to := range sendTo {
			// if we have already received this message from the peer,
			// skip over it
			var skipPeer bool
			for _, from := range cached.receivedFrom {
				if to == from {

					skipPeer = true
				}
			}

			// if we have already received this message from the peer,
			// we should not add the message to the peer's queue
			if skipPeer {
				continue
			}

			// add the message to the relay queue for the peer we have not
			// received it from.
			relay[to] = append(relay[to], m)
		}

	}

	n.RelayQueue = relay

	// clear receive queue because we have moved these to our broadcast queue
	n.ReceiveQueue = []Message{}
}
