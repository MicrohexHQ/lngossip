package main

import "testing"

func TestSimulate(t *testing.T) {
	dbc := connectAndResetForTesting(t)
	node1, node2, node3, node4 := "node1", "node2", "node3", "node4"

	// Triangular simulation
	nodes := map[string]Node{
		node1: MakeFloodNode(node1, []string{node2}),
		node2: MakeFloodNode(node2, []string{node1, node3}),
		node3: MakeFloodNode(node3, []string{node2}),
	}

	mMgr := &floodManager{
		messages: map[int][]Message{
			0: []Message{
				&ChannelUpdate{id: 1, Node: node1, chanID: "chan1"},
				&ChannelUpdate{id: 2, Node: node2, chanID: "chan2"},
			},
			1: []Message{
				&ChannelUpdate{id: 3, Node: node3, chanID: "chan3"},
			},
			2: []Message{},
		},
		lastBucket: 3,
	}

	simulate(dbc, mMgr, nodes)

	// Circular simulation
	nodes = map[string]Node{
		node1: MakeFloodNode(node1, []string{node2, node4}),
		node2: MakeFloodNode(node2, []string{node1, node3}),
		node3: MakeFloodNode(node3, []string{node2, node4}),
		node4: MakeFloodNode(node4, []string{node1, node3}),
	}

	mMgr = &floodManager{
		messages: map[int][]Message{
			0: []Message{
				&ChannelUpdate{id: 1, Node: node1, chanID: "chan1"},
			},
		},
		lastBucket: 1,
	}

	simulate(dbc, mMgr, nodes)

}
