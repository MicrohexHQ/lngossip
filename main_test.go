package main

import "testing"

func TestSimulate(t *testing.T) {
	node1, node2, node3 := "node1", "node2", "node3"

	nodes := []Node{
		MakeFloodNode(node1, []string{node2}),
		MakeFloodNode(node2, []string{node1, node3}),
		MakeFloodNode(node3, []string{node2}),
	}

	mMgr := &floodManager{
		messages: map[int][]Message{
			0: []Message{
				&ChannelUpdate{id: 1, Node: node1},
				&ChannelUpdate{id: 2, Node: node2},
			},
			1: []Message{
				&ChannelUpdate{id: 3, Node: node3},
			},
			2: []Message{},
		},
		lastBucket: 3,
	}

	simulate(mMgr, nodes)
}
