package main

import (
	"testing"
)

func TestSimulate(t *testing.T) {

	nodeA, nodeB, nodeC, nodeD := "nodeA", "nodeB", "nodeC", "nodeD"
	nodeE, nodeF, nodeG, nodeH := "nodeE", "nodeF", "nodeG", "nodeH"

	// nodeGraph provides a set of nodes with the following topology:
	// A ---- B ---- C
	// |      |
	// D ---- E ---- F
	//				 |
	//  	  H ---- G
	// It is declared outside of tests to save some space
	nodeGraph := map[string]Node{
		nodeA: MakeFloodNode(nodeA, []string{nodeB, nodeD}),
		nodeB: MakeFloodNode(nodeB, []string{nodeA, nodeC, nodeE}),
		nodeC: MakeFloodNode(nodeC, []string{nodeB}),
		nodeD: MakeFloodNode(nodeD, []string{nodeA, nodeE}),
		nodeE: MakeFloodNode(nodeE, []string{nodeB, nodeD, nodeF}),
		nodeF: MakeFloodNode(nodeF, []string{nodeE, nodeG}),
		nodeG: MakeFloodNode(nodeG, []string{nodeF, nodeH}),
		nodeH: MakeFloodNode(nodeH, []string{nodeG}),
	}

	tests := []struct {
		name     string
		nodes    map[string]Node
		messages map[int][]Message
		// checkResults checks some values for the simulation to check that they
		// are expected. The expected values are hand calculated, but the logic is
		// explained in comments as much as possible. The notition used is:
		// Mx* means node generated message x
		// y.Mx means that node y sent you message x
		// y(Mx) means that node y knows about message x
		checkResults func(t *testing.T, dbc *labelledDB)
	}{
		{
			name: "Linear nodes",
			// A ---- B ---- C
			nodes: map[string]Node{
				nodeA: MakeFloodNode(nodeA, []string{nodeB}),
				nodeB: MakeFloodNode(nodeB, []string{nodeA, nodeC}),
				nodeC: MakeFloodNode(nodeC, []string{nodeB}),
			},
			// Tick 0: A(M1*) B(M2*)
			// Tick 1: A(M1, b.M2) 			B(a.M1, M2) 		C(b.M2, M3*)
			// Tick 2: A(M1, b.M2) 			B(a.M1, M2, c.M3) 	C(M2, M3)
			// Tick 3: A(M1, b.M2, b.M3) 	B(a.M1, M2, c.M3) 	C(b.M1, b.M2, M3)
			messages: map[int][]Message{
				0: {
					&ChannelUpdate{id: 1, Node: nodeA, chanID: "chan1"},
					&ChannelUpdate{id: 2, Node: nodeB, chanID: "chan2"},
				},
				1: {
					&ChannelUpdate{id: 3, Node: nodeC, chanID: "chan3"},
				},
				2: {},
			},
			checkResults: func(t *testing.T, dbc *labelledDB) {
				for i := 1; i < 4; i++ {
					count, err := GetDuplicateCount(dbc, int64(i))
					if err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatalf("Expected have no duplicates, got: %v", count)
					}
				}
			},
		},

		{
			name: "Circular simulation",
			// A ---- B
			// |      |
			// D ---- C
			nodes: map[string]Node{
				nodeA: MakeFloodNode(nodeA, []string{nodeB, nodeD}),
				nodeB: MakeFloodNode(nodeB, []string{nodeA, nodeC}),
				nodeC: MakeFloodNode(nodeC, []string{nodeB, nodeD}),
				nodeD: MakeFloodNode(nodeD, []string{nodeA, nodeC}),
			},
			// Tick 0: A(M1*)
			// Tick 1: A(M1) B(M1) D(M1)
			// Tick 2: A(M1) B(M1) D(M1) C(B.M1, D.M1)
			messages: map[int][]Message{
				0: {
					&ChannelUpdate{id: 1, Node: nodeA, chanID: "chan1"},
				},
			},
			checkResults: func(t *testing.T, dbc *labelledDB) {
				count, err := GetDuplicateCount(dbc, 1)
				if err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("Expected C to receive one duplicate, got: %v", count)
				}

				latency, err := GetMessageLatency(dbc, 1)
				if err != nil {
					t.Fatal(err)
				}
				if latency != (4 / 3) {
					t.Fatalf("Expected latency: %v, got %v", 24/7, latency)
				}
			},
		},

		{
			name: "Single message propagate complex graph",
			// A ---- B ---- C
			// |      |
			// D ---- E ---- F
			//				 |
			//  	  H ---- G
			nodes: nodeGraph,
			// Tick 0: H(M1*)
			// Tick 1: G(h.M1), H(M1)
			// Tick 2: F(g.M1) G(h.M1), H(M1)
			// Tick 3: E(g.M1) F(g.M1) G(h.M1), H(M1)
			// Tick 4: B(e.M1) D(e.M1) E(g.M1) F(g.M1) G(h.M1), H(M1)
			// Tick 5: A(b.M1, d.M1) C(b.M1) B(e.M1) D(e.M1) E(g.M1) F(g.M1) G(h.M1), H(M1)
			messages: map[int][]Message{
				0: {
					&ChannelUpdate{id: 1, Node: nodeH, chanID: "chan1"},
				},
			},
			checkResults: func(t *testing.T, dbc *labelledDB) {
				count, err := GetDuplicateCount(dbc, 1)
				if err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("Expected A to receive one duplicate, got: %v", count)
				}

				latency, err := GetMessageLatency(dbc, 1)
				if err != nil {
					t.Fatal(err)
				}
				if latency != (24 / 7) {
					t.Fatalf("Expected latency: %v, got %v", 24/7, latency)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbc := connectAndResetForTesting(t)

			mMgr := &floodManager{
				messages:   test.messages,
				lastBucket: len(test.messages),
			}

			simulate(dbc, mMgr, test.nodes)

			test.checkResults(t, dbc)
		})
	}
}
