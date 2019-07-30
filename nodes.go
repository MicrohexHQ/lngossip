package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"

	"github.com/lightningnetwork/lnd/lnrpc"
)

var chanGraphPath = flag.String("chan_graph",
	"/Users/carla/personal/src/github.com/carlaKC/lngossip/data/July10/graph.txt",
	"Path to channel graph obtained from LND's describe graph call")

type chanGraph struct {
	/// The list of `LightningNode`s in this channel graph
	Nodes []*lnrpc.LightningNode `protobuf:"bytes,1,rep,name=nodes,proto3" json:"nodes,omitempty"`
	Edges []*ChannelEdge         `protobuf:"bytes,2,rep,name=edges,proto3" json:"edges,omitempty"`
}

// TODO(carla): figure why couldn't read in channelID and capacity without adding (,string) to json
type ChannelEdge struct {
	ChannelId  uint64 `protobuf:"varint,1,opt,name=channel_id,proto3" json:"channel_id,omitempty,string"`
	ChanPoint  string `protobuf:"bytes,2,opt,name=chan_point,proto3" json:"chan_point,omitempty"`
	LastUpdate uint32 `protobuf:"varint,3,opt,name=last_update,proto3" json:"last_update,omitempty"` // Deprecated: Do not use.
	Node1Pub   string `protobuf:"bytes,4,opt,name=node1_pub,proto3" json:"node1_pub,omitempty"`
	Node2Pub   string `protobuf:"bytes,5,opt,name=node2_pub,proto3" json:"node2_pub,omitempty"`
	Capacity   int64  `protobuf:"varint,6,opt,name=capacity,proto3" json:"capacity,omitempty,string"`
}

func readChanGraph() (map[string]Node, error) {
	file, err := ioutil.ReadFile(*chanGraphPath)
	if err != nil {
		return nil, err
	}

	var graph chanGraph
	if err := json.Unmarshal(file, &graph); err != nil {
		return nil, err
	}

	nodes := make(map[string]Node)
	for _, node := range graph.Nodes {
		nodes[node.PubKey] = &FloodNode{
			Pubkey:         node.PubKey,
			RelayQueue:     make(map[string][]Message),
			CachedMessages: make(map[string]cachedMessage),
		}
	}

	for _, edge := range graph.Edges {
		nodes[edge.Node1Pub].AddPeer(edge.Node2Pub)
		nodes[edge.Node2Pub].AddPeer(edge.Node1Pub)
	}

	log.Printf("Read in channel graph with %v nodes and %v edges",
		len(graph.Nodes), len(graph.Edges))

	return nodes, nil
}
