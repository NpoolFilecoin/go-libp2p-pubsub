package floodsub

import (
	"fmt"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	nodes "github.com/ipfs/go-ipld-format"
)

type rcNode struct {
	node  nodes.Node
	links []*cid.Cid
	rc    uint
}

func newRcNode(node nodes.Node) *rcNode {
	links := node.Links()
	cids := make([]*cid.Cid, len(links))
	for i := range links {
		cids[i] = links[i].Cid
	}
	return &rcNode{
		links: cids,
		rc:    1,
		node:  node,
	}
}

// associatedDAG is a special IPLD DAG for storing gossip.
//
// We use this to figure out what data should be gossiped with a message/topic
// based on two conditions:
//  1. Is a node reachable from the message/topic?
//  2. Did we either:
//     a. Receive the node with along with the message?
//     b. Need the node to process/validate the message/topic? (TODO)
//
// If both of these conditions hold, we "associate" the node with the message
// and/or topic and gossip it along with it (when appropriate).
//
// So, *why* can't we just forward all nodes attached to a message along with the message?
//  1. They may be unrelated to the message because:
//    a. We may have a bad peer that's attaching junk to messages.
//    b. Nodes are attached to the RPC, not individual messages, and we can receive
//       multiple messages per-RPC.
//  2. To save bandwidth, peers may either:
//    a. Decide to not subscribe to "gossip" from some peers. This allows nodes to
//       learn that messages exist from *many* nodes (redundancy) without receiving
//       the entire message once per connected peer.
//    b. Not forward gossip to some peers.
//
// TODO: consider making these associations topic-specific.
type associatedDAG struct {
	// root CIDs -> node CIDs
	roots map[string]*cid.Set
	// node CIDs -> ref-counted nodes
	nodes map[string]*rcNode
}

func newAssociatedDAG() *associatedDAG {
	return &associatedDAG{
		roots: make(map[string]*cid.Set),
		nodes: make(map[string]*rcNode),
	}

}

// Get gets a node from the DAG.
func (g *associatedDAG) Get(node *cid.Cid) (nodes.Node, error) {
	id := node.KeyString()
	rn, ok := g.nodes[id]
	if !ok {
		// TODO: Use standardized error
		return nil, fmt.Errorf("not found")
	}
	return rn.node, nil
}

// AddAssociatedDAG adds a dag rooted at root and associate the nodes in the dag
// with root.
func (g *associatedDAG) AddAssociatedDAG(root *cid.Cid, data []blocks.Block) {
	if len(data) == 0 {
		return
	}

	dataMap := make(map[string]blocks.Block, len(data))
	for _, block := range data {
		dataMap[block.Cid().KeyString()] = block
	}

	rootId := root.KeyString()
	bset, ok := g.roots[rootId]
	if !ok {
		bset = cid.NewSet()
		g.roots[rootId] = bset
	}

	queue := make([]*cid.Cid, 1, len(data))
	queue[0] = root

	for len(queue) > 0 {
		next := queue[len(queue)-1]
		nextId := next.KeyString()

		queue = queue[:len(queue)-1]

		// Already seen.
		if bset.Has(next) {
			continue
		}
		// We don't have it.
		block, ok := dataMap[nextId]
		if !ok {
			continue
		}
		// Skip decoding if already decoded.
		rn, ok := g.nodes[nextId]
		if ok {
			rn.rc += 1
		} else {
			node, err := nodes.Decode(block)
			if err != nil {
				log.Warningf("failed to decode gossiped node: %s", err)
				continue
			}
			rn = newRcNode(node)
			g.nodes[nextId] = rn
		}

		// Update.
		bset.Add(next)
		queue = append(queue, rn.links...)
	}
}

// RemoveAssociatedDAG gets the list of nodes associated with root.
func (g *associatedDAG) GetAssociatedDAG(root *cid.Cid) []*cid.Cid {
	if bset, ok := g.roots[root.KeyString()]; ok {
		return bset.Keys()
	} else {
		return nil
	}
}

// RemoveAssociatedDAG removes the DAG associated with root.
func (g *associatedDAG) RemoveAssociatedDAG(root *cid.Cid) {
	rootId := root.KeyString()
	bset, ok := g.roots[rootId]
	if !ok {
		return
	}
	delete(g.roots, rootId)

	for _, blockCid := range bset.Keys() {
		bId := blockCid.KeyString()
		brecord, ok := g.nodes[bId]
		if !ok {
			panic("found a reference to a non-existent block")
		}
		brecord.rc -= 1
		if brecord.rc == 0 {
			delete(g.nodes, bId)
		}
	}

}
