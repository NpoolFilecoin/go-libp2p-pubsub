package floodsub

import (
	"context"
	"fmt"
	"sync"

	cid "github.com/ipfs/go-cid"
	nodes "github.com/ipfs/go-ipld-format"
)

// A very basic DAGService for testing.
type basicDAG struct {
	lock  sync.RWMutex
	nodes map[string]nodes.Node
}

func newBasicDAG() *basicDAG {
	return &basicDAG{
		nodes: make(map[string]nodes.Node),
	}
}

// Get all nodes in the dag.
func (d *basicDAG) Nodes() []nodes.Node {
	d.lock.RLock()
	defer d.lock.RUnlock()

	nds := make([]nodes.Node, 0, len(d.nodes))

	for _, v := range d.nodes {
		nds = append(nds, v)
	}
	return nds
}

func (d *basicDAG) Get(ctx context.Context, c *cid.Cid) (nodes.Node, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if node, ok := d.nodes[c.KeyString()]; ok {
		return node, nil
	} else {
		return nil, fmt.Errorf("not found")
	}
}

func (d *basicDAG) GetMany(ctx context.Context, cids []*cid.Cid) <-chan *nodes.NodeOption {
	d.lock.RLock()
	defer d.lock.RUnlock()

	ch := make(chan *nodes.NodeOption, len(cids))
	for _, c := range cids {
		if node, ok := d.nodes[c.KeyString()]; ok {
			ch <- &nodes.NodeOption{Node: node}
		} else {
			ch <- &nodes.NodeOption{Err: fmt.Errorf("not found")}
		}
	}
	return ch
}

func (d *basicDAG) OfflineNodeGetter() nodes.NodeGetter {
	return d
}

func (d *basicDAG) Add(node nodes.Node) (*cid.Cid, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	c := node.Cid()

	log.Debugf("DAG[%p] adding %s", d, c)
	d.nodes[c.KeyString()] = node
	return c, nil
}

func (d *basicDAG) AddMany(nds []nodes.Node) ([]*cid.Cid, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	cids := make([]*cid.Cid, len(nds))
	for i, node := range nds {
		c := node.Cid()
		log.Debugf("DAG[%p] adding %s", d, c)
		d.nodes[c.KeyString()] = node
		cids[i] = c
	}
	return cids, nil
}

func (d *basicDAG) Remove(node nodes.Node) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	c := node.Cid()
	key := c.KeyString()
	if _, ok := d.nodes[key]; ok {
		log.Debugf("DAG[%p] removing %s", d, c)
		delete(d.nodes, key)
		return nil
	} else {
		return fmt.Errorf("not found")
	}
}

var _ nodes.DAGService = &basicDAG{}
