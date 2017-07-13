package floodsub

import (
	"context"
	"fmt"
	"time"

	pb "github.com/libp2p/go-floodsub/pb"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	nodes "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
	timecache "github.com/whyrusleeping/timecache"
)

const ID = protocol.ID("/floodsub/1.0.0")

var log = logging.Logger("floodsub")

type PubSub struct {
	host host.Host

	// incoming messages from other peers
	incoming chan *RPC

	// messages we are publishing out to our peers
	publish chan *pubMessageReq

	// control channel for adding new subscriptions
	addSub chan *addSubReq

	// get list of topics we are subscribed to
	getTopics chan *topicReq

	// get chan of peers we are connected to
	getPeers chan *listPeerReq

	// control channel for canceling subscriptions
	cancelCh chan *Subscription

	// a notification channel for new outbound streams to other peers
	newPeers chan inet.Stream

	// a notification channel for when one of our outbound peers die
	peerDead chan peer.ID

	// The set of topics we are subscribed to
	myTopics map[string]map[*Subscription]struct{}

	// topics tracks which topics each of our peers are subscribed to
	topics map[string]map[peer.ID]struct{}

	// list of connected outbound peers
	peers        map[peer.ID]chan *RPC
	seenMessages *timecache.TimeCache

	// DAG of gossiped blocks.
	gossip *associatedDAG

	// The system's DAG.
	dag nodes.DAGService

	ctx context.Context
}

type message struct {
	topics  []*cid.Cid
	message *cid.Cid
}

type RPC struct {
	pb.RPC

	// unexported on purpose, not sending this over the wire
	from peer.ID
}

// NewFloodSub returns a new FloodSub management object
func NewFloodSub(ctx context.Context, h host.Host, d nodes.DAGService) *PubSub {
	ps := &PubSub{
		host:         h,
		dag:          d,
		ctx:          ctx,
		incoming:     make(chan *RPC, 32),
		publish:      make(chan *pubMessageReq),
		newPeers:     make(chan inet.Stream),
		peerDead:     make(chan peer.ID),
		cancelCh:     make(chan *Subscription),
		getPeers:     make(chan *listPeerReq),
		addSub:       make(chan *addSubReq),
		getTopics:    make(chan *topicReq),
		myTopics:     make(map[string]map[*Subscription]struct{}),
		topics:       make(map[string]map[peer.ID]struct{}),
		peers:        make(map[peer.ID]chan *RPC),
		seenMessages: timecache.NewTimeCache(time.Second * 30),
		gossip:       newAssociatedDAG(),
	}

	h.SetStreamHandler(ID, ps.handleNewStream)
	h.Network().Notify((*PubSubNotif)(ps))

	go ps.processLoop(ctx)

	return ps
}

// processLoop handles all inputs arriving on the channels
func (p *PubSub) processLoop(ctx context.Context) {
	for {
		select {
		case s := <-p.newPeers:
			pid := s.Conn().RemotePeer()
			ch, ok := p.peers[pid]
			if ok {
				log.Error("already have connection to peer: ", pid)
				close(ch)
			}

			messages := make(chan *RPC, 32)
			go p.handleSendingMessages(ctx, s, messages)
			messages <- p.getHelloPacket()

			p.peers[pid] = messages

		case pid := <-p.peerDead:
			ch, ok := p.peers[pid]
			if ok {
				close(ch)
			}

			delete(p.peers, pid)
			for _, t := range p.topics {
				delete(t, pid)
			}
		case treq := <-p.getTopics:
			var out []*cid.Cid
			for t := range p.myTopics {
				// TODO: We shouldn't have to parse this. We
				// should store the *actual* Cids here.
				c, err := cid.Cast([]byte(t))
				if err != nil {
					panic("failed to parse a known-valid CID.")
				}
				out = append(out, c)
			}
			treq.resp <- out
		case sub := <-p.cancelCh:
			p.handleRemoveSubscription(sub)
		case sub := <-p.addSub:
			p.handleAddSubscription(sub)
		case preq := <-p.getPeers:
			tmap, ok := p.topics[preq.topic]
			if preq.topic != "" && !ok {
				preq.resp <- nil
				continue
			}
			var peers []peer.ID
			for p := range p.peers {
				if preq.topic != "" {
					_, ok := tmap[p]
					if !ok {
						continue
					}
				}
				peers = append(peers, p)
			}
			preq.resp <- peers
		case rpc := <-p.incoming:
			p.handleIncomingRPC(rpc)
		case req := <-p.publish:
			p.maybePublishMessage(p.host.ID(), req.message, req.gossip)
		case <-ctx.Done():
			log.Info("pubsub processloop shutting down")
			return
		}
	}
}

// handleRemoveSubscription removes Subscription sub from bookeeping.
// If this was the last Subscription for a given topic, it will also announce
// that this node is not subscribing to this topic anymore.
// Only called from processLoop.
func (p *PubSub) handleRemoveSubscription(sub *Subscription) {
	subs := p.myTopics[sub.topicId]

	if subs == nil {
		return
	}

	sub.err = fmt.Errorf("subscription cancelled by calling sub.Cancel()")
	close(sub.ch)
	delete(subs, sub)

	if len(subs) == 0 {
		delete(p.myTopics, sub.topicId)
		p.announce(sub.topicId, false)
	}
}

// handleAddSubscription adds a Subscription for a particular topic. If it is
// the first Subscription for the topic, it will announce that this node
// subscribes to the topic.
// Only called from processLoop.
func (p *PubSub) handleAddSubscription(req *addSubReq) {
	topicId := string(req.topic.Bytes())
	subs := p.myTopics[topicId]

	// announce we want this topic
	if len(subs) == 0 {
		p.announce(topicId, true)
	}

	// make new if not there
	if subs == nil {
		p.myTopics[topicId] = make(map[*Subscription]struct{})
		subs = p.myTopics[topicId]
	}

	sub := &Subscription{
		ch:       make(chan *cid.Cid, 32),
		topicId:  topicId,
		topic:    req.topic,
		cancelCh: p.cancelCh,
	}

	p.myTopics[sub.topicId][sub] = struct{}{}

	req.resp <- sub
}

// announce announces whether or not this node is interested in a given topic
// Only called from processLoop.
func (p *PubSub) announce(topicId string, sub bool) {
	subopt := &pb.RPC_SubOpts{
		Topicid:   []byte(topicId),
		Subscribe: &sub,
	}

	out := rpcWithSubs(subopt)
	for _, peer := range p.peers {
		peer <- out
	}
}

// notifySubs sends a given message to all corresponding subscribbers.
// Only called from processLoop.
func (p *PubSub) notifySubs(msg *message) {
	for _, topic := range msg.topics {
		subs := p.myTopics[string(topic.Bytes())]
		for f := range subs {
			f.ch <- msg.message
		}
	}
}

// seenMessage returns whether we already saw this message before
func (p *PubSub) seenMessage(id string) bool {
	return p.seenMessages.Has(id)
}

// markSeen marks a message as seen such that seenMessage returns `true' for the given id
func (p *PubSub) markSeen(id string) {
	p.seenMessages.Add(id)
}

func (p *PubSub) handleIncomingRPC(rpc *RPC) {
	p.handleIncomingSubscriptions(rpc.from, rpc.GetSubscriptions())

	messages := p.parseIncomingMessages(rpc.from, rpc.GetPublish())
	if len(messages) == 0 {
		// We don't care about anything in this message set.
		// Don't even bother parsing the gossip.
		// In the future, we *may* decide we care about the gossip and
		// decide to pilfer it for stuff we want.
		return
	}

	gossip := parseBlocks(rpc.GetGossip())

	for _, msg := range messages {
		p.maybePublishMessage(rpc.from, msg, gossip)
	}
}

func parseBlocks(pblocks []*pb.Block) []blocks.Block {
	gossip := make([]blocks.Block, 0, len(pblocks))
	for _, pblock := range pblocks {
		block, err := decodeBlock(pblock)
		if err != nil {
			// Already logged it.
			continue
		}
		gossip = append(gossip, block)
	}

	return gossip
}

func (p *PubSub) handleIncomingSubscriptions(from peer.ID, subopts []*pb.RPC_SubOpts) {
	for _, subopt := range subopts {
		t := subopt.GetTopicid()
		if subopt.GetSubscribe() {
			tmap, ok := p.topics[string(t)]
			if !ok {
				tmap = make(map[peer.ID]struct{})
				p.topics[string(t)] = tmap
			}

			tmap[from] = struct{}{}
		} else {
			tmap, ok := p.topics[string(t)]
			if !ok {
				continue
			}
			delete(tmap, from)
		}
	}
}

func (p *PubSub) parseIncomingMessages(from peer.ID, pmessages []*pb.Message) []*message {
	messages := make([]*message, 0, len(pmessages))
	for _, pmsg := range pmessages {
		ptopicIDs := pmsg.GetTopicIDs()
		topicIDs := make([]*cid.Cid, 0, len(ptopicIDs))
		for _, t := range ptopicIDs {
			if _, ok := p.myTopics[string(t)]; !ok {
				continue
			}
			c, err := cid.Cast(t)
			if err != nil {
				// We know this CID is valid because we're subscribed to it!
				panic("we're subscribed to an invalid CID")
			}
			topicIDs = append(topicIDs, c)
		}

		if len(topicIDs) == 0 {
			// Nothing to do.
			log.Warning("Received message we didn't subscribe to. Dropping.")
			continue
		}

		mcid, err := cid.Cast(pmsg.GetMessageCid())
		if err != nil {
			log.Warning("Received message with an invalid CID. Dropping.")
			continue
		}

		messages = append(messages, &message{
			topics:  topicIDs,
			message: mcid,
		})
	}
	return messages
}

func msgID(topic *cid.Cid, msg *cid.Cid) string {
	// Look ma! No separators!
	// That's fine because CIDs are prefix-free.
	topicBytes := topic.Bytes()
	msgBytes := msg.Bytes()
	id := make([]byte, len(topicBytes)+len(msgBytes))
	id = append(id, topicBytes...)
	id = append(id, msgBytes...)
	return string(id)
}

func (p *PubSub) maybePublishMessage(from peer.ID, msg *message, gossip []blocks.Block) {
	// Filter out topics on which we've already seen this message
	filteredTopics := make([]*cid.Cid, 0, len(msg.topics))
	for _, topic := range msg.topics {
		id := msgID(topic, msg.message)

		// Check if we have already processed it.
		if p.seenMessage(id) {
			continue
		}
		p.markSeen(id)

		filteredTopics = append(filteredTopics, topic)
	}
	// Replace the existing TopicIDs with filtered ones.
	// This is important because we can't *validate* this message against
	// topics on which we are not subscribed. If we don't remove unknown
	// topicIDs, a popular topic could be used as an amplification attack
	// against nodes only subscribed to low-bandwidth topics.
	msg.topics = filteredTopics

	// We've seen it on all topics, abort publish.
	if len(msg.topics) == 0 {
		return
	}

	p.gossip.AddAssociatedDAG(msg.message, gossip)

	// TODO: validate here.
	// NOTE: validation will change how this works significantly as we're
	// going to need to do it asynchronously. However, I'm going to leave
	// that for a future commit.

	p.publishMessage(from, msg)
}

func decodeBlock(pblock *pb.Block) (blocks.Block, error) {
	prefix, err := cid.PrefixFromBytes(pblock.GetPrefix())
	if err != nil {
		log.Warning("Received block with invalid CID prefix: ", err)
		return nil, err
	}
	c, err := prefix.Sum(pblock.GetData())
	if err != nil {
		log.Info("Failed to generate CID for block: ", err)
		return nil, err
	}

	block, err := blocks.NewBlockWithCid(pblock.GetData(), c)
	if err != nil {
		log.Error("The CID we *just* generated failed to match the block: ", err)
		return nil, err
	}
	return block, nil
}

func encodeBlock(block blocks.Block) *pb.Block {
	return &pb.Block{
		Prefix: block.Cid().Prefix().Bytes(),
		Data:   block.RawData(),
	}
}

func (p *PubSub) publishMessage(from peer.ID, msg *message) {
	gossipCids := p.gossip.GetAssociatedDAG(msg.message)
	gossip := make([]*pb.Block, len(gossipCids))
	for i, c := range gossipCids {
		node, err := p.gossip.Get(c)
		if err != nil {
			panic("that shouldn't happen...")
		}
		if _, err = p.dag.Add(node); err != nil {
			log.Infof("failed to put gossiped node %s in dag: %s", c, err)
		}
		gossip[i] = &pb.Block{
			Prefix: c.Prefix().Bytes(),
			Data:   node.RawData(),
		}
	}
	p.gossip.RemoveAssociatedDAG(msg.message)
	p.notifySubs(msg)

	tosend := make(map[peer.ID]struct{})
	pmsg := &pb.Message{
		MessageCid: msg.message.Bytes(),
		TopicIDs:   make([][]byte, len(msg.topics)),
	}
	for i, topic := range msg.topics {
		pmsg.TopicIDs[i] = topic.Bytes()

	}
	for _, topicId := range pmsg.TopicIDs {
		tmap, ok := p.topics[string(topicId)]
		if !ok {
			continue
		}

		for p, _ := range tmap {
			tosend[p] = struct{}{}
		}
	}

	out := rpcWithMessages(pmsg)
	out.Gossip = gossip

	for pid := range tosend {
		if pid == from {
			continue
		}

		mch, ok := p.peers[pid]
		if !ok {
			continue
		}

		go func() { mch <- out }()
	}
}

type addSubReq struct {
	topic *cid.Cid
	resp  chan *Subscription
}

// Subscribe returns a new Subscription for the given topic
func (p *PubSub) Subscribe(topic *cid.Cid) (*Subscription, error) {
	out := make(chan *Subscription, 1)
	p.addSub <- &addSubReq{
		topic: topic,
		resp:  out,
	}

	return <-out, nil
}

type topicReq struct {
	resp chan []*cid.Cid
}

// GetTopics returns the topics this node is subscribed to
func (p *PubSub) GetTopics() []*cid.Cid {
	out := make(chan []*cid.Cid, 1)
	p.getTopics <- &topicReq{resp: out}
	return <-out
}

type pubMessageReq struct {
	message *message
	gossip  []blocks.Block
}

// Publish publishes data under the given topic
func (p *PubSub) Publish(topic *cid.Cid, msg *cid.Cid, data ...blocks.Block) error {
	p.publish <- &pubMessageReq{
		message: &message{
			message: msg,
			topics:  []*cid.Cid{topic},
		},
		gossip: data,
	}
	return nil
}

type listPeerReq struct {
	resp  chan []peer.ID
	topic string
}

// ListPeers returns a list of peers we are connected to.
func (p *PubSub) ListPeers(topic *cid.Cid) []peer.ID {
	out := make(chan []peer.ID)
	var tid string
	if topic != nil {
		tid = string(topic.Bytes())
	}
	p.getPeers <- &listPeerReq{
		resp:  out,
		topic: tid,
	}
	return <-out
}
