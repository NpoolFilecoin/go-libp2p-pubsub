package floodsub

import (
	"context"
	"fmt"
	"time"

	pb "github.com/libp2p/go-floodsub/pb"

	cid "github.com/ipfs/go-cid"
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
	publish chan *message

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
func NewFloodSub(ctx context.Context, h host.Host) *PubSub {
	ps := &PubSub{
		host:         h,
		ctx:          ctx,
		incoming:     make(chan *RPC, 32),
		publish:      make(chan *message),
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
		case msg := <-p.publish:
			p.maybePublishMessage(p.host.ID(), msg)
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
	for _, subopt := range rpc.GetSubscriptions() {
		t := subopt.GetTopicid()
		if subopt.GetSubscribe() {
			tmap, ok := p.topics[string(t)]
			if !ok {
				tmap = make(map[peer.ID]struct{})
				p.topics[string(t)] = tmap
			}

			tmap[rpc.from] = struct{}{}
		} else {
			tmap, ok := p.topics[string(t)]
			if !ok {
				continue
			}
			delete(tmap, rpc.from)
		}
	}

	for _, pmsg := range rpc.GetPublish() {
		// Pull out the mcid first.
		pmcid := pmsg.GetMessageCid()
		mcid, err := cid.Cast(pmcid)
		if err != nil {
			log.Warning("Received message with an invalid CID. Dropping.")
			continue
		}

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
		p.maybePublishMessage(rpc.from, &message{
			topics:  topicIDs,
			message: mcid,
		})
	}
}

func (p *PubSub) maybePublishMessage(from peer.ID, msg *message) {
	// Filter out topics on which we've already seen this message
	filteredTopics := make([]*cid.Cid, 0, len(msg.topics))
	messageBytes := msg.message.Bytes()
	for _, topic := range msg.topics {
		// Look ma! No separators!
		// That's fine because CIDs are prefix-free.
		id := string(append(topic.Bytes(), messageBytes...))

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

	p.notifySubs(msg)

	err := p.publishMessage(from, msg)
	if err != nil {
		log.Error("publish message: ", err)
	}
}

func (p *PubSub) publishMessage(from peer.ID, msg *message) error {
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

	return nil
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

// Publish publishes data under the given topic
func (p *PubSub) Publish(topic *cid.Cid, data *cid.Cid) error {
	p.publish <- &message{
		message: data,
		topics:  []*cid.Cid{topic},
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
