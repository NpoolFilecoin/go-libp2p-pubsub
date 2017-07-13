package floodsub

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
	host "github.com/libp2p/go-libp2p-host"
	netutil "github.com/libp2p/go-libp2p-netutil"
	peer "github.com/libp2p/go-libp2p-peer"
	mh "github.com/multiformats/go-multihash"
	//bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	bhost "github.com/libp2p/go-libp2p-blankhost"
)

func stringCid(data string) *cid.Cid {
	cid, err := cid.NewPrefixV1(cid.Raw, mh.ID).Sum([]byte(data))
	if err != nil {
		panic("Couldn't make a raw SHA256 CID?")
	}
	return cid
}

func checkMessageRouting(t *testing.T, topic *cid.Cid, pubs []*PubSub, subs []*Subscription) {

	for i, p := range pubs {
		msg := stringCid(fmt.Sprintf("msg %d", i))
		err := p.Publish(topic, msg)
		if err != nil {
			t.Fatal(err)
		}

		for _, s := range subs {
			assertReceive(t, s, msg)
		}
	}
}

func getNetHosts(t *testing.T, ctx context.Context, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		netw := netutil.GenSwarmNetwork(t, ctx)
		h := bhost.NewBlankHost(netw)
		out = append(out, h)
	}

	return out
}

func connect(t *testing.T, a, b host.Host) {
	pinfo := a.Peerstore().PeerInfo(a.ID())
	err := b.Connect(context.Background(), pinfo)
	if err != nil {
		t.Fatal(err)
	}
}

func sparseConnect(t *testing.T, hosts []host.Host) {
	for i, a := range hosts {
		for j := 0; j < 3; j++ {
			n := rand.Intn(len(hosts))
			if n == i {
				j--
				continue
			}

			b := hosts[n]

			connect(t, a, b)
		}
	}
}

func connectAll(t *testing.T, hosts []host.Host) {
	for i, a := range hosts {
		for j, b := range hosts {
			if i == j {
				continue
			}

			connect(t, a, b)
		}
	}
}

func getPubsubs(ctx context.Context, hs []host.Host) []*PubSub {
	var psubs []*PubSub
	for _, h := range hs {
		psubs = append(psubs, NewFloodSub(ctx, h))
	}
	return psubs
}

func assertReceive(t *testing.T, ch *Subscription, exp *cid.Cid) {
	select {
	case msg := <-ch.ch:
		if !msg.Equals(exp) {
			t.Fatalf("got wrong message, expected %s but got %s", pretty(exp), pretty(msg))
		}
	case <-time.After(time.Second * 5):
		t.Logf("%#v\n", ch)
		t.Fatal("timed out waiting for message of: ", pretty(exp))
	}
}

func TestBasicFloodsub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := getNetHosts(t, ctx, 20)

	psubs := getPubsubs(ctx, hosts)

	topic := stringCid("foobar")

	var msgs []*Subscription
	for _, ps := range psubs {
		subch, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	//connectAll(t, hosts)
	sparseConnect(t, hosts)

	time.Sleep(time.Millisecond * 100)

	for i := 0; i < 100; i++ {
		msg := stringCid(fmt.Sprintf("%d the flooooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		psubs[owner].Publish(topic, msg)

		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			if !msg.Equals(got) {
				t.Fatalf("got wrong message! expected %s, got %s", pretty(msg), pretty(got))
			}
		}
	}

}

func TestMultihops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 6)
	topic := stringCid("foobar")

	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])
	connect(t, hosts[2], hosts[3])
	connect(t, hosts[3], hosts[4])
	connect(t, hosts[4], hosts[5])

	var subs []*Subscription
	for i := 1; i < 6; i++ {
		ch, err := psubs[i].Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, ch)
	}

	time.Sleep(time.Millisecond * 100)

	msg := stringCid("i like cats")
	err := psubs[0].Publish(topic, msg)
	if err != nil {
		t.Fatal(err)
	}

	// last node in the chain should get the message
	select {
	case out := <-subs[4].ch:
		if !out.Equals(msg) {
			t.Fatal("got wrong data")
		}
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for message")
	}
}

func TestReconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)
	topic := stringCid("cats")

	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[0], hosts[2])

	A, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	B, err := psubs[2].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	msg := stringCid("apples and oranges")
	err = psubs[0].Publish(topic, msg)
	if err != nil {
		t.Fatal(err)
	}

	assertReceive(t, A, msg)
	assertReceive(t, B, msg)

	B.Cancel()

	time.Sleep(time.Millisecond * 50)

	msg2 := stringCid("potato")
	err = psubs[0].Publish(topic, msg2)
	if err != nil {
		t.Fatal(err)
	}

	assertReceive(t, A, msg2)
	select {
	case _, ok := <-B.ch:
		if ok {
			t.Fatal("shouldnt have gotten data on this channel")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for B chan to be closed")
	}

	nSubs := len(psubs[2].myTopics[string(topic.Bytes())])
	if nSubs > 0 {
		t.Fatal(`B should have 0 subscribers for channel "cats", has`, nSubs)
	}

	ch2, err := psubs[2].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	nextmsg := stringCid("ifps is kul")
	err = psubs[0].Publish(topic, nextmsg)
	if err != nil {
		t.Fatal(err)
	}

	assertReceive(t, ch2, nextmsg)
}

// make sure messages arent routed between nodes who arent subscribed
func TestNoConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 10)
	topic := stringCid("foobar")

	psubs := getPubsubs(ctx, hosts)

	ch, err := psubs[5].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	err = psubs[0].Publish(topic, stringCid("TESTING"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-ch.ch:
		t.Fatal("shouldnt have gotten a message")
	case <-time.After(time.Millisecond * 200):
	}
}

func TestSelfReceive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := stringCid("foobar")
	host := getNetHosts(t, ctx, 1)[0]

	psub := NewFloodSub(ctx, host)

	msg := stringCid("hello world")

	err := psub.Publish(topic, msg)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 10)

	ch, err := psub.Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	msg2 := stringCid("goodbye world")
	err = psub.Publish(topic, msg2)
	if err != nil {
		t.Fatal(err)
	}

	assertReceive(t, ch, msg2)
}

func TestOneToOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := stringCid("foobar")
	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])

	ch, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 50)

	checkMessageRouting(t, topic, psubs, []*Subscription{ch})
}

func assertPeerLists(t *testing.T, hosts []host.Host, ps *PubSub, has ...int) {
	peers := ps.ListPeers(nil)
	set := make(map[peer.ID]struct{})
	for _, p := range peers {
		set[p] = struct{}{}
	}

	for _, h := range has {
		if _, ok := set[hosts[h].ID()]; !ok {
			t.Fatal("expected to have connection to peer: ", h)
		}
	}
}

func TestTreeTopology(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := stringCid("fizzbuzz")
	hosts := getNetHosts(t, ctx, 10)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])
	connect(t, hosts[1], hosts[4])
	connect(t, hosts[2], hosts[3])
	connect(t, hosts[0], hosts[5])
	connect(t, hosts[5], hosts[6])
	connect(t, hosts[5], hosts[8])
	connect(t, hosts[6], hosts[7])
	connect(t, hosts[8], hosts[9])

	/*
		[0] -> [1] -> [2] -> [3]
		 |      L->[4]
		 v
		[5] -> [6] -> [7]
		 |
		 v
		[8] -> [9]
	*/

	var chs []*Subscription
	for _, ps := range psubs {
		ch, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}

		chs = append(chs, ch)
	}

	time.Sleep(time.Millisecond * 50)

	assertPeerLists(t, hosts, psubs[0], 1, 5)
	assertPeerLists(t, hosts, psubs[1], 0, 2, 4)
	assertPeerLists(t, hosts, psubs[2], 1, 3)

	checkMessageRouting(t, topic, []*PubSub{psubs[9], psubs[3]}, chs)
}

func assertHasTopics(t *testing.T, ps *PubSub, exptopics ...*cid.Cid) {
	topics := ps.GetTopics()
	sort.Slice(topics, func(i, j int) bool {
		return string(topics[i].Bytes()) < string(topics[j].Bytes())
	})
	sort.Slice(exptopics, func(i, j int) bool {
		return string(exptopics[i].Bytes()) < string(exptopics[j].Bytes())
	})

	if len(topics) != len(exptopics) {
		t.Fatalf("expected to have %v, but got %v", exptopics, topics)
	}

	for i, v := range exptopics {
		if !topics[i].Equals(v) {
			t.Fatalf("expected %s but have %s", v, topics[i])
		}
	}
}

func TestSubReporting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host := getNetHosts(t, ctx, 1)[0]
	psub := NewFloodSub(ctx, host)

	topicFoo := stringCid("foo")
	topicBar := stringCid("bar")
	topicBaz := stringCid("baz")
	topicFish := stringCid("fish")

	fooSub, err := psub.Subscribe(topicFoo)
	if err != nil {
		t.Fatal(err)
	}

	barSub, err := psub.Subscribe(topicBar)
	if err != nil {
		t.Fatal(err)
	}

	assertHasTopics(t, psub, topicFoo, topicBar)

	_, err = psub.Subscribe(topicBaz)
	if err != nil {
		t.Fatal(err)
	}

	assertHasTopics(t, psub, topicFoo, topicBar, topicBaz)

	barSub.Cancel()
	assertHasTopics(t, psub, topicFoo, topicBaz)
	fooSub.Cancel()
	assertHasTopics(t, psub, topicBaz)

	_, err = psub.Subscribe(topicFish)
	if err != nil {
		t.Fatal(err)
	}

	assertHasTopics(t, psub, topicBaz, topicFish)
}

func TestPeerTopicReporting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 4)
	psubs := getPubsubs(ctx, hosts)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[0], hosts[2])
	connect(t, hosts[0], hosts[3])

	_, err := psubs[1].Subscribe(stringCid("foo"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = psubs[1].Subscribe(stringCid("bar"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = psubs[1].Subscribe(stringCid("baz"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = psubs[2].Subscribe(stringCid("foo"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = psubs[2].Subscribe(stringCid("ipfs"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = psubs[3].Subscribe(stringCid("baz"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = psubs[3].Subscribe(stringCid("ipfs"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 10)

	peers := psubs[0].ListPeers(stringCid("ipfs"))
	assertPeerList(t, peers, hosts[2].ID(), hosts[3].ID())

	peers = psubs[0].ListPeers(stringCid("foo"))
	assertPeerList(t, peers, hosts[1].ID(), hosts[2].ID())

	peers = psubs[0].ListPeers(stringCid("baz"))
	assertPeerList(t, peers, hosts[1].ID(), hosts[3].ID())

	peers = psubs[0].ListPeers(stringCid("bar"))
	assertPeerList(t, peers, hosts[1].ID())
}

func TestSubscribeMultipleTimes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)
	psubs := getPubsubs(ctx, hosts)
	topic := stringCid("foo")

	connect(t, hosts[0], hosts[1])

	sub1, err := psubs[0].Subscribe(stringCid("foo"))
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := psubs[0].Subscribe(stringCid("foo"))
	if err != nil {
		t.Fatal(err)
	}

	// make sure subscribing is finished by the time we publish
	time.Sleep(1 * time.Millisecond)

	msg := stringCid("bar")
	psubs[1].Publish(topic, msg)

	out, err := sub1.Next(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v.", err)
	}

	if !out.Equals(msg) {
		t.Fatalf("data is %s, expected %s.", pretty(out), pretty(msg))
	}

	out, err = sub2.Next(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v.", err)
	}
	if !out.Equals(msg) {
		t.Fatalf("data is %s, expected %s.", pretty(out), pretty(msg))
	}
}

func pretty(c *cid.Cid) string {
	decoded, _ := mh.Decode([]byte(c.Hash()))
	fmt.Printf("here %v\n", decoded.Code)
	if decoded.Code == mh.ID {
		// TODO: Check if type is utf8 string?
		return fmt.Sprintf("i/%s", decoded.Digest)
	}
	return fmt.Sprintf("h/%s", c.String())
}

func assertPeerList(t *testing.T, peers []peer.ID, expected ...peer.ID) {
	sort.Sort(peer.IDSlice(peers))
	sort.Sort(peer.IDSlice(expected))

	if len(peers) != len(expected) {
		t.Fatalf("mismatch: %s != %s", peers, expected)
	}

	for i, p := range peers {
		if expected[i] != p {
			t.Fatalf("mismatch: %s != %s", peers, expected)
		}
	}
}
