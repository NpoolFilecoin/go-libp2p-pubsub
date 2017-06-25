package floodsub

import (
	"context"
	cid "github.com/ipfs/go-cid"
)

type Subscription struct {
	topicId  string
	topic    *cid.Cid
	ch       chan *cid.Cid
	cancelCh chan<- *Subscription
	err      error
}

func (sub *Subscription) Topic() *cid.Cid {
	return sub.topic
}

func (sub *Subscription) Next(ctx context.Context) (*cid.Cid, error) {
	select {
	case msg, ok := <-sub.ch:
		if !ok {
			return msg, sub.err
		}
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sub *Subscription) Cancel() {
	sub.cancelCh <- sub
}
