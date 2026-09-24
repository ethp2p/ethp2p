package broadcast

import (
	"context"
	"sync"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/protocol"
)

// channelDelivery owns inbound streams across the producer/inbox/actor handoff.
// Registration and termination share a lock: a late select may still enqueue,
// but its stream is either in the termination snapshot or already rejected.
// Cancellation runs outside the lock and never waits for network progress.
type channelDelivery struct {
	inbox   chan<- channelEvent
	done    <-chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	closed  bool
	streams map[ethp2p.ReceiveStream]struct{}
}

func newChannelDelivery(inbox chan<- channelEvent) *channelDelivery {
	ctx, cancel := context.WithCancel(context.Background())
	return &channelDelivery{inbox: inbox, done: ctx.Done(), ctx: ctx, cancel: cancel, streams: make(map[ethp2p.ReceiveStream]struct{})}
}

func (d *channelDelivery) add(s ethp2p.ReceiveStream) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return false
	}
	d.streams[s] = struct{}{}
	return true
}

func (d *channelDelivery) release(s ethp2p.ReceiveStream) {
	d.mu.Lock()
	delete(d.streams, s)
	d.mu.Unlock()
}

func (d *channelDelivery) close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	d.cancel()
	streams := d.streams
	d.streams = nil
	d.mu.Unlock()
	for s := range streams {
		s.CancelRead(protocol.Unspecified)
	}
}
