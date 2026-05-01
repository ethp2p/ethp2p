package sim

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethp2p/ethp2p/broadcast"
)

type readinessObserver struct {
	broadcast.Observer
	ready *peerReadyTracker
}

func (o *readinessObserver) OnPeerSubscribed(peerID broadcast.PeerID, channelID broadcast.ChannelID) {
	o.Observer.OnPeerSubscribed(peerID, channelID)
	if o.ready != nil {
		o.ready.mark(peerID, channelID)
	}
}

type peerReadyTracker struct {
	channelID broadcast.ChannelID
	mu        sync.Mutex
	peers     map[broadcast.PeerID]struct{}
}

func newPeerReadyTracker(channelID broadcast.ChannelID) *peerReadyTracker {
	return &peerReadyTracker{
		channelID: channelID,
		peers:     make(map[broadcast.PeerID]struct{}),
	}
}

func (t *peerReadyTracker) mark(peerID broadcast.PeerID, channelID broadcast.ChannelID) {
	if channelID != t.channelID {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[peerID] = struct{}{}
}

func (t *peerReadyTracker) await(ctx context.Context, peers []int) error {
	expected := make(map[broadcast.PeerID]struct{}, len(peers))
	for _, p := range peers {
		expected[broadcast.PeerID(fmt.Sprint(p))] = struct{}{}
	}
	if len(expected) == 0 {
		return nil
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if t.has(expected) {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (t *peerReadyTracker) has(expected map[broadcast.PeerID]struct{}) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for p := range expected {
		if _, ok := t.peers[p]; !ok {
			return false
		}
	}
	return true
}
