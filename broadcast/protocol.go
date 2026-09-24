package broadcast

import (
	"context"
	"errors"
	"slices"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/protocol"
)

// These selectors are the broadcast wire contract. Stack owns selector
// advertisement and routing; broadcast only declares the selectors it
// consumes and handles the streams delivered by Stack.
const (
	BCAST protocol.Selector = 1
	SESS  protocol.Selector = 2
	CHUNK protocol.Selector = 3
)

// Serve consumes peer and stream notifications for a broadcast subsystem.
// Stack may deliver a stream event before its corresponding peer notification;
// both event kinds carry the same stable *ethp2p.Peer pointer, so the engine
// binds them to one PeerConn. Serve owns neither the notification channels nor
// the borrowed connections. It returns when ctx is cancelled, the Engine is
// closed, or both input channels are closed. Stopping Serve stops notification
// consumption; existing bindings end with Peer.Context or Engine.Close. The
// application must stop producers and dispose of events left in its channels.
// Notifications must carry the non-nil Context and authenticated ID supplied by Stack.
func (e *Engine) Serve(ctx context.Context, peers <-chan *ethp2p.Peer, streams <-chan ethp2p.StreamEvent) error {
	if ctx == nil {
		return errors.New("nil serve context")
	}
	e.serveMu.Lock()
	if e.ctx.Err() != nil {
		e.serveMu.Unlock()
		return nil
	}
	e.serveWG.Add(1)
	e.serveMu.Unlock()
	defer e.serveWG.Done()

	for peers != nil || streams != nil {
		var event engineEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.ctx.Done():
			return nil
		case peer, ok := <-peers:
			if !ok {
				peers = nil
				continue
			}
			event = engineEvent{kind: evPeerEvent, appPeer: peer}
		case stream, ok := <-streams:
			if !ok {
				streams = nil
				continue
			}
			event = engineEvent{kind: evStreamEvent, stream: stream}
		}

		select {
		case e.eventCh <- event:
			continue
		case <-ctx.Done():
		case <-e.ctx.Done():
		}
		// Delivery failed, so Serve still owns the stream, if any.
		event.stream.Reject()
		return ctx.Err()
	}
	return nil
}

func supportsBroadcast(peer *ethp2p.Peer) bool {
	if peer == nil || peer.ID == "" || peer.Context == nil || peer.Context.Err() != nil {
		return false
	}
	return supportsBroadcastSelectors(peer.Selectors)
}

func supportsBroadcastSelectors(selectors []protocol.Selector) bool {
	return slices.Contains(selectors, BCAST) && slices.Contains(selectors, SESS) && slices.Contains(selectors, CHUNK)
}

func isBroadcastSelector(selector protocol.Selector) bool {
	return selector == BCAST || selector == SESS || selector == CHUNK
}
