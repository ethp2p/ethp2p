package broadcast

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/wire"
)

// These selectors are the broadcast wire contract. Stack owns selector
// Hello exchange and routing; broadcast only declares the selectors it
// consumes and handles the streams delivered by Stack.
const (
	BCAST wire.Selector = 1
	SESS  wire.Selector = 2
	CHUNK wire.Selector = 3
)

var (
	// Reconstructed ends a SESS stream after the sender reconstructs its message.
	Reconstructed = wire.ProtocolCode(1)
	// Redundant ends a CHUNK stream when the receiver no longer needs its chunk.
	Redundant = wire.ProtocolCode(2)

	errChunkRedundant = errors.New("chunk is no longer needed")
	errStreamRefused  = errors.New("stream will not be processed")
)

func streamFailureCode(err error) wire.Code {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return wire.Timeout
	}
	if timeout, ok := errors.AsType[net.Error](err); ok && timeout.Timeout() {
		return wire.Timeout
	}
	return wire.Unspecified
}

func streamCancellationCode(cause error) wire.Code {
	if streamFailureCode(cause) == wire.Timeout {
		return wire.Timeout
	}
	if errors.Is(cause, errChunkRedundant) {
		return Redundant
	}
	if errors.Is(cause, errStreamRefused) {
		return wire.Refused
	}
	return wire.Unspecified
}

// Register attaches broadcast to stack before Start. The engine drains its
// subsystem in its own event loop. Register may succeed only once.
func (e *Engine) Register(stack *ethp2p.Stack) error {
	e.registerMu.Lock()
	defer e.registerMu.Unlock()
	if e.subsystem.Load() != nil {
		return errors.New("broadcast already registered")
	}
	if stack == nil {
		return errors.New("nil stack")
	}
	if e.ctx.Err() != nil {
		return errors.New("broadcast engine closed")
	}
	sub, err := stack.Register("broadcast", []wire.Selector{BCAST, SESS, CHUNK}, ethp2p.SubsystemConfig{
		Policy: func(p *ethp2p.Peer) bool { return supportsBroadcastSelectors(p.Selectors()) },
	})
	if err != nil {
		return err
	}
	if err := sub.Notify(e.deliveryWake); err != nil {
		return err
	}
	e.subsystem.Store(sub)
	return nil
}

func supportsBroadcast(peer *ethp2p.Peer) bool {
	if peer == nil || peer.ID() == "" || peer.Context() == nil || peer.Context().Err() != nil {
		return false
	}
	return supportsBroadcastSelectors(peer.Selectors())
}

func supportsBroadcastSelectors(selectors []wire.Selector) bool {
	return slices.Contains(selectors, BCAST) && slices.Contains(selectors, SESS) && slices.Contains(selectors, CHUNK)
}

func isBroadcastSelector(selector wire.Selector) bool {
	return selector == BCAST || selector == SESS || selector == CHUNK
}
