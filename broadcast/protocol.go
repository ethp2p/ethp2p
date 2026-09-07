package broadcast

import (
	"context"
	"fmt"

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
)

const (
	bcastCodepoint   protocol.Codepoint = 1
	sessionCodepoint protocol.Codepoint = 2
	chunkCodepoint   protocol.Codepoint = 3
)

var protocolDescriptors = [...]protocol.Descriptor{
	{
		Codepoint: bcastCodepoint,
		Name:      "ethp2p/bcast",
	},
	{
		Codepoint: sessionCodepoint,
		Name:      "ethp2p/session",
	},
	{
		Codepoint: chunkCodepoint,
		Name:      "ethp2p/chunk",
	},
}

// Protocols returns the broadcast protocols and their per-connection handlers.
func (e *Engine) Protocols() protocol.Set {
	descriptors := make([]protocol.Descriptor, len(protocolDescriptors))
	copy(descriptors, protocolDescriptors[:])
	return protocol.Set{
		Descriptors: descriptors,
		Bind: func(ctx context.Context, conn transport.Conn) (protocol.ConnectionHandlers, error) {
			peer, err := e.bindPeer(ctx, conn)
			if err != nil {
				return protocol.ConnectionHandlers{}, fmt.Errorf("bind broadcast peer: %w", err)
			}
			return protocol.ConnectionHandlers{
				ByCodepoint: map[protocol.Codepoint]protocol.Handlers{
					bcastCodepoint:   {AcceptUni: peer.acceptBcast},
					sessionCodepoint: {AcceptUni: peer.acceptSession},
					chunkCodepoint:   {AcceptUni: peer.acceptChunk},
				},
				Done:  peer.done,
				Close: peer.Close,
			}, nil
		},
	}
}

func (e *Engine) bindPeer(ctx context.Context, conn transport.Conn) (*PeerConn, error) {
	bound := make(chan *PeerConn, 1)
	select {
	case e.eventCh <- engineEvent{kind: evPeerConnected, conn: conn, ctx: ctx, bound: bound}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.ctx.Done():
		return nil, e.ctx.Err()
	}
	select {
	case peer := <-bound:
		return peer, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.ctx.Done():
		return nil, e.ctx.Err()
	}
}
