package transport

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

const (
	// deliveryQueueLen bounds streams classified but not yet handed over.
	// Delivery is expected immediate, so this is just a safety buffer.
	deliveryQueueLen = 4
	// maxPendingStreamsPerConn limits concurrent stream classifications.
	// Fragmentation, packet loss, or a stalled peer can delay classification;
	// the bound limits resources held by streams awaiting their first bytes.
	maxPendingStreamsPerConn = 4
)

type sharedConn struct {
	conn *quic.Conn
	sem  chan struct{}

	// outboxes for classified incoming streams
	libp2pBi  chan *quic.Stream
	ethp2pBi  chan *quic.Stream
	ethp2pUni chan *quic.ReceiveStream

	// View-closed bitfield over sideLibp2p|sideEthp2p. The raw connection
	// closes once both views are closed, so closing one view never kills
	// the other. Accessed atomically.
	closed uint32
}

// closeSide releases one view and closes the connection when both are released.
func (s *sharedConn) closeSide(want side, code quic.ApplicationErrorCode, reason string) {
	// OrUint32 returns the pre-OR value. RMW atomicity means no other close
	// can land between its read and write, so old|want is the post-OR value.
	if atomic.OrUint32(&s.closed, uint32(want))|uint32(want) == uint32(sideLibp2p|sideEthp2p) {
		_ = s.conn.CloseWithError(code, reason)
	}
}

// split splits an ethp2p-negotiated connection into the shared state backing
// its two views. Callers build the views with ethp2p() and libp2p() and stamp
// the view metadata themselves. The caller owns the goroutine lifecycle:
// everything joins t.wg, so shutdown waits for the drainers and their
// classifiers.
func (t *TransportShared) split(raw *quic.Conn) *sharedConn {
	sc := &sharedConn{
		conn:      raw,
		sem:       make(chan struct{}, maxPendingStreamsPerConn),
		libp2pBi:  make(chan *quic.Stream, deliveryQueueLen),
		ethp2pBi:  make(chan *quic.Stream, deliveryQueueLen),
		ethp2pUni: make(chan *quic.ReceiveStream, deliveryQueueLen),
	}
	for range maxPendingStreamsPerConn {
		sc.sem <- struct{}{}
	}

	t.wg.Go(func() { sc.drainBidi(&t.wg) })
	t.wg.Go(sc.drainUni)

	return sc
}

// ethp2p returns the ethp2p view. The caller must set its authentication and direction.
func (r *sharedConn) ethp2p() *connEth {
	return &connEth{sc: r}
}

// libp2p returns the libp2p view of the shared connection.
func (r *sharedConn) libp2p() quicreuse.QUICConn {
	return (*connLib)(r)
}

// drainBidi accepts bidirectional streams and classifies them.
// The semaphore limits the number of concurrent classifications.
func (r *sharedConn) drainBidi(wg *sync.WaitGroup) {
	ctx := r.conn.Context()

	for {
		select {
		case <-r.sem:
		case <-ctx.Done():
			return
		}

		stream, err := r.conn.AcceptStream(ctx)
		if err != nil {
			r.sem <- struct{}{}
			if ctx.Err() == nil {
				slog.Warn("failed to accept stream", "err", err)
			}
			return
		}

		wg.Go(func() {
			defer func() { r.sem <- struct{}{} }()

			isEthp2p, err := classify(ctx, stream)
			if err != nil {
				slog.Warn("failed to classify stream", "err", err)
				stream.CancelRead(streamReset)
				stream.CancelWrite(streamReset)
				return
			}

			queue := r.libp2pBi
			if isEthp2p {
				queue = r.ethp2pBi
			}

			select {
			case queue <- stream:
			case <-ctx.Done():
				// connection was closed, no stream-level cleanup needed
			default:
				stream.CancelRead(streamReset)
				stream.CancelWrite(streamReset)
			}
		})
	}
}

// drainUni routes all incoming unidirectional streams to ethp2p; libp2p uses none.
func (r *sharedConn) drainUni() {
	ctx := r.conn.Context()

	for {
		stream, err := r.conn.AcceptUniStream(ctx)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("failed to accept stream", "err", err)
			}
			return
		}
		select {
		case r.ethp2pUni <- stream:
		case <-ctx.Done():
			// connection was closed, no stream-level cleanup needed
			return
		default:
			stream.CancelRead(streamReset)
		}
	}
}
