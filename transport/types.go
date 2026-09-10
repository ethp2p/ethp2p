package transport

import (
	"context"
	"sync"

	"github.com/quic-go/quic-go"
)

var _ Conn = (*ethp2pConn)(nil)

// PeerID is the binary multihash representation used by libp2p peer.ID. A
// PeerID is not the human-readable base-encoded form of that multihash.
type PeerID string

// AuthInfo identifies both ends of an ethp2p connection and carries the
// identity key authenticated for the remote endpoint. NewShared derives these
// values from secp256k1 identity keys; NewQUICConn trusts caller-supplied values.

// TODO now that we have the ethp2p.Stack which holds our local identity,
// drop local peer from AuthInfo. And the remote PeerID can be obtained from the
// PubKey, so drop the field too.
type AuthInfo struct {
	// Local is the identity derived from the key passed to NewShared,
	// or supplied by the caller of NewQUICConn.
	Local PeerID
	// Remote is the identity bound to the remote TLS certificate.
	Remote PeerID
	// RemoteKey is the public key authenticated for the remote endpoint.
	// It is nil for conns built by NewQUICConn unless the caller supplies it.
	RemoteKey *PubKey
}

// ConnDir records which endpoint sent the first QUIC packet. Its zero
// value is ConnDirOut.
type ConnDir int

const (
	// ConnDirOut means the local endpoint initiated the connection.
	ConnDirOut ConnDir = iota
	// ConnDirIn means the remote endpoint initiated the connection.
	ConnDirIn
)

// Conn is the ethp2p view of an authenticated QUIC connection. A Conn can be
// used concurrently by multiple goroutines.
type Conn interface {
	// OpenStream waits for permission to open a bidirectional stream. It returns
	// when the stream opens, ctx ends, or the connection closes.
	OpenStream(context.Context) (Stream, error)
	// AcceptBiStream waits for the next bidirectional stream routed to ethp2p. It
	// returns when a stream arrives, ctx ends, or the connection closes.
	AcceptBiStream(context.Context) (Stream, error)
	// OpenUniStream waits for permission to open a unidirectional send stream.
	// It returns when the stream opens, ctx ends, or the connection closes.
	OpenUniStream(context.Context) (SendStream, error)
	// AcceptUniStream waits for the next unidirectional receive stream. All
	// incoming unidirectional streams on an ethp2p_0 connection route here.
	AcceptUniStream(context.Context) (ReceiveStream, error)
	// SendDatagram queues payload as one QUIC datagram. It does not wait for
	// delivery and does not inspect ctx.
	SendDatagram(context.Context, []byte) error
	// RecvDatagram waits for the next QUIC datagram. It returns when a datagram
	// arrives, ctx ends, or the connection closes.
	RecvDatagram(context.Context) ([]byte, error)
	// Close releases the ethp2p view. The QUIC connection closes when both views
	// are released. Until then, Close does not interrupt this view's I/O.
	Close() error
	// SupportsStreams always reports true for this QUIC transport.
	SupportsStreams() bool
	// SupportsDatagrams reports whether both peers enabled QUIC datagrams.
	SupportsDatagrams() bool
	// ConnectionStats returns QUIC bytes sent and received. The sent count
	// includes retransmissions. The received count includes duplicate stream
	// data. Neither count includes UDP framing.
	ConnectionStats() (bytesSent, bytesReceived uint64)
	// Direction reports which endpoint initiated the QUIC connection.
	Direction() ConnDir
	// AuthInfo returns the connection's authenticated identities, including
	// the remote public key. NewQUICConn returns the caller-supplied metadata.
	AuthInfo() AuthInfo
}

// NewQUICConn routes streams from an already-authenticated QUIC connection and
// returns its ethp2p view. It trusts auth without verification. The caller must
// not accept streams directly from raw. Closing the returned view closes raw.
func NewQUICConn(raw *quic.Conn, direction ConnDir, auth AuthInfo) Conn {
	var wg sync.WaitGroup // Routing ends when raw closes; this adapter does not wait.
	sc := &sharedConn{
		conn:      raw,
		sem:       make(chan struct{}, maxPendingStreamsPerConn),
		libp2pBi:  make(chan *quic.Stream, deliveryQueueLen),
		ethp2pBi:  make(chan *quic.Stream, deliveryQueueLen),
		ethp2pUni: make(chan *quic.ReceiveStream, uniDeliveryQueueLen),
	}
	for range maxPendingStreamsPerConn {
		sc.sem <- struct{}{}
	}
	wg.Go(func() { sc.drainBidi(&wg) })
	wg.Go(sc.drainUni)

	// Single-view adapter: the libp2p view never exists, so mark it done at
	// birth. Its code never wins; the ethp2p view always closes last.
	_ = sc.libp2p().CloseWithError(appNoError, "closed")
	ethp2p := sc.ethp2p()
	ethp2p.dir = direction
	ethp2p.auth = auth
	return ethp2p
}
