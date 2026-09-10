package transport

import (
	"context"
	"sync"

	"github.com/quic-go/quic-go"
)

// PeerID is the binary multihash representation used by libp2p peer.ID. A
// PeerID is not the human-readable base-encoded form of that multihash.
type PeerID string

// ConnDir records which endpoint sent the first QUIC packet. Its zero
// value is ConnDirOut. No Conn reports it: the dialing side already knows
// whether it dialed or accepted, and the connection manager carries the
// direction alongside the connection it owns.
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
	// RemotePeerID returns the identity authenticated for the remote endpoint,
	// or the caller-supplied identity for connections built by NewQUICConn. It
	// returns the empty PeerID when the connection carries neither. The local
	// identity belongs to the owning endpoint, not to the connection.
	RemotePeerID() PeerID
}

// NewQUICConn routes streams from an already-authenticated QUIC connection and
// returns its ethp2p view. The caller must not accept streams directly from raw.
// Closing the returned view closes raw.
//
// remote is the identity reported for the remote endpoint, used when the
// connection carries no ethp2p TLS certificate to derive one from. Passing the
// empty PeerID falls back to the certificate, which is only meaningful for a
// connection that did complete an ethp2p handshake.
func NewQUICConn(raw *quic.Conn, remote PeerID) Conn {
	// Routing ends when raw closes; this adapter does not wait for its
	// dispatchers.
	var wg sync.WaitGroup
	sc := newSharedConn(raw, &wg, remote)

	// Single-view adapter: the libp2p view never exists, so mark it done at
	// birth. Its code never wins; the ethp2p view always closes last.
	_ = sc.libp2p().CloseWithError(appNoError, "closed")
	return sc.ethp2p()
}
