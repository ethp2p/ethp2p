package transport

import (
	"context"
)

// PeerID is the binary multihash representation used by libp2p peer.ID. A
// PeerID is not the human-readable base-encoded form of that multihash.
type PeerID string

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
	// Close releases the ethp2p view. Pending and later accepts, opens, and
	// datagram calls fail, and streams arriving for this view are reset. Streams
	// already handed out remain usable until the physical connection closes.
	// The QUIC connection closes when both views are released.
	Close() error
	// SupportsDatagrams reports whether both peers enabled QUIC datagrams.
	SupportsDatagrams() bool
	// ConnectionStats returns QUIC bytes sent and received. The sent count
	// includes retransmissions. The received count includes duplicate stream
	// data. Neither count includes UDP framing.
	ConnectionStats() (bytesSent, bytesReceived uint64)
	// RemotePeerID returns the identity authenticated for the remote endpoint.
	// The local identity belongs to the owning endpoint, not to the connection.
	RemotePeerID() PeerID
}
