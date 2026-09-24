package transport

import (
	"context"

	"github.com/ethp2p/ethp2p/protocol"
)

// PeerID is the binary multihash representation used by libp2p peer.ID. A
// PeerID is not the human-readable base-encoded form of that multihash.
type PeerID string

// Conn is the ethp2p view of an authenticated QUIC connection. A Conn can be
// used concurrently by multiple goroutines.
type Conn interface {
	// Outbound reports whether this view was created by Dial.
	Outbound() bool
	// OpenStream waits for permission to open a bidirectional stream and writes
	// its selector frame before returning it.
	OpenStream(context.Context, protocol.Selector) (Stream, error)
	// AcceptStream returns a bidirectional stream with its selector frame consumed.
	AcceptStream(context.Context) (Stream, protocol.Selector, error)
	// OpenUniStream waits for permission to open a unidirectional send stream
	// and writes its selector frame before returning it.
	OpenUniStream(context.Context, protocol.Selector) (SendStream, error)
	// AcceptUniStream returns a unidirectional stream that skips its selector
	// frame on its first Read. The dispatcher consumes control streams separately.
	AcceptUniStream(context.Context) (ReceiveStream, protocol.Selector, error)
	// SendDatagram queues payload as one QUIC datagram. It does not wait for
	// delivery and does not inspect ctx.
	SendDatagram(context.Context, []byte) error
	// RecvDatagram waits for the next QUIC datagram. It returns when a datagram
	// arrives, ctx ends, or the connection closes.
	RecvDatagram(context.Context) ([]byte, error)
	// PeerHello waits for the peer's validated Hello and returns a fresh copy.
	// It may be called repeatedly and returns the closure cause if the view closes.
	PeerHello(context.Context) (Hello, error)
	// CloseWithCode sends GoAway with a stack code, then releases this view.
	CloseWithCode(protocol.Code) error
	// Close is CloseWithCode(protocol.Closing). Pending and later accepts, opens, and
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
