package transport

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type deadliner interface {
	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

var ErrDatagramsUnsupported = errors.New("datagrams not supported yet")

// PeerID is an authenticated peer identifier. It uses the canonical libp2p
// string encoding, so it converts to and from libp2p's peer.ID freely.
type PeerID string

// AuthInfo contains the authenticated identities at both ends of a connection.
type AuthInfo struct {
	Local  PeerID
	Remote PeerID
}

// Clone returns an independent copy of the authentication data.
func (a AuthInfo) Clone() AuthInfo { return a }

// ConnDirection indicates whether a connection was initiated by us or by them.
//
// TODO Deal with simultaneous open.
type ConnDirection int

const (
	Outbound ConnDirection = iota
	Inbound
)

// Conn is the low-level interface for stream and datagram operations with a peer.
type Conn interface {
	// TODO deconflict which peer opens the stream, or support simultaneous open.
	//
	// OpenStream opens a new outbound bidirectional stream.
	OpenStream(ctx context.Context) (Stream, error)
	// AcceptStream accepts an inbound bidirectional stream.
	AcceptStream(ctx context.Context) (Stream, error)

	// OpenUniStream opens a new outbound unidirectional stream.
	OpenUniStream(ctx context.Context) (SendStream, error)
	// AcceptUniStream accepts an inbound unidirectional stream.
	AcceptUniStream(ctx context.Context) (ReceiveStream, error)

	// SendDatagram sends a datagram.
	SendDatagram(ctx context.Context, payload []byte) error
	// RecvDatagram receives a datagram.
	RecvDatagram(ctx context.Context) ([]byte, error)

	// Close closes the transport.
	Close() error

	// SupportsStreams returns true if the transport supports streams.
	SupportsStreams() bool
	// SupportsDatagrams returns true if the transport supports datagrams.
	SupportsDatagrams() bool

	ConnectionStats() (bytesSent, bytesReceived uint64)

	Direction() ConnDirection

	// AuthInfo returns the authenticated identities at both ends of the
	// connection. Every transport authenticates with TLS 1.3, so every
	// connection carries it.
	AuthInfo() AuthInfo
}

// SendStream is a unidirectional write-only stream.
type SendStream interface {
	Write(p []byte) (int, error)
	Close() error
	CancelWrite(code uint64)
	SetWriteDeadline(time.Time) error
}

// ReceiveStream is a unidirectional read-only stream.
type ReceiveStream interface {
	Read(p []byte) (int, error)
	CancelRead(code uint64)
	SetReadDeadline(time.Time) error
}

// Stream is a bidirectional byte stream.
type Stream interface {
	deadliner

	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
	Reset() error
	CancelRead(code uint64)
	CancelWrite(code uint64)
}

// StreamResetError is returned by Read when the remote peer reset the stream
// with an application error code.
type StreamResetError struct {
	Code uint64
}

func (e *StreamResetError) Error() string {
	return fmt.Sprintf("stream reset: code 0x%02x", e.Code)
}
