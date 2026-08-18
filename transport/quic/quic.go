// Package quic implements the Transport interface using QUIC.
package quic

import (
	"bytes"
	"context"
	"errors"

	"github.com/ethp2p/ethp2p/transport"
	quictls "github.com/ethp2p/ethp2p/transport/quic/tls"
	"github.com/quic-go/quic-go"
)

var (
	_ transport.Conn          = (*Conn)(nil)
	_ transport.Stream        = (*quicStream)(nil)
	_ transport.SendStream    = quicSendStream{}
	_ transport.ReceiveStream = quicReceiveStream{}
)

// quicStream wraps a quic-go stream to satisfy transport.Stream.
// quic-go uses a named type StreamErrorCode (not a type alias), so we
// need this adapter to convert uint64 arguments and avoid type leakage.
type quicStream struct {
	*quic.Stream
}

func (s quicStream) CancelRead(code uint64)  { s.Stream.CancelRead(quic.StreamErrorCode(code)) }
func (s quicStream) CancelWrite(code uint64) { s.Stream.CancelWrite(quic.StreamErrorCode(code)) }
func (s quicStream) Reset() error {
	s.Stream.CancelRead(quic.StreamErrorCode(0))
	s.Stream.CancelWrite(quic.StreamErrorCode(0))
	return s.Stream.Close()
}

// quicSendStream wraps a quic-go SendStream to satisfy transport.SendStream.
type quicSendStream struct {
	*quic.SendStream
}

func (s quicSendStream) CancelWrite(code uint64) {
	s.SendStream.CancelWrite(quic.StreamErrorCode(code))
}

// quicReceiveStream wraps a quic-go ReceiveStream to satisfy transport.ReceiveStream.
type quicReceiveStream struct {
	*quic.ReceiveStream
}

func (s quicReceiveStream) Read(p []byte) (int, error) {
	n, err := s.ReceiveStream.Read(p)
	if err != nil {
		var se *quic.StreamError
		if errors.As(err, &se) {
			return n, &transport.StreamResetError{Code: uint64(se.ErrorCode)}
		}
	}
	return n, err
}

func (s quicReceiveStream) CancelRead(code uint64) {
	s.ReceiveStream.CancelRead(quic.StreamErrorCode(code))
}

// Conn implements transport.Conn using QUIC.
type Conn struct {
	conn *quic.Conn
	dir  transport.ConnDirection
	auth transport.AuthInfo

	remoteID  quictls.ID
	remoteKey quictls.Key
}

// NewTransport creates a new QUIC transport from an underlying connection.
func NewTransport(conn *quic.Conn, dir transport.ConnDirection, auth transport.AuthInfo) *Conn {
	return &Conn{conn: conn, dir: dir, auth: auth}
}

// AuthInfo returns an independent copy of the authenticated endpoint data.
func (t *Conn) AuthInfo() transport.AuthInfo { return t.auth.Clone() }

// VerifyPeer derives the remote peer's identity from the connection's TLS
// state and checks it against expect (nil accepts any). For outbound
// connections the handshake already pinned the identity via ClientConfig;
// for inbound connections this is the only identity check. Call it before
// RemotePeer/RemoteKey.
func (t *Conn) VerifyPeer(expect quictls.ID) error {
	key, id, err := quictls.VerifyPeerCert(t.conn.ConnectionState().TLS.PeerCertificates)
	if err != nil {
		return err
	}
	if expect != nil && !bytes.Equal(expect, id) {
		return quictls.ErrPeerMismatch{Expected: expect, Actual: id}
	}
	t.remoteID, t.remoteKey = id, key
	return nil
}

// RemotePeer returns the verified remote peer ID.
func (t *Conn) RemotePeer() quictls.ID { return t.remoteID }

// RemoteKey returns the verified remote identity key.
func (t *Conn) RemoteKey() quictls.Key { return t.remoteKey }

// Direction returns whether this connection is inbound or outbound.
func (t *Conn) Direction() transport.ConnDirection { return t.dir }

// SupportsStreams returns true unconditionally.
func (t *Conn) SupportsStreams() bool {
	return true
}

// SupportsDatagrams returns true if the underlying QUIC connection supports datagrams.
func (t *Conn) SupportsDatagrams() bool {
	return t.conn.ConnectionState().SupportsDatagrams
}

// OpenStream opens a stream, blocking until there is capacity to do so.
// TODO consider using the async path here.
func (t *Conn) OpenStream(ctx context.Context) (transport.Stream, error) {
	s, err := t.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return quicStream{s}, nil
}

// AcceptStream returns the next stream opened by the peer, blocking until
// one is received.
func (t *Conn) AcceptStream(ctx context.Context) (transport.Stream, error) {
	s, err := t.conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return quicStream{s}, nil
}

// OpenUniStream opens a unidirectional outbound stream.
func (t *Conn) OpenUniStream(ctx context.Context) (transport.SendStream, error) {
	s, err := t.conn.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return quicSendStream{s}, nil
}

// AcceptUniStream accepts a unidirectional inbound stream.
func (t *Conn) AcceptUniStream(ctx context.Context) (transport.ReceiveStream, error) {
	s, err := t.conn.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	return quicReceiveStream{s}, nil
}

// SendDatagram sends a datagram. This will panic if called on a connection that does not
// support datagrams. Make sure to check SupportsDatagrams() before calling this method.
func (t *Conn) SendDatagram(_ context.Context, payload []byte) error {
	return t.conn.SendDatagram(payload)
}

// RecvDatagram receives a datagram. This will panic if called on a connection
// that does not support datagrams. Make sure to check SupportsDatagrams() before
// calling this method.
func (t *Conn) RecvDatagram(ctx context.Context) ([]byte, error) {
	return t.conn.ReceiveDatagram(ctx)
}

// Close closes the underlying QUIC connection with application error code 0.
func (t *Conn) Close() error {
	return t.conn.CloseWithError(0, "closed")
}

// ConnectionStats returns the total bytes sent and received on the connection.
// TODO make this return richer stats, without coupling to the quic-go type.
func (t *Conn) ConnectionStats() (uint64, uint64) {
	s := t.conn.ConnectionStats()
	return s.BytesSent, s.BytesReceived
}
