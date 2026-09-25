package transport

import (
	"errors"
	"fmt"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

var (
	// ErrSinkBound reports an operation reserved for unbound transport mode.
	ErrSinkBound = errors.New("ethp2p transport is bound to a sink")
	// ErrDialLegacyPeer reports that [Ethp2pTransport.Dial] authenticated the
	// peer but negotiated libp2p instead of ethp2p_0.
	ErrDialLegacyPeer = errors.New("peer dialed by ethp2p is legacy")
	// ErrNoHello reports a Dial before the first successful SetHello.
	ErrNoHello = errors.New("ethp2p Hello not configured")
	// ErrViewClosed matches any ViewClosedError regardless of its code or origin.
	ErrViewClosed = errors.New("connection view closed")

	// ErrClosed reports an operation on a closed SharedTransport or an Accept
	// on a detached libp2p listener.
	ErrClosed           = errors.New("shared transport closed")
	errAlreadyListening = errors.New("already listening")
	errNoKeyExtension   = errors.New("expected certificate to contain the key extension")
	errCertChain        = errors.New("expected one certificate in the chain")
	errNilPacketConn    = errors.New("nil PacketConn")
	errNilKey           = errors.New("nil identity key")
	errInvalidSig       = errors.New("signature invalid")
)

const (
	// A libp2p-last normal release is Unspecified to an ethp2p receiver
	// still awaiting GoAway. Generic failures stay outside the stack namespace.
	appNoError quic.ApplicationErrorCode = 0
	appFailure quic.ApplicationErrorCode = 1
)

// ethp2pError interprets a remote connection close at the ethp2p boundary.
// Keep raw.Context (and its child view contexts) intact: cancellation remains
// synchronous with QUIC and requires no extra goroutine or shutdown handoff.
// GoAway's first-wins ViewClosedError takes precedence. Unknown even values
// must round-trip through ParseCode; its Unspecified fallback is not evidence
// that an arbitrary application code denotes a view closure.
func ethp2pError(err error) error {
	if _, ok := errors.AsType[*ViewClosedError](err); ok {
		return err
	}
	app, ok := errors.AsType[*quic.ApplicationError](err)
	if !ok || !app.Remote || uint64(app.ErrorCode)&1 != 0 {
		return err
	}
	raw := uint64(app.ErrorCode)
	code := wire.ParseCode(0, raw)
	if code.Wire() != raw {
		return err
	}
	return &ViewClosedError{Code: code, Remote: true}
}

// ErrPeerMismatch reports that a dial authenticated a valid peer identity that
// differs from the nonempty peer ID passed to [Ethp2pTransport.Dial].
type ErrPeerMismatch struct {
	// Expected is the peer ID supplied by the dialer.
	Expected PeerID
	// Actual is the peer ID derived from the remote TLS certificate.
	Actual PeerID
}

// Error returns both peer IDs in hexadecimal form.
func (e ErrPeerMismatch) Error() string {
	return fmt.Sprintf("peer ID mismatch: expected %x, actual %x", []byte(e.Expected), []byte(e.Actual))
}

// StreamResetError reports a QUIC stream cancellation returned by
// Stream.Read or ReceiveStream.Read.
type StreamResetError struct {
	// Code is the QUIC application error code carried by the cancellation.
	Code uint64
}

// Error formats Code in hexadecimal.
func (e *StreamResetError) Error() string {
	return fmt.Sprintf("stream reset: code 0x%02x", e.Code)
}
