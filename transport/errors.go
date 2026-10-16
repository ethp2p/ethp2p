package transport

import (
	"errors"
	"fmt"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

var (
	// ErrDialLegacyPeer reports that [Ethp2pTransport.Dial] authenticated the
	// peer but negotiated libp2p instead of ethp2p_0.
	ErrDialLegacyPeer = errors.New("peer dialed by ethp2p is legacy")
	// ErrViewClosed matches any ViewClosedError regardless of its code or origin.
	ErrViewClosed = errors.New("connection view closed")

	// ErrClosed reports an operation on a closed Endpoint or an Accept
	// on a detached libp2p listener.
	ErrClosed           = errors.New("endpoint closed")
	errAlreadyListening = errors.New("already listening")
	errNotWired         = errors.New("transport: Start before both views are wired")
	errAlreadyStarted   = errors.New("transport: Start called twice")
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

// ViewClosedError is the closure cause of an ethp2p view: a local CloseWithCode,
// or, with Remote set, a peer that closed the connection with a stack code.
type ViewClosedError struct {
	Code   wire.Code
	Remote bool
}

func (e *ViewClosedError) Error() string {
	if e.Remote {
		return fmt.Sprintf("peer closed ethp2p view: %s", e.Code)
	}
	return fmt.Sprintf("closed ethp2p view: %s", e.Code)
}

// Is matches ErrViewClosed independently of the code and origin.
func (e *ViewClosedError) Is(target error) bool { return target == ErrViewClosed }

// ethp2pError maps a remote connection close carrying a defined even stack
// code to a ViewClosedError; a local close's ViewClosedError is kept. An even
// code ParseCode does not round-trip is not a stack code: ParseCode's
// Unspecified fallback would misreport it as a view closure.
func ethp2pError(err error) error {
	if _, ok := errors.AsType[*ViewClosedError](err); ok {
		return err
	}
	app, ok := errors.AsType[*quic.ApplicationError](err)
	if !ok || !app.Remote || uint64(app.ErrorCode)&1 != 0 {
		return err
	}
	raw := uint64(app.ErrorCode)
	code := wire.ParseCode(raw)
	if code.Wire() != raw {
		return err
	}
	return &ViewClosedError{Code: code, Remote: true}
}

// ConnectionClosed reports whether a stream operation failed because the whole
// QUIC connection ended: an application or transport close, a stateless reset,
// or an idle timeout. quic-go returns these from blocked stream calls before
// [Conn.Done] is necessarily closed, so callers cannot use Done to tell a
// connection end from a stream-level failure.
func ConnectionClosed(err error) bool {
	_, app := errors.AsType[*quic.ApplicationError](err)
	_, transport := errors.AsType[*quic.TransportError](err)
	_, reset := errors.AsType[*quic.StatelessResetError](err)
	_, idle := errors.AsType[*quic.IdleTimeoutError](err)
	return app || transport || reset || idle
}
