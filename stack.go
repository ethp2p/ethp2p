// Package ethp2p exchanges protocol support and routes streams to registered
// subsystems. Applications own connections and subsystem lifetimes.
package ethp2p

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/ethp2p/ethp2p/internal/ctxutil"
	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
)

const selectorTimeout = 5 * time.Second

// ErrNoProtocols means no shared selectors were accepted by subsystem policies.
var ErrNoProtocols = errors.New("no shared protocols")

// Stack exchanges selectors and delivers streams for connections supplied by
// the application. Its zero value is ready for subsystem registration.
// Registration and notification configuration freeze on the first ServeConn.
type Stack struct {
	mu         sync.Mutex
	serving    bool
	subsystems map[string]*Subsystem
	selectors  map[protocol.Selector]*Subsystem
}

// ServeConn exchanges selector advertisements on each endpoint's first outgoing
// unidirectional stream. Each advertisement begins with the reserved selector
// frame for protocol.AdvertisementSelector, followed by ascending selector frames
// and FIN. It then notifies matching subsystems and routes subsequent streams,
// which also begin with one selector frame. The caller must invoke it once per
// connection, before opening other unidirectional streams.
// It blocks until ctx ends, connection I/O fails, or negotiation fails. While
// peer notification is blocked by backpressure, only ctx can interrupt the send;
// the application must cancel ctx when it stops consuming notifications.
// It never closes conn, the endpoint, notification channels, or a subsystem.
// The caller owns shutdown of connections and streams already delivered.
func (s *Stack) ServeConn(ctx context.Context, conn transport.Conn) error {
	if conn == nil {
		return errors.New("nil connection")
	}
	s.mu.Lock()
	s.serving = true
	local := slices.Sorted(maps.Keys(s.selectors))
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	shared, err := s.exchange(ctx, conn, local)
	if err != nil {
		return err
	}
	routes, err := s.notifyPeers(ctx, conn, shared)
	if err != nil {
		return err
	}
	if len(routes) == 0 {
		return ErrNoProtocols
	}

	// Both loops must return before the borrowed connection leaves this scope.
	errs := make(chan error, 2)
	go func() { errs <- serveBi(ctx, conn, routes) }()
	go func() { errs <- serveUni(ctx, conn, routes) }()
	first := <-errs
	cancel()
	return errors.Join(first, <-errs)
}

func (s *Stack) exchange(ctx context.Context, conn transport.Conn, local []protocol.Selector) ([]protocol.Selector, error) {
	ctx, cancel := context.WithTimeout(ctx, selectorTimeout)
	defer cancel()

	// Both endpoints send while reading, so neither waits for the other to lead.
	written := make(chan error, 1)
	go func() {
		err := writeSelectors(ctx, conn, local)
		if err != nil {
			cancel()
		}
		written <- err
	}()
	remote, readErr := readSelectors(ctx, conn)
	if readErr != nil {
		cancel()
	}
	if err := errors.Join(readErr, <-written); err != nil {
		return nil, fmt.Errorf("exchange selectors: %w", err)
	}
	return protocol.Intersect(local, remote), nil
}

func writeSelectors(ctx context.Context, conn transport.Conn, selectors []protocol.Selector) error {
	stream, err := conn.OpenUniStream(ctx)
	if err != nil {
		return err
	}
	stop := ctxutil.OnCancel(ctx, func() { stream.CancelWrite(0) })
	err = protocol.WriteSelectors(stream, selectors)
	if err == nil {
		err = stream.Close() // FIN terminates the selector list.
	}
	stop()
	err = errors.Join(err, ctx.Err())
	if err != nil {
		stream.CancelWrite(0)
	}
	return err
}

func readSelectors(ctx context.Context, conn transport.Conn) ([]protocol.Selector, error) {
	stream, err := conn.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	stop := ctxutil.OnCancel(ctx, func() { stream.CancelRead(0) })
	selectors, err := protocol.ReadSelectors(stream)
	stop()
	// Cancellation aborts QUIC I/O with a reset; preserve its context cause too.
	err = errors.Join(err, ctx.Err())
	if err != nil {
		stream.CancelRead(0)
	}
	return selectors, err
}

func serveBi(ctx context.Context, conn transport.Conn, routes map[protocol.Selector]route) error {
	for {
		stream, err := conn.AcceptBiStream(ctx)
		if err != nil {
			return err
		}
		selector, err := readSelector(ctx, stream)
		route, ok := routes[selector]
		if err != nil || !ok || route.streams == nil {
			_ = stream.Reset()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		event := StreamEvent{Peer: route.peer, Selector: selector, Stream: stream}
		select {
		case route.streams <- event:
		case <-ctx.Done():
			_ = stream.Reset()
			return ctx.Err()
		}
	}
}

func serveUni(ctx context.Context, conn transport.Conn, routes map[protocol.Selector]route) error {
	for {
		stream, err := conn.AcceptUniStream(ctx)
		if err != nil {
			return err
		}
		selector, err := readSelector(ctx, stream)
		route, ok := routes[selector]
		if err != nil || !ok || route.streams == nil {
			stream.CancelRead(0)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		event := StreamEvent{Peer: route.peer, Selector: selector, Stream: stream}
		select {
		case route.streams <- event:
		case <-ctx.Done():
			stream.CancelRead(0)
			return ctx.Err()
		}
	}
}

// readSelector reads a stream's selector frame under selectorTimeout.
func readSelector(ctx context.Context, stream transport.ReceiveStream) (protocol.Selector, error) {
	ctx, cancel := context.WithTimeout(ctx, selectorTimeout)
	defer cancel()
	stop := ctxutil.OnCancel(ctx, func() { stream.CancelRead(0) })
	selector, err := protocol.ReadSelector(stream)
	stop()
	return selector, errors.Join(err, ctx.Err())
}
