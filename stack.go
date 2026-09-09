// Package ethp2p exchanges protocol support and routes streams to registered
// subsystems. Applications own connections and subsystem lifetimes.
package ethp2p

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

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

// ServeConn exchanges selectors on each endpoint's first outgoing unidirectional
// stream, notifies matching subsystems, and routes subsequent streams. The caller
// must invoke it once per connection, before opening other unidirectional streams.
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
	stop := onCancel(ctx, func() { stream.CancelWrite(0) })
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
	stop := onCancel(ctx, func() { stream.CancelRead(0) })
	selectors, err := protocol.ReadSelectors(bufio.NewReader(stream))
	stop()
	// Cancellation aborts QUIC I/O with a reset; preserve its context cause too.
	err = errors.Join(err, ctx.Err())
	if err != nil {
		stream.CancelRead(0)
	}
	return selectors, err
}

// onCancel returns cleanup that joins a callback already in flight. A stream
// must not reach its handler while selector cancellation can still reset it.
func onCancel(ctx context.Context, cancel func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		cancel()
	})
	return func() {
		if !stop() {
			<-done
		}
	}
}

func serveBi(ctx context.Context, conn transport.Conn, routes map[protocol.Selector]route) error {
	for {
		stream, err := conn.AcceptBiStream(ctx)
		if err != nil {
			return err
		}
		reader := bufio.NewReader(stream)
		selector, err := readSelector(ctx, stream, reader)
		route, ok := routes[selector]
		if err != nil || !ok || route.streams == nil {
			_ = stream.Reset()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		event := StreamEvent{Peer: route.peer, Selector: selector, Stream: bufferedStream{Stream: stream, reader: reader}}
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
		reader := bufio.NewReader(stream)
		selector, err := readSelector(ctx, stream, reader)
		route, ok := routes[selector]
		if err != nil || !ok || route.streams == nil {
			stream.CancelRead(0)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		event := StreamEvent{Peer: route.peer, Selector: selector, Stream: bufferedReceiveStream{ReceiveStream: stream, reader: reader}}
		select {
		case route.streams <- event:
		case <-ctx.Done():
			stream.CancelRead(0)
			return ctx.Err()
		}
	}
}

func readSelector(ctx context.Context, stream transport.ReceiveStream, reader *bufio.Reader) (protocol.Selector, error) {
	ctx, cancel := context.WithTimeout(ctx, selectorTimeout)
	defer cancel()
	stop := onCancel(ctx, func() { stream.CancelRead(0) })
	selector, err := protocol.ReadSelector(reader)
	stop()
	return selector, errors.Join(err, ctx.Err())
}

// These wrappers retain selector read-ahead for the handler while forwarding
// cancellation, writes, and deadlines to the original stream.
type bufferedStream struct {
	transport.Stream
	reader *bufio.Reader
}

func (s bufferedStream) Read(p []byte) (int, error) { return s.reader.Read(p) }

type bufferedReceiveStream struct {
	transport.ReceiveStream
	reader *bufio.Reader
}

func (s bufferedReceiveStream) Read(p []byte) (int, error) { return s.reader.Read(p) }
