// Package ethp2p assembles the ethp2p stack. It holds the node's
// authenticated identity and the key material shared with libp2p, without
// depending on libp2p itself.
package ethp2p

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
)

// PrivKey is the private-key capability the stack requires. libp2p's
// crypto.PrivKey satisfies it; adapters live outside this package.
type PrivKey interface {
	// Sign signs data with the private key.
	Sign(data []byte) ([]byte, error)
}

// Stack is the top-level ethp2p assembly. Set the fields, then call Init.
type Stack struct {
	// PeerID is the node's authenticated peer ID, shared with libp2p.
	PeerID transport.PeerID
	// Key authenticates connections ethp2p initiates.
	Key PrivKey
	// BroadcastConfig configures the broadcast engine.
	BroadcastConfig broadcast.EngineConfig
	// Transport is the shared QUIC endpoint handed off by the application.
	// It is optional; when nil, Stack manages no transport. When set,
	// Close tears the whole endpoint down after active connections drain.
	Transport *transport.TransportShared

	initialized atomic.Bool
	protocols   protocol.Registry
	engine      *broadcast.Engine

	mu        sync.Mutex
	closed    bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeErr  error
	closeDone chan struct{}
}

type Config struct {
	Key       PrivKey
	Broadcast broadcast.EngineConfig
}

func NewStack(cfg *Config) *Stack {
	panic("unimplemented: use Stack literal + Init")
	// TODO move all configuration validation here from Init
	// TODO PeerID should be derived from the PrivKey
	// TODO let's try to reconcile the PrivKey type here with transport/crypto (that one is more complete I think)
	// TODO add a Router abstraction to route protocols
	// TODO Init will need to start the shared transport?
}

// Init validates the configuration and constructs the broadcast engine. It is
// an error to call Init on an already-initialized stack.
func (s *Stack) Init() error {
	if !s.initialized.CompareAndSwap(false, true) {
		return errors.New("stack already initialized")
	}
	if s.PeerID == "" {
		return errors.New("peer ID is required")
	}
	if s.Key == nil {
		return errors.New("key is required")
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.closeDone = make(chan struct{})
	s.engine = broadcast.NewEngine(s.BroadcastConfig)
	if err := s.protocols.Register(s.engine.Protocols()); err != nil {
		s.engine.Close()
		s.cancel()
		return fmt.Errorf("register broadcast protocols: %w", err)
	}
	return nil
}

// ServeConn routes inbound streams on one authenticated ethp2p connection.
func (s *Stack) ServeConn(ctx context.Context, conn transport.Conn) error {
	if conn == nil {
		return errors.New("connection is required")
	}
	s.mu.Lock()
	if !s.initialized.Load() || s.engine == nil {
		s.mu.Unlock()
		return errors.New("stack is not initialized")
	}
	if s.closed {
		s.mu.Unlock()
		return errors.New("stack is closed")
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	serveCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()

	routes, err := s.protocols.Bind(serveCtx, conn)
	if err != nil {
		conn.Close()
		return fmt.Errorf("bind protocols: %w", err)
	}
	defer routes.Close()

	errs := make(chan error, 2)
	go func() { errs <- serveBi(serveCtx, conn, routes) }()
	go func() { errs <- serveUni(serveCtx, conn, routes) }()

	var result error
	completed := 0
	select {
	case err := <-errs:
		result = err
		completed++
	case <-routes.Done():
	}
	cancel()
	_ = conn.Close()
	for completed < 2 {
		result = errors.Join(result, <-errs)
		completed++
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	return result
}

func serveBi(ctx context.Context, conn transport.Conn, routes *protocol.Routes) error {
	for {
		stream, err := conn.AcceptBiStream(ctx)
		if err != nil {
			return err
		}
		routeBi(stream, routes)
	}
}

func serveUni(ctx context.Context, conn transport.Conn, routes *protocol.Routes) error {
	for {
		stream, err := conn.AcceptUniStream(ctx)
		if err != nil {
			return err
		}
		routeUni(stream, routes)
	}
}

func routeBi(stream transport.Stream, routes *protocol.Routes) {
	reader := bufio.NewReader(stream)
	codepoint, err := protocol.ReadSelector(reader)
	if err != nil {
		stream.Reset()
		return
	}
	handlers, ok := routes.Lookup(codepoint)
	if !ok || handlers.AcceptBi == nil {
		stream.Reset()
		return
	}
	handlers.AcceptBi(bufferedStream{Stream: stream, reader: reader})
}

func routeUni(stream transport.ReceiveStream, routes *protocol.Routes) {
	reader := bufio.NewReader(stream)
	codepoint, err := protocol.ReadSelector(reader)
	if err != nil {
		stream.CancelRead(0)
		return
	}
	handlers, ok := routes.Lookup(codepoint)
	if !ok || handlers.AcceptUni == nil {
		stream.CancelRead(0)
		return
	}
	handlers.AcceptUni(bufferedReceiveStream{ReceiveStream: stream, reader: reader})
}

// bufferedStream retains selector read-ahead for the handler while forwarding
// writes, cancellation, and deadlines to the original stream.
type bufferedStream struct {
	transport.Stream
	reader *bufio.Reader
}

func (s bufferedStream) Read(p []byte) (int, error) { return s.reader.Read(p) }

// bufferedReceiveStream retains selector read-ahead for a unidirectional handler.
type bufferedReceiveStream struct {
	transport.ReceiveStream
	reader *bufio.Reader
}

func (s bufferedReceiveStream) Read(p []byte) (int, error) { return s.reader.Read(p) }

// Close stops the stack and waits for active connections.
func (s *Stack) Close() error {
	s.mu.Lock()
	if s.closeDone == nil {
		s.closeDone = make(chan struct{})
	}
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closeErr
	}
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()

	// Tear down the shared endpoint first: parked Accept loops fail and
	// the socket dies, so active connections drain below instead of hanging.
	var transportErr error
	if s.Transport != nil {
		transportErr = s.Transport.Close()
	}
	s.wg.Wait()
	var err error
	if s.engine != nil {
		err = s.engine.Close()
	}
	err = errors.Join(transportErr, err)
	s.mu.Lock()
	s.closeErr = err
	close(s.closeDone)
	s.mu.Unlock()
	return err
}

// ID returns the authenticated peer ID of this node. Valid after Init.
func (s *Stack) ID() string { return string(s.PeerID) }

// BroadcastEngine returns the stack's broadcast engine, constructed by Init.
func (s *Stack) BroadcastEngine() *broadcast.Engine { return s.engine }
