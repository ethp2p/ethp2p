package protocol

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/ethp2p/ethp2p/transport"
	"google.golang.org/protobuf/encoding/protowire"
)

var (
	ErrInvalidDescriptor = errors.New("invalid protocol descriptor")
	ErrReservedCodepoint = errors.New("reserved protocol codepoint")
	ErrCodepointInUse    = errors.New("protocol codepoint already registered")
	ErrNameInUse         = errors.New("protocol name already registered")
	ErrInvalidHandler    = errors.New("invalid protocol handler")
)

// Codepoint identifies an ethp2p stream protocol on the wire.
type Codepoint uint64

// Descriptor describes one registered stream protocol.
type Descriptor struct {
	Codepoint Codepoint
	Name      string
}

// Handlers accepts inbound streams for one protocol on one connection. A
// handler takes ownership of its stream and may apply subsystem backpressure.
type Handlers struct {
	AcceptBi  func(transport.Stream)
	AcceptUni func(transport.ReceiveStream)
}

// ConnectionHandlers holds one subsystem's handlers and lifetime for one
// connection.
type ConnectionHandlers struct {
	ByCodepoint map[Codepoint]Handlers
	Done        <-chan struct{}
	Close       func()
}

// Set describes one subsystem's protocols and binds their handlers to one
// connection.
type Set struct {
	Descriptors []Descriptor
	Bind        func(context.Context, transport.Conn) (ConnectionHandlers, error)
}

// Registry holds the protocol sets enabled by one ethp2p stack. Its zero value
// is ready to use.
type Registry struct {
	mu     sync.RWMutex
	byCode map[Codepoint]Descriptor
	sets   []Set
}

// Register adds a protocol set to the registry.
func (r *Registry) Register(set Set) error {
	if len(set.Descriptors) == 0 {
		return fmt.Errorf("%w: empty set", ErrInvalidDescriptor)
	}
	if set.Bind == nil {
		return fmt.Errorf("%w: nil bind function", ErrInvalidHandler)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for i, descriptor := range set.Descriptors {
		if descriptor.Name == "" {
			return fmt.Errorf("%w: empty name", ErrInvalidDescriptor)
		}
		if err := validateCodepoint(descriptor.Codepoint); err != nil {
			return err
		}
		for codepoint, registered := range r.byCode {
			if codepoint == descriptor.Codepoint {
				return fmt.Errorf("%w: %d is %q", ErrCodepointInUse, descriptor.Codepoint, registered.Name)
			}
			if registered.Name == descriptor.Name {
				return fmt.Errorf("%w: %q uses %d", ErrNameInUse, descriptor.Name, codepoint)
			}
		}
		for _, pending := range set.Descriptors[i+1:] {
			if pending.Codepoint == descriptor.Codepoint {
				return fmt.Errorf("%w: %d appears twice", ErrCodepointInUse, descriptor.Codepoint)
			}
			if pending.Name == descriptor.Name {
				return fmt.Errorf("%w: %q appears twice", ErrNameInUse, descriptor.Name)
			}
		}
	}
	if r.byCode == nil {
		r.byCode = make(map[Codepoint]Descriptor)
	}
	descriptors := slices.Clone(set.Descriptors)
	for _, descriptor := range descriptors {
		r.byCode[descriptor.Codepoint] = descriptor
	}
	set.Descriptors = descriptors
	r.sets = append(r.sets, set)
	return nil
}

// Bind binds every registered set to conn and returns its flat route table.
func (r *Registry) Bind(ctx context.Context, conn transport.Conn) (*Routes, error) {
	r.mu.RLock()
	sets := slices.Clone(r.sets)
	r.mu.RUnlock()

	routes := &Routes{
		byCode: make(map[Codepoint]Handlers),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}
	for _, set := range sets {
		bound, err := set.Bind(ctx, conn)
		if err != nil {
			routes.Close()
			return nil, err
		}
		if bound.Done != nil {
			routes.watch(bound.Done)
		}
		if bound.Close != nil {
			routes.close = append(routes.close, bound.Close)
		}
		declared := make(map[Codepoint]struct{}, len(set.Descriptors))
		for _, descriptor := range set.Descriptors {
			declared[descriptor.Codepoint] = struct{}{}
			handler, ok := bound.ByCodepoint[descriptor.Codepoint]
			if !ok || handler.AcceptBi == nil && handler.AcceptUni == nil {
				routes.Close()
				return nil, fmt.Errorf("%w: %s", ErrInvalidHandler, descriptor.Name)
			}
			routes.byCode[descriptor.Codepoint] = handler
		}
		for codepoint := range bound.ByCodepoint {
			if _, ok := declared[codepoint]; !ok {
				routes.Close()
				return nil, fmt.Errorf("%w: undeclared codepoint %d", ErrInvalidHandler, codepoint)
			}
		}
	}
	return routes, nil
}

// Routes holds the handlers bound to one connection.
type Routes struct {
	byCode   map[Codepoint]Handlers
	done     chan struct{}
	closed   chan struct{}
	close    []func()
	doneOnce sync.Once
	once     sync.Once
	watchers sync.WaitGroup
}

// Lookup returns the handlers bound to codepoint.
func (r *Routes) Lookup(codepoint Codepoint) (Handlers, bool) {
	handler, ok := r.byCode[codepoint]
	return handler, ok
}

// Done closes when a bound protocol set stops using the connection.
func (r *Routes) Done() <-chan struct{} {
	return r.done
}

// Close releases every bound protocol set in reverse order.
func (r *Routes) Close() {
	r.once.Do(func() {
		close(r.closed)
		r.watchers.Wait()
		r.signalDone()
		for i := len(r.close) - 1; i >= 0; i-- {
			r.close[i]()
		}
	})
}

func (r *Routes) watch(done <-chan struct{}) {
	r.watchers.Go(func() {
		select {
		case <-done:
			r.signalDone()
		case <-r.closed:
		}
	})
}

func (r *Routes) signalDone() {
	r.doneOnce.Do(func() { close(r.done) })
}

// WriteSelector writes one Protobuf unsigned-varint codepoint at the start of
// a stream. The selected protocol applies to the rest of the stream.
func WriteSelector(w io.Writer, codepoint Codepoint) error {
	if err := validateCodepoint(codepoint); err != nil {
		return err
	}

	selector := protowire.AppendVarint(nil, uint64(codepoint))

	n, err := w.Write(selector)
	if err == nil && n != len(selector) {
		return io.ErrShortWrite
	}
	return err
}

// ReadSelector reads the stream's initial unsigned-varint codepoint. It validates
// the encoding only; the caller resolves the codepoint to a handler. If r buffers
// reads, the handler must continue reading through r to retain protocol data.
func ReadSelector(r io.ByteReader) (Codepoint, error) {
	value, err := binary.ReadUvarint(r)
	return Codepoint(value), err
}

func validateCodepoint(codepoint Codepoint) error {
	switch codepoint {
	case 0:
		return fmt.Errorf("%w: 0", ErrReservedCodepoint)
	case Codepoint('/'):
		return fmt.Errorf("%w: %d conflicts with libp2p", ErrReservedCodepoint, codepoint)
	default:
		return nil
	}
}
