// Package devhook carries test instrumentation into ethp2p components without
// widening their public configuration. Only internal/nettest attaches hooks.
package devhook

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ethp2p/ethp2p/internal/trace"
)

// Point names a place where a component can be paused.
type Point uint8

const (
	PointHandshake     Point = iota + 1 // Before the broadcast handshake completes.
	PointSessionOpen                    // Before a received SESS open reaches the channel.
	PointRoutingUpdate                  // Before a received routing update is applied.
	PointChunkRead                      // Before accepted inbound chunk data is read.
	PointChunkWrite                     // Before outbound chunk data is written.
)

// String identifies the gate point in a trace timeline.
func (p Point) String() string {
	switch p {
	case PointHandshake:
		return "handshake"
	case PointSessionOpen:
		return "session-open"
	case PointRoutingUpdate:
		return "routing-update"
	case PointChunkRead:
		return "chunk-read"
	case PointChunkWrite:
		return "chunk-write"
	default:
		panic(fmt.Sprintf("devhook: unknown point %d", p))
	}
}

// Site identifies a gate crossing. Empty fields are unknown at that point.
type Site struct {
	// Point is the required crossing to match.
	Point Point
	// Peer is an authenticated ID; it may be unknown before handshake.
	Peer string
	// Channel and Message are empty until their wire headers are available.
	Channel, Message string
}

// ChunkFault changes one outbound chunk. Its zero value sends the chunk unchanged.
type ChunkFault struct {
	// Err fails this write and is reported to the strategy.
	Err error
	// Data, when non-nil, replaces the outbound payload.
	Data []byte
	// Duplicate sends an identical second chunk on a new stream.
	Duplicate bool
}

// Hooks are optional instrumentation points. A nil pointer or field disables
// that point. The harness installs them before starting the component.
type Hooks struct {
	// Trace receives internal component decisions.
	Trace trace.Sink
	// Gate pauses a crossing at site while a matching hold is active. It
	// reports false when ctx ends first.
	Gate func(ctx context.Context, site Site) bool
	// ChunkWrite changes one outbound chunk before it is sent.
	ChunkWrite func(peer, channel, message string, chunk []byte) ChunkFault
	// MaxConcurrentReads overrides the session read limit; zero uses the default.
	MaxConcurrentReads int
}

// Tracing reports whether trace events are recorded. Emitters construct an
// event only when it returns true, so disabled tracing costs no allocation.
func (h *Hooks) Tracing() bool { return h != nil && h.Trace != nil }

// ReadLimit returns the overridden session read limit, or def when none is set.
func (h *Hooks) ReadLimit(def int) int {
	if h == nil || h.MaxConcurrentReads == 0 {
		return def
	}
	return h.MaxConcurrentReads
}

// FaultChunk returns the fault for one outbound chunk. It is the zero fault,
// which sends the chunk unchanged, when no ChunkWrite hook is set.
func (h *Hooks) FaultChunk(peer, channel, message string, chunk []byte) ChunkFault {
	if h == nil || h.ChunkWrite == nil {
		return ChunkFault{}
	}
	return h.ChunkWrite(peer, channel, message, chunk)
}

// Wait pauses at site while its gate holds. It reports false when ctx ends
// first, so callers can stop without touching closed state.
func (h *Hooks) Wait(ctx context.Context, site Site) bool {
	if h == nil || h.Gate == nil {
		return true
	}
	return h.Gate(ctx, site)
}

var attachers = make(map[reflect.Type]func(any, *Hooks))

// Register installs the attacher for configuration type T. Owning packages
// call it from init, before concurrent tests can call Attach.
func Register[T any](attach func(target *T, h *Hooks)) {
	typ := reflect.TypeFor[*T]()
	if _, exists := attachers[typ]; exists {
		panic("devhook: duplicate registration for " + typ.String())
	}
	attachers[typ] = func(target any, h *Hooks) { attach(target.(*T), h) }
}

// Attach wires h into a registered configuration pointer. It panics for an
// unregistered type.
func Attach(target any, h *Hooks) {
	attach, ok := attachers[reflect.TypeOf(target)]
	if !ok {
		panic(fmt.Sprintf("devhook: unregistered configuration %T", target))
	}
	attach(target, h)
}
