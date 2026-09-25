// Package trace defines the events recorded by deterministic network tests.
// It has no dependency on the protocol packages that emit its events.
package trace

// Event is a typed trace event. Only this package can implement it.
type Event interface{ traceEvent() }

// Sink receives events. Emitters construct an event only when
// devhook.Hooks.Tracing reports true.
type Sink interface {
	// Emit accepts a value event; the receiver may retain it.
	Emit(Event)
}
