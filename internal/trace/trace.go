// Package trace defines the events recorded by deterministic network tests:
// broadcast Observer callbacks and development hold crossings, both mirrored
// by internal/harness. It has no dependency on the protocol packages.
package trace

// Event is a typed trace event. Only this package can implement it.
type Event interface{ traceEvent() }
