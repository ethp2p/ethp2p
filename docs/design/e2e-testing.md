# Deterministic end-to-end tests

`internal/nettest` builds real `ethp2p.Stack` and `broadcast.Engine` nodes on
simnet packet connections. `Run` owns a `testing/synctest` bubble: it starts
the packet network, runs the scenario, closes every node and the network, and
then leaves the bubble. Tests use `Settle`, `Advance`, and `Await` for virtual
time. They do not open UDP sockets or use wall-clock waits.

Each node has a deterministic secp256k1 key derived from its name, a signed
ENR, and a sequential public IPv4 address. Construction registers the broadcast
engine with the stack, runs every `BeforeStart` callback in option order, and
then starts the stack. Callbacks can register additional subsystems before
registration freezes. The stack owns connection admission and stream delivery.

`Connect(a, b)` records a symmetric one-way packet latency (1 ms by default)
and calls `a.Stack.Connect` with b's signed record and a context bounded by
`DefaultTimeout`. Duplicate links panic. `Bandwidth` configures node bandwidth;
`Latency` configures a link. `Disconnect(a, b)` disconnects a's stack view of b,
closes both nodes' accepted libp2p views, and removes the harness link. Its latency
entry remains so packets already in flight can still be routed. Disconnecting an
absent link panics. `ConnInfo(t, a, b)` returns a's current stack connection
entry for b, failing with the trace timeline if absent. Reconnection yields a
new connection ID.

`Channel` attaches RS with `rs.DefaultConfig`; `nettest.Attach(nd, id, scheme)`
accepts any scheme through a generic top-level function. Each channel
continuously drains its subscription into a synchronized collection.
`Received`, `RequireReceived`, and `RequireNotReceived` settle the bubble
before reading that collection. `AwaitReceived` waits in virtual time, settles,
and checks exact-once delivery and payload bytes.

`AwaitPeers` compares the exact current peer set on a channel. It applies
subscribe, unsubscribe, and peer-gone events in trace order, so reconnects
cannot satisfy a wait with stale subscriptions. `Within` bounds an operation
that observes its supplied context at every blocking point; on timeout it
prints the timeline, cancels that context, and joins the operation. Call
non-cancellable shutdowns directly so a bubble deadlock exposes a hang.

Node shutdown cancels its context, disconnects its links, closes libp2p views
and the listener, closes the stack, closes channels in reverse attach order,
closes the engine, shared endpoint, and packet connection, then joins node
goroutines. `Run` releases all gates before shutting down nodes in reverse
construction order and reports unexpected errors; `transport.ErrClosed` and
`net.ErrClosed` are normal closure.

`internal/trace` owns the event types. It imports no ethp2p packages, allowing
stack and broadcast to emit into the same `Sink`; transport can join later.
The harness adapts every current `broadcast.Observer` callback to a typed
event. `Recorder` serializes emissions with a sequence number and virtual
timestamp. `Match` compares event types and nonzero fields; zero fields are
wildcards. Nested structs and nonnil slices compare exactly, while error
fields use `errors.Is`. Events are values; a pointer event panics at the
matcher or recorder boundary. Peer IDs render as short hex in an event's
`String` and as node names in the harness timeline. `Where` handles predicates.
`Require` matches an ordered
subsequence. On failure, or with `NETTEST_TRACE=1`, `Run` logs the entire
timeline. The observer records events in callback order, but concurrent
callbacks on different goroutines can interleave; tests should assert only
causal order.

`Run` joins every goroutine owned by each node before the bubble exits.
An intentionally leaked node goroutine would block that join, so a failing
leak test cannot run in-process alongside passing tests: `testing.T` cannot
isolate an expected bubble failure. The package self-tests exercise virtual
`Await` timeout, matching, ordering, and a live two-node round trip.

The `e2e/` package replaces the 14 scenarios formerly in `broadcast/tests/`
and adds a disconnect/reconnect regression. Stack-level and broadcast-internal
scenario migrations are separate units.
The shared transport scenario uses both a libp2p routed QUIC stream and an
ethp2p broadcast message on one physical connection. A full go-libp2p host on
that simnet endpoint remains unavailable: the pinned libp2p
`quicreuse.ConnManager.LendTransport` rejects the endpoint's specific local IP
address.

## Coverage reproduction

From a checkout where the pinned `ref/go-libp2p` replacement resolves, run:

```sh
GOCACHE=$(mktemp -d) go test -coverpkg=./broadcast/...,./transport/...,. \
  -coverprofile=/tmp/ethp2p-coverage.out \
  . ./broadcast ./broadcast/rs ./e2e ./enr ./internal/ctxutil \
  ./internal/devhook ./internal/nettest ./wire ./sim ./transport
python3 scripts/coverage_summary.py /tmp/ethp2p-coverage.out
```

Run the same commands at the base commit to compare results. The script
merges repeated source blocks with covered-if-any semantics, then reports the
total and requested production packages. The command lists packages with tests;
Go 1.27's distributed toolchain lacks `covdata`, which `go test -cover ./...`
tries to invoke for packages without tests. In worktrees without `ref/`, link it to the existing pinned fork checkout
before running the commands. No fork content or version
change is required.

## Broadcast internal scenarios

Unit 2 moves the mock-driven broadcast session, channel, engine, and peer
scenarios into `e2e/`. The remaining in-package tests in
`broadcast/protocol_test.go` and `broadcast/rs/strategy_test.go` check pure
selector and RS strategy calculations. The three existing microbenchmarks
retain small benchmark-only fixtures: their timed loops measure session
dispatch, outbound processing, and membership churn without simnet or QUIC
overhead. The membership benchmark lives in an external test package so it can
attach the real RS scheme without an import cycle; test-only bridges retain
its in-memory peer fixture. A fourth benchmark checks disabled trace allocation.

`internal/devhook` attaches optional instrumentation to `EngineConfig` without
adding public broadcast settings. A node owns one `Hooks` pointer, which the
engine passes to its channels, sessions, and peer bindings. Every internal
trace event is constructed inside a nil-sink guard. The harness stamps these
events into the same recorder as the public observer callbacks. The
`BenchmarkDisabledTrace` hot emit site is the allocation check for the
disabled path.

`Node.Hold(devhook.Site{Point: ...})` pauses a matching crossing. Empty peer,
channel, and message fields are wildcards. `GateHeld` confirms the component
reached the crossing; `GateReleased` confirms it resumed. Each wait selects
against the owning peer, session, or channel context. Closing that owner
therefore unblocks a held crossing, and `Run` releases all gates before closing
nodes. `PointChunkRead` occurs only at the session's inbound data read, after
the CHUNK header identifies its channel and message. The binding backpressure
scenario instead leaves a real CHUNK header incomplete to occupy the peer's
reader semaphore. `NodeOption ChunkFaults` can fail,
replace, or duplicate an outbound chunk, while `MaxConcurrentReads` overrides
the session read limit only in the harness. These controls let the E2E tests
hold a relay's final read until its peers complete, then assert that it
disposes only after reconstruction.

## Next units

1. Add stack negotiation events and raw peers for malformed-input scenarios.
2. Add transport view and selector events, then migrate component tests.
3. Revisit full go-libp2p interop when the endpoint and pinned reuse manager
   can share a compatible listening address without real sockets.
