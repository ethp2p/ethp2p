# Stack connection management and shared QUIC lifetimes

Status: historical proposal, superseded by [008](008-stack.md).
The [implementation status](#implementation-status) below describes the reviewed 2026-09-08 baseline,
not the current branch. See [`stack.go`](../stack.go) for the implemented APIs.

This document describes the proposed evolution of `ethp2p.Stack` from a per-connection stream router
into the owner of ethp2p connection management.
It records the reasoning behind the proposal, the smallest fixes to the reviewed patch,
and the decisions that needed implementation evidence at that baseline.

The source baseline is commit `a0815c57`, compared with `main` at `b027eb51`.
Runtime observations below come from the review of that commit on 2026-09-08.
Proposed types and methods are sketches of the baseline design.
Some now exist with different contracts; inspect the implementation before using them.

## Implementation status

This section records what existed at commit `a0815c57`.
`NewStack`, `Start`, `Connect`, `Disconnect`, the connection table, and connection IDs
have since been implemented in [`stack.go`](../stack.go).

Implemented:

- Per-side interest registration releases unclaimed views.
  An unconsumed view no longer keeps a physical connection open.
- A full delivery queue releases only that side's view.
- `Ethp2pTransport.Dial` keeps the returned ethp2p view open when the libp2p queue is full.
- Closing the libp2p listener detaches libp2p without stopping the endpoint.
- Libp2p-initiated dials are libp2p-only.
- Selector reads are bounded and cancellable.
  Their cancellation callback is joined before stream handoff.
- Every ethp2p stream starts with a length-delimited selector frame.
  Each frame has a uvarint length and codepoint;
  the first outgoing unidirectional stream carries the advertisement header with reserved selector
  `0`.
- The shared dispatcher peeks at bidirectional frames without consuming bytes.
  Stack reads selectors for both stream directions.
- `Peer.OpenUniStream` and `Peer.OpenStream` write selectors for subsystems.
  The raw connection is not exposed through `Peer`.
- Remote identity comes from the handshake's verify callback.
- The direction constants were removed entirely, making the migration below moot.
- Closing a view fails its pending and later accept, open, and datagram calls.
  New streams routed to that view are reset.
- Incoming SESS and CHUNK streams wait in bounded queues for the BCAST handshake.
  A failed handshake and queue overflow cancel the affected streams.

Not yet implemented at the reviewed baseline:

- Resetting streams already handed out when a view closes.
- Outbound SESS stream admission that preserves CHUNK progress under stream-credit pressure.
- Stack-owned connection management: `NewStack`, `Start`, `Connect`,
  `Disconnect`, a connection table, and connection IDs.
- Dial coordination.
- The remaining open decisions.

## Recommendation

Keep one shared QUIC endpoint and one inbound dispatcher per physical connection.
Give each stack's connection view an independent, observable lifetime.
Make `Stack` responsible for admitting ethp2p connections, binding protocols, coordinating dials,
and removing connections after cleanup.

This design follows the requirement as if connection sharing had existed from the beginning.
The relevant objects are a physical QUIC connection, a stack's view of that connection,
and the protocols bound to the view.
Each object needs an owner and a termination contract.

The first implementation should support explicit connections.
Discovery, peer scoring, duty-aware selection,
and automatic reconnection can then operate through the same connection-management methods.
Those policies should not be prerequisites for correct connection cleanup.

## What the reviewed patch did

The implementation spans four responsibilities:

| Component | Baseline responsibility | Contract proposed at the baseline |
| --- | --- | --- |
| `SharedTransport` | Owns the QUIC endpoint and publishes libp2p and ethp2p views | Tracking streams already handed to a view so they can be stopped before physical connection close |
| `sharedConn` | Owns per-view cancellation, classifies bidirectional streams, and queues inbound streams | Completion tracking for streams already handed out |
| `Stack.ServeConn` | Negotiates selectors, binds protocols, and routes incoming streams; its `Peer` API writes selectors for subsystem opens | Connection identity, admission, dial coordination, and inspection |
| `broadcast.Engine` | Tracks peers and channel subscriptions, and ignores cleanup from a replaced binding | Connection admission and lifetime remain with the application |
| `broadcast.PeerConn` | Performs BCAST handshake and queues early SESS and CHUNK streams until it completes | Outbound SESS stream admission under stream-credit pressure |

At the reviewed baseline, the application accepted or dialed a connection and launched
`Stack.ServeConn` itself. The simulation retained raw connections for bandwidth accounting
in `BroadcastNode.conns`.

The baseline `Stack` had a wait group for active `ServeConn` calls.
It had no connection table and could not list connections, select a connection for a peer,
coalesce duplicate dials, or disconnect a specific peer.

The relevant implementation files are:

- [Stack construction and routing](../stack.go).
- [Shared endpoint lifecycle and physical stream dispatch](../transport/shared.go).
- [Ethp2p connection view](../transport/ethp2p.go).
- [Libp2p connection view](../transport/libp2p.go).
- [Protocol registration and binding](../protocol/protocol.go).
- [Broadcast connection lifecycle](../broadcast/peer.go).
- [Broadcast control and stream creation](../broadcast/peer_ctrl.go).
- [Simulation connection ownership](../sim/node.go).

## Evidence from the review

The review found five commit-specific issues:

| Issue | Observed result | Consequence |
| --- | --- | --- |
| Five incoming unidirectional streams | Eight concurrent broadcasts delivered zero messages before an eight-second timeout | Long-lived streams can consume the capacity needed for message data |
| Closing one connection view | `ServeConn` remained blocked after cancellation while the libp2p view stayed active | Connection cleanup can depend on shutting down the whole endpoint |
| Incomplete protocol selector | A valid later stream remained blocked for more than the classification timeout | One incomplete selector prevents later streams of that direction from reaching their handlers |
| Full outbound libp2p connection queue | Closing the returned ethp2p connection did not close QUIC after the seventeenth unconsumed publication | The undelivered libp2p view remains logically open |
| Removed direction constants | Integration tests failed to compile | Their callers did not complete the transport API migration |

Changing only `MaxIncomingUniStreams` from five to 256 in a temporary overlay allowed all eight
broadcasts to complete in 0.31 seconds.
This experiment identifies the immediate constraint.
It does not establish 256 as a sufficient limit for every workload.

The review also reproduced a hang after resetting BCAST while an idle SESS reader remained open.
The current `PeerConn` cancels inbound readers through its peer context and joins those readers
before the binding finishes.
Outbound SESS stream admission remains unresolved
because opening a stream can block the broadcast control loop
while QUIC stream credit is unavailable.

Existing untagged tests passed with fresh race-enabled runs,
including transport interoperability and simulation tests.
The integration-tagged broadcast tests failed at compilation.

Those passing results used the local patched libp2p checkout.
A clean export of the commit could not build
because `go.mod` replaces libp2p with the ignored `ref/go-libp2p` directory.
The dependency limitation has since been resolved by pinning the libp2p branch in `go.mod`.

The temporary reproduction tests were diagnostic artifacts outside the checkout.
They are not part of this proposal's committed verification suite.
Each implementation unit needs an equivalent repository test
before its behavior is considered established.

## Smallest fixes to the current patch

### Stream admission must preserve forward progress

BCAST, SESS, and CHUNK all use unidirectional streams.
BCAST is long-lived.
A SESS stream can remain open while its message needs chunks.
If those streams occupy every available slot,
CHUNK cannot open a stream to make the progress that would allow a session to finish.

The smallest workload fix is to raise the transport limit.
The smallest complete fix also needs admission control for outbound long-lived session streams.

The proposed policy is to bound concurrently open outbound SESS streams per connection across all
channels.
Excess session requests wait in a bounded queue.
Their chunk outboxes become eligible only after their session stream opens.

The broadcast control loop must remain able to process session completion and stream closure
while new sessions wait for capacity:

```go
// CURRENT: opening a SESS stream can block the control loop.
s, err := p.streams.OpenUniStream(p.ctx, SESS)

// PROPOSED: conceptual control-loop behavior.
switch event := event.(type) {
case peerOpenSession:
	enqueueSession(event)
case peerCloseSession:
	closeSession(event)
	releaseSessionSlot()
}
tryPendingSessions() // Must not block the control loop on QUIC credit.
```

This is not yet a complete allocator design.
QUIC's incoming stream limit belongs to the remote endpoint.
A local `Close` also does not mean the remote endpoint has immediately returned stream credit.
Other registered protocols can consume unidirectional streams too.

The implementation must therefore define how it learns the usable budget,
how it preserves CHUNK capacity, and how blocked open attempts resume.
It must also define what happens when the pending session queue is full.
A formula based only on the local transport configuration is insufficient
when peers can use different limits.

Keep the first implementation within transport stream allocation
and the existing broadcast control loop.
A general priority scheduler would add scope
before the current workload has a proven admission policy.

Evidence that would change this recommendation would be a QUIC allocation mechanism
that already reserves credit by stream purpose,
or a different session lifetime that removes the dependency on CHUNK progress.

### A closed view must stop its operations

Partially implemented: closed views stop calls; handed-out streams remain live.

Originally, `sharedConn.closeSide` only changed a bit and closed physical QUIC
when both views had closed.
Open and accept operations did not observe those bits.

Retain the physical-close rule, but add a real lifetime to each view:

```go
// CURRENT: only the physical connection has a usable lifetime.
type sharedConn struct {
	conn   *quic.Conn
	closed uint32
	// Delivery queues omitted.
}

// PROPOSED: conceptual ownership, not complete declarations.
type connView struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	closed  bool
	streams map[streamID]ownedStream
}

type sharedConn struct {
	conn *quic.Conn
	eth  connView
	lib  connView
	// Delivery queues and physical-close coordination omitted.
}
```

Closing a view prevents new stream registration, cancels pending operations,
and aborts the streams that view owns.
It then retires its interest in the physical connection.
Physical QUIC closes when both interests retire, or when the endpoint or remote peer closes it.

Stream registration and closure must use the same synchronization boundary.
If an open completes after the view has closed,
registration fails and the newly opened stream is aborted.
The dispatcher must apply the same rule before handing an inbound stream to a view.

The stream set cannot grow for the lifetime of the connection.
Completed streams must leave it.
A bidirectional stream remains owned until both halves finish.
Closing the write half alone does not release a blocked reader.

There is an implementation decision here that the earlier type sketch does not resolve.
The current libp2p interface returns concrete `*quic.Stream` values.
A wrapper cannot automatically intercept every consumer read through that interface.
The implementation needs a verified completion hook,
or a narrow change to the stream adaptation boundary.
It must not assume a write-side context signals completion of both halves.

Libp2p already performs some stream cleanup in its own connection wrapper.
That behavior should be reused where its contract is sufficient,
rather than duplicated without analysis.
The transport must still handle queued streams, pending accepts, and operations racing with closure.

### Read the complete selector with bounded cancellation

This fix is implemented by `readSelector` for both `Stack` routing paths.

The shared dispatcher peeks at the bidirectional frame length
and first payload byte to distinguish libp2p from ethp2p without consuming either byte.
It clears its deadline after that classification.
`Stack` then reads the complete selector synchronously for both stream directions.

The implemented fix is a bounded, cancellable selector read in both routing paths.
Join the cancellation callback before the handler takes ownership of the stream.
Reset only the offending stream on failure.

```go
// Both serveBi and serveUni use the same complete selector reader.
selector, err := readSelector(ctx, stream)
if err != nil {
	return
}
// readSelector has finished its cancellation callback.
dispatch(selector, stream)
```

The helper must account for a cancellation callback that has already started.
Calling the stop function returned by `context.AfterFunc` does not itself wait
for a running callback.
Ownership must not pass to a handler while selector cleanup can still reset its stream.

The timeout bounds the delay of the current serial routing loop.
It does not make that loop independent of every slow selector or blocking handler.
Bounded concurrent selector reads are a possible later improvement.
That change must preserve broadcast's bounded pre-handshake queues
and avoid unbounded goroutine creation.

### Dispose of the undelivered libp2p view

This fix is implemented by `SharedTransport.offerLibp2p`.

The outbound overflow fix is called from
[Ethp2pTransport.Dial](../transport/ethp2p.go):

```go
// BASELINE: the returned view was closed on libp2p queue overflow.
select {
case t.libQ <- sc.libp2p():
default:
	_ = ethp2p.Close()
}

// CURRENT: offerLibp2p releases the libp2p view if its queue is full.
t.shared.offerLibp2p(sc.libp2p())
```

The returned ethp2p view remains usable.
The view that nobody received releases its interest in the physical connection.

### Complete the direction-constant migration

This migration is moot: direction constants were removed entirely.
The integration helpers no longer use them.

## Ownership from first principles

The proposed separation is:

```text
SharedTransport
  owns the shared QUIC endpoint
  owns physical connections and their dispatchers
    libp2p view
      owns libp2p operations and streams
    ethp2p view
      owns ethp2p operations and streams
      Stack owns admission and the connection runtime
        protocol bindings own subsystem state
```

`Stack` owns the ethp2p view after admission.
It does not decide whether libp2p still needs its view of the same physical connection.

Broadcast owns channel subscriptions and message sessions.
It can report that its connection binding has failed,
but it should not be the only component that knows an authenticated connection exists.

The current shutdown order closes the libp2p host, then `SharedTransport`,
then the supplied packet connection.
The proposed `Stack.Close` would own the shared endpoint after a successful construction.
Per-peer `Disconnect` closes ethp2p views only.
A node-wide ban or physical disconnection is a different operation and needs explicit policy.

The proposed endpoint shutdown contract must also state who closes the supplied `net.PacketConn`.
Currently the application closes it after `SharedTransport`.
Resolving that ownership for the proposed `Stack` is part of implementation.

## Stack construction and public methods

At the reviewed baseline, `Stack` held identity apart from its transport:

```go
// BASELINE: simplified from stack.go at a0815c57.
type Stack struct {
	PeerID          transport.PeerID
	Key             PrivKey
	BroadcastConfig broadcast.EngineConfig
	Transport       *transport.SharedTransport
	// Initialization and shutdown fields omitted.
}

func (s *Stack) Init() error
func (s *Stack) ServeConn(context.Context, transport.Conn) error
```

The stack has since acquired the connection ownership API described in spec 008.
The design below records its rationale; consult [`stack.go`](../stack.go) for current behavior.
At the baseline, `Key` participated in stack validation while transport performed authentication.
The proposed constructor derives identity from its endpoint:

```go
// PROPOSED: signatures, not implemented declarations.
type Config struct {
	Broadcast broadcast.EngineConfig
}

type Stack struct {
	endpoint  *transport.SharedTransport
	engine    *broadcast.Engine
	protocols protocol.Registry

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	started bool
	closing bool
	nextID  ConnID
	peers   map[transport.PeerID]*peerState
	wg      sync.WaitGroup
	// Shared Close completion and result omitted.
}

func NewStack(
	endpoint *transport.SharedTransport,
	cfg Config,
) (*Stack, error)

func (s *Stack) Start(ctx context.Context) error
func (s *Stack) Connect(
	ctx context.Context,
	peer transport.PeerID,
	addr net.Addr,
) error
func (s *Stack) Connections() []ConnInfo
func (s *Stack) Disconnect(peer transport.PeerID) error
func (s *Stack) Close() error
```

Using the concrete endpoint is the smallest production API.
The project is QUIC-native, so this change does not need a general transport plugin system.
A small consumer-owned interface remains an option
if the simulation's existing QUIC endpoint needs it.
There are two real callers to compare before selecting that boundary.

Construction validates configuration and builds the engine and registry.
It returns an error without leaving partially owned resources behind.
Ownership of the endpoint transfers only on successful construction.

`Start` begins acceptance after the application attaches and subscribes its channels.
It freezes protocol registration for that stack lifetime.
It must reject a second start and any start after shutdown begins.

The endpoint currently starts listening lazily on attachment.
The final `Start` implementation needs either synchronous listener preparation
or an explicit way to report a later fatal accept failure.
Returning success and silently losing the only accept loop is not an acceptable contract.

`Connect` succeeds when the authenticated ethp2p connection has been admitted
and routing has started.
This is distinct from broadcast handshake completion or readiness of every registered protocol.

The caller's context bounds the connection attempt and its wait for admission.
After success, `Stack` owns the connection lifetime.
A caller that releases its request context must not accidentally disconnect a successfully
established peer.

`Connections` returns snapshots, not mutable records or live stream handles.
`Disconnect` prevents an in-flight dial for that peer from publishing a replacement
after the method has completed.

## Connection identity and peer identity

A peer ID identifies an authenticated remote node.
A connection ID identifies one local connection instance.
Reconnection and simultaneous dials make those identities different even
if the initial policy prefers one active ethp2p connection per peer.

```go
// PROPOSED: fields that establish ownership.
type ConnID uint64

type ConnInfo struct {
	ID        ConnID
	Peer      transport.PeerID
	Active    bool
	Since     time.Time
}

type managedConn struct {
	id     ConnID
	conn   transport.Conn
	routes *protocol.Routes
	cancel context.CancelFunc
	done   chan struct{}
}

type peerState struct {
	conns   map[ConnID]*managedConn
	active  *managedConn
	dialing *dialAttempt
}

type dialAttempt struct {
	done chan struct{}
	err  error
}
```

One peer table inside `Stack` is sufficient initially.
Each peer record contains its candidate, active, and draining connections.
A separate public connection-manager object would add a layer without a second owner or caller
that needs it.

The connection ID is local and monotonically allocated under the stack lock.
It is not a wire identifier and must not be used to compare candidates at opposite endpoints.

An old connection's cleanup removes only that connection:

```go
// PROPOSED: while holding the stack table lock.
delete(peer.conns, finished.id)
if peer.active == finished {
	peer.active = nil
}
```

Broadcast needs the same identity check.
Its current peer-gone event is keyed only by peer ID.
The proposed internal event carries the binding that ended:

```go
// CURRENT
e.NotifyPeerGone(peerID)

// PROPOSED: event contains the specific binding.
e.peerStopped(binding)

// In the engine event loop:
if e.peers[binding.id] != binding {
	return
}
delete(e.peers, binding.id)
```

The broadcast peer table remains useful for subscriptions and protocol state.
It is not a second authoritative table of physical connections.

## One admission path for accepted and dialed connections

Both entry points should call an internal admission function:

```go
// PROPOSED: conceptual flow, with error handling omitted.

// In the accept loop:
conn, err := s.endpoint.Ethp2p().Accept(s.ctx)
s.admit(conn)

// In the dial worker:
conn, err := s.endpoint.Ethp2p().Dial(dialCtx, addr, peer)
s.admit(conn)
```

Admission validates the authenticated identity, applies connection limits,
assigns a local connection ID, and resolves the candidate's role.
It then registers the connection before starting its managed runtime.

Rejection releases the ethp2p view.
A bind failure also releases the view and rolls back its table entry.
Neither path may abandon a connection that the endpoint has already delivered.

The table lock protects short state transitions.
Network operations, protocol binding, callbacks, connection closure,
and waits happen outside the lock.
Otherwise one stalled peer could block admission and shutdown for unrelated peers.

All accepted work must join the stack wait group before shutdown can begin waiting.
Dial attempts count as work too.
Tracking only successful `ServeConn` calls leaves a race where an in-flight dial publishes
after the stack has supposedly closed.

The existing `ServeConn` body becomes the internal `runConn` implementation.
Production callers no longer own a second accept loop or launch a goroutine
for each connection themselves.

## Runtime termination and endpoint shutdown

Per-connection shutdown has a dependency order:

```go
// PROPOSED: conceptual runConn cleanup.
cancel()
conn.Close()
routingWG.Wait()
routes.Close()
s.remove(mc)
close(mc.done)
```

The close operation aborts the view's I/O before any join waits for that I/O to finish.
Runtime errors and context cancellation must enter this cleanup path directly.

There is a second contract to settle in `protocol.ConnectionHandlers`.
Its current `Done` channel can be interpreted as either a request to stop the connection or evidence
that all subsystem goroutines have stopped.
Those meanings produce different wait dependencies.

The implementation should distinguish termination notification from joining the binding.
For example, a binding context can signal failure immediately,
while `Close` performs an idempotent stop and join after transport I/O has been aborted.
The exact API can stay small, but `Stack` must not wait for an already blocked subsystem to announce
that all its readers have exited.

At stack shutdown, admission closes first.
The stack cancels its accept loop, pending dials, and connection runtimes.
It closes the shared endpoint under the retained ownership contract,
then joins its work and closes the broadcast engine.
Concurrent `Close` callers wait for the same completion and receive the same result.

The endpoint close is an application shutdown action.
It must not be the mechanism required to make ordinary per-peer cancellation work.

## Dial coordination and duplicate connections

Concurrent `Connect` calls for one peer should share a dial attempt.
An already suitable active connection satisfies a new request without another handshake.

The shared attempt should have its own bounded lifetime.
Cancelling one waiting caller should not fail every other waiter.
Stack shutdown and explicit disconnect can cancel the attempt for everyone.

For simultaneous opposite-direction dials, both endpoints need the same selection rule.
Preferring the connection initiated by the lower canonical peer ID is a candidate rule.
Always keeping the first local arrival can make the two endpoints keep different physical
connections.

That direction rule does not resolve every duplicate case.
Multiple independent dialers can produce connections with the same direction.
Local `ConnID` values cannot break that tie consistently across endpoints.
The implementation must either prevent those duplicate attempts at a shared owner
or define a connection identifier that both endpoints can compare.
This remains open until the outbound libp2p policy is selected.

The connection table should retain visibility of every managed candidate
and draining connection even when the active-binding policy selects only one.
A duplicate must not escape accounting because it is not the primary.

## The libp2p boundary limits what Stack can promise

`Libp2pTransport.Dial` uses the libp2p caller's TLS configuration unchanged,
negotiates only `libp2p`, and never produces an ethp2p view.
An inbound connection that negotiated only `libp2p` also stays libp2p-only,
because classification happens once, at handshake time.

The first version of `Stack.Connect` should initiate ethp2p-aware dials
and manage the resulting ethp2p views.
A legacy result remains legacy-only.
`ErrDialLegacyPeer` must not create an active ethp2p table entry
or trigger an immediate unbounded retry loop.
The transport can still deliver the libp2p view according to its existing contract.

Guaranteeing one physical connection per peer across both stacks requires more work than a stack
peer table.
Both dial entry points need common reuse and selection rules.
An existing connection that negotiated only `libp2p` cannot silently change its negotiated protocol.

Libp2p-initiated dials are libp2p-only.
One shared physical connection needs ethp2p-first dialing or a shared dial owner.
A shared dial owner must preserve libp2p's certificate verification.
It must also preserve the expected peer identity and identity callback behavior.
The libp2p TLS callback must provide the remote key before `Dial` returns.

I would keep this broader coordination as a separate implementation unit.
That choice limits the first manager to a contract it can fulfill without replacing libp2p's peering
machinery.

## Alternatives and the reasons to defer them

| Alternative | Benefit | Reason to defer or reject |
| --- | --- | --- |
| Close physical QUIC whenever either view closes | Small close implementation | A routine ethp2p failure would disconnect healthy libp2p traffic |
| Raise the stream limit and stop there | Fixes the observed eight-message workload | Sustained long-lived streams can exhaust the larger limit too |
| Keep all streams in a per-view map forever | Makes close find every historical stream | Memory grows with total traffic rather than concurrent activity |
| Add `map[PeerID]Conn` to the current stack | Provides a first peer lookup | Cannot safely represent replacement, draining, and duplicate connections |
| Add a public connection-manager package immediately | Offers an independently named component | Current ownership and callers fit inside `Stack` |
| Replace all control with a new global event loop | Centralizes state changes | Introduces another scheduling boundary before existing mutex-based ownership has failed |
| Make the stack transport-agnostic | Simplifies some test substitutions | The actual design is QUIC-native; use only the interface needed by real adapters |
| Add discovery and retries with the first table | Approaches the long-term peering design sooner | Multiplies dial and shutdown paths before their lifecycle is reliable |

These choices preserve future changes without requiring them now.
An additional owner, a measured contention problem,
or a concrete connection reuse requirement would justify revisiting the corresponding boundary.

## Compatibility and caller migration

The proposed view lifetimes, table, and constructor do not require a new wire format.
A session-budget negotiation extension would require its own compatibility decision
if the implementation needs one.

The public Go API does change.
Production callers move from a `Stack` literal and `Init` to `NewStack` and `Start`.
They move external accept and dial orchestration into `Stack`.

The simulation must remain an explicit caller in that migration.
Its topology-derived identities, QUIC configuration,
and cumulative bandwidth measurements must survive.
If closed connections leave the active table,
their final byte counters need to move into cumulative totals rather than disappear from reported
bandwidth.
Shared QUIC counters describe physical traffic from both views, not ethp2p-only bytes.

An internal adoption method or a narrow endpoint adapter can preserve simulation injection without
retaining two public lifecycle models.
Selecting that adapter is part of the constructor implementation unit.

The documentation migration includes the
[shared transport contract](../transport/doc.go),
[broadcast framing specification](002-ec-broadcast.md), and simulation examples.
The final descriptions must agree about endpoint startup, selector ownership, view closure,
identity, and socket ownership.

## Implementation sequence and the proof for each unit

| Unit | Result | Evidence required before the next unit |
| --- | --- | --- |
| Reproducible baseline | Resolvable patched dependency and compiling integration callers | A clean checkout builds and the integration test package compiles |
| Local correctness fixes (implemented) | Correct overflow disposal, bounded selector reads, unified selector frames, and selector-writing `Peer` methods | Queue overflow preserves the returned view; pre-handshake SESS and CHUNK streams wait or cancel correctly; incomplete selectors recover and respect cancellation |
| View lifetimes | Closing a view aborts its work and releases stream tracking | Opposite view remains usable; blocked opens, accepts, reads, and writes terminate; completed-stream bookkeeping stays bounded |
| Outbound broadcast stream admission | Long-lived SESS streams cannot prevent CHUNK progress | Concurrent and sustained broadcasts complete under small budgets, delayed reads, and cancellation |
| Managed connection runtime | Stack identifies, owns, and removes each connection | Admission rollback, context cancellation, and stale cleanup leave no orphaned records or readers |
| Construction and acceptance | Stack owns normal inbound connection delivery | Start and close races terminate; channel setup precedes acceptance; fatal accept errors are observable |
| Dial coordination and inspection | Connect, snapshots, and disconnect share the admission path | Concurrent dials coalesce; disconnect defeats late publication; opposite-direction duplicates converge |
| Caller and document migration | Simulation and application use the selected lifecycle | Simulation behavior and cumulative statistics remain correct; obsolete lifecycle callers are removed |
| Broader peering policy | Desired peers, backoff, or shared libp2p dial coordination | Separate evidence for the selected policy and its interaction with shutdown |

The units need not become separate public abstractions.
Their purpose is to make each behavioral change independently reviewable and verifiable.
The small fixes can land before the full manager exists.

Repository validation includes these commands,
subject to the dependency and integration compilation blockers described above:

```sh
go test -mod=readonly -race -count=1 ./...
go test -mod=readonly -race -count=1 -tags=integration ./broadcast/tests
```

The existing suite alone is insufficient.
The runtime reproductions and the admission, replacement,
and shutdown scenarios in the table need repository tests
that exercise actual shared QUIC connections.

## Decisions still open

The proposal establishes ownership and delivery order.
These details still need code-level evidence before implementation can be called complete:

1. How stream admission observes remote credit and reserves data progress.
2. How per-view bookkeeping observes complete stream termination through
   the current concrete libp2p stream interface.
3. How protocol bindings distinguish failure notification from completed cleanup.
4. How `Start` reports listener preparation and later fatal accept failures.
5. Which endpoint adapter preserves simulation injection with the fewest methods.
6. Who closes the supplied packet socket, consistently across callers.
7. Whether libp2p and ethp2p share an outbound dial owner, and how
   same-direction duplicates are resolved if they do not.

The next useful implementation work is the view-lifetime contract and its tests.
A peer table built before that contract would make blocked connections visible without making them
manageable.
