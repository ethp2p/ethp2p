# The ethp2p stack: routing, delivery, and connections

Status: units 1–5 implemented; datagram delivery remains deferred.
Supersedes the connection-ownership proposal in [006](006-stack-connection-management.md)
and the selector advertisement stream.

## 0. Introduction

The shared QUIC transport has landed.
It authenticates peers, shares one endpoint and one set of connections with libp2p,
gives each stack an independent view of a connection,
and runs one dispatcher per physical connection.
The original layers above it were built while the transport was deliberately minimal.
Applications owned accept loops and connection lists, the stack ran routing goroutines,
and subsystems received peers and streams on separate unordered channels.
The implementation described here moves connection pumps into transport
and gives protocols ordered delivery queues.

This document redesigns the layers above QUIC around four principles.

1. **ethp2p is a library with minimal runtime and state.**
   The stack is a meeting point between connections and protocols.
   It owns registration, a connection table, and delivery queues bounded by QUIC credit.
   It runs no goroutines of its own.
2. **One runtime per connection, where one already exists.**
   The transport's per-connection dispatcher already reads the first bytes of every stream.
   It now reads the whole selector, runs the stack's control stream, and receives datagrams.
   Nothing above it needs a goroutine per connection.
3. **Protocols pull.**
   The stack queues events for each protocol and signals a wake channel the protocol supplies.
   The protocol drains the events in whatever loop shape suits it.
4. **Outcomes are explicit.**
   Every stream reset and view closure carries a code in a defined namespace,
   so a peer can tell "unsupported", "overloaded", and "done" apart.

## 1. About this document

Sections 3 through 7 specify wire behavior and use RFC 2119 language such as MUST, SHOULD, and MAY.
Sections 8 through 10 specify the Go API of this implementation.
They are normative for this repository but do not affect interoperability.
Section 11 describes the migration of existing code, and section 12 the implementation sequence.

## 2. Terminology

**Endpoint.**
The shared QUIC endpoint (`transport.SharedTransport`) that owns the UDP socket's QUIC state,
the listener, and every physical connection.

**View.**
One stack's use of a physical connection.
A physical connection has at most one ethp2p view and at most one libp2p view.
It closes when both views are released.

**Stack.**
The ethp2p component that owns protocol registration, the connection table,
and delivery queues (`ethp2p.Stack`).

**Protocol, subsystem.**
A registered consumer of one or more selectors, such as broadcast.

**Selector.**
The unsigned integer identifying a protocol's streams and datagrams on the wire.
Selector `0` identifies the stack's control stream.

**Selector frame.**
An unsigned-varint length `n` between 1 and 10,
followed by the selector's minimal unsigned-varint encoding, which occupies exactly `n` bytes.

**Shared selectors.**
The intersection of the selectors both endpoints announced on a connection.

**Peer.**
A protocol's handle for one admitted connection.
The same physical connection yields one peer handle per protocol that accepted it.

## 3. Stream routing

### 3.1. Stream heads

Every ethp2p stream, unidirectional or bidirectional, MUST begin with exactly one selector frame.
The selected protocol owns every byte after the frame.

### 3.2. Classification on shared connections

libp2p opens only bidirectional streams,
and every libp2p stream begins with a multistream-select message whose length prefix is at least 19
(`/multistream/1.0.0\n`).
A selector frame's length prefix is between 1 and 10.
The first byte of a stream therefore identifies its owner.

For each incoming bidirectional stream, the dispatcher MUST:

1. Read the first byte.
   If it is between `0x01` and `0x0a`, the stream is ethp2p; continue at step 3.
2. Otherwise, if the second byte is `/`, hand the stream to libp2p with both bytes unconsumed.
   Otherwise reset the stream with stack code `BadSelector`.
3. Read the remaining `n` bytes of the selector frame and validate it.
   Reset the stream with `BadSelector` if the encoding does not occupy exactly `n` bytes.

Incoming unidirectional streams are always ethp2p and start at step 3 with the first byte as `n`.

All of steps 1 to 3 MUST complete under a single deadline per stream
(5 seconds in this implementation).
A stream that misses it is reset with `Timeout`.
The dispatcher MUST bound the number of streams it classifies concurrently per connection
and per direction, so that slow streams in one direction cannot delay the other,
and a slow stream cannot delay faster streams beyond that bound.

The dispatcher consumes a bidirectional stream's selector frame before handing it to the stack.
It only peeks at a non-control unidirectional stream's frame;
the delivered receive adapter skips that frame on its first read.
A unidirectional stream carrying only its selector therefore holds stream credit
until the protocol reads it.
The control stream consumes its frame before entering the control reader.
The ethp2p connection view writes the selector frame when it opens a stream.
It validates the frame length and the selector's minimal encoding with the same codec used by the
stack.

Classified ethp2p streams wait in a per-connection queue, in completion order,
until the stack takes them, including streams that arrive before `Hello`.
The control stream bypasses that queue.
Each queued stream holds QUIC stream credit until it completes,
so incoming stream limits bound the queue and push back on the peer (section 9.3).
The dispatcher never resets an ethp2p stream because a queue is full.
A classifier releases its slot as soon as it enqueues the stream.

Because classification depends on the length byte rather than the first payload byte,
selector `0x2f` is no longer reserved.

### 3.3. Routing

After classification, the dispatcher routes the stream by selector:

- Selector `0` on a unidirectional stream is the peer's control stream (section 4).
  A second one, or selector `0` on a bidirectional stream, is a control violation (section 4.5).
- A selector that is not shared is reset with `UnsupportedSelector`.
- A stream that arrives before the peer's `Hello` waits and is routed when the `Hello` is processed.
  Like every queued stream, it is bounded by stream credit (section 9.3).
- Otherwise, the stream is delivered to the protocol that registered the selector (section 9).

## 4. The control stream

Each endpoint opens exactly one unidirectional stream with selector `0` on every ethp2p view,
as soon as the view exists, and keeps it open for the lifetime of the view.

Using one unidirectional stream per direction, rather than one bidirectional stream, is deliberate.
A bidirectional control stream needs an opener,
and when both endpoints open at once they need a rule to discard one stream.
With one outbound and one inbound stream per endpoint, simultaneous open is the normal case,
and there is nothing to resolve.
Broadcast's BCAST streams follow the same pattern for the same reason.

### 4.1. Messages

The control stream carries a sequence of frames.
Each frame is an unsigned-varint length followed by a protobuf-encoded `Control` message.
Frames MUST NOT exceed 16 KiB.

```protobuf
message Control {
  oneof message {
    Hello  hello   = 1;
    GoAway go_away = 2;
  }
}

message Hello {
  // Selectors this endpoint serves, strictly ascending, at most 1024, none zero.
  repeated uint64 selectors = 1;
  // Optional: the sender's current node record (EIP-778 RLP encoding).
  bytes record = 2;
}

message GoAway {
  // A stack-namespace code value (section 5.2).
  uint64 code = 1;
}
```

A receiver MUST ignore a `Control` message whose `message` field is unset or unknown,
so that later revisions can add messages.

### 4.2. Hello

`Hello` MUST be the first frame on the control stream, and each endpoint MUST send it exactly once.
A receiver that does not receive a valid `Hello` within 5 seconds of the view's creation MUST close
the view with `ControlViolation`.

A `Hello` is invalid if its selectors are not strictly ascending, exceed 1024, or include `0`,
or if `record` is present and fails verification,
or if the record's identity differs from the identity authenticated by the TLS handshake.

The transport validates selectors on both send and receive.

The stack validates each non-empty record.
It closes the view with `ControlViolation` when verification or identity matching fails.

An invalid `Hello` is a control violation.

After both `Hello` messages have been exchanged,
the shared selectors are fixed for the lifetime of the view.

### 4.3. Closing a view

To close its view of a connection, an endpoint SHOULD send `GoAway` with a reason code,
then send FIN on its control stream, then release its view.
An endpoint that receives `GoAway` or FIN on the peer's control stream MUST treat the peer's view
as closed: it stops delivering new streams and datagrams for the connection to protocols,
ends every peer handle for the connection, and releases its own view.

A stack-namespace reset of the control stream closes the peer view with the reset's code.

A protocol-namespace reset is a control violation.

FIN without a preceding `GoAway` closes the peer view with `Unspecified`.

The endpoint answers a peer's closure with FIN on its own control stream, without `GoAway`.

`GoAway` is best-effort when releasing this view also closes the physical connection:
the connection close may overtake it, leaving the peer with a connection error.

This signal is the only way a peer learns that an ethp2p view closed
while libp2p keeps the physical connection open.

### 4.4. Sending order

An endpoint SHOULD open its control stream and send `Hello`
before opening any other stream on the view.
The receiver does not depend on stream order
(section 3.3), but early streams consume the receiver's bounded pre-`Hello` queue.

### 4.5. Control violations

On a control violation the endpoint sends `GoAway(ControlViolation)` and FIN on its control stream.
It cancels reading the peer's control stream with `ControlViolation` and releases its view.

Resetting its own stream could let the peer discard the `GoAway` frame.

### 4.6. Stateless resets

An endpoint SHOULD configure a QUIC stateless reset key derived deterministically from its identity
key, for example with HKDF-SHA256 over the private key and the label `ethp2p stateless reset`.
A restarted endpoint then answers packets on its previous connections with stateless resets,
and its peers close those connections within one round trip.
That shortens the window in which a stale connection wins a duplicate check (section 10.4).

## 5. Outcome codes

Stream resets (`RESET_STREAM`, `STOP_SENDING`) and `GoAway` carry codes.

### 5.1. Wire layout

A QUIC application error code is a varint. ethp2p splits it into a value and a one-bit namespace:

```text
code = value << 1 | namespace        namespace 0: stack, 1: protocol
```

A protocol-namespace code is interpreted by the selector of the stream it resets.
The selector is not encoded, because both endpoints already know it.

Code `0` is the stack's `Unspecified`,
which is also what a QUIC implementation sends when an application gives no code.

Values below 32 encode in one byte in either namespace, and values below 8192 in two.

### 5.2. Stack codes

| Value | Name | Sent by | Meaning |
| ----: | ---- | ------- | ------- |
| 0 | `Unspecified` | anyone | No specific reason, including ordinary cancellation. |
| 1 | `Refused` | anyone | The receiver will not process this stream. |
| 2 | `Overloaded` | anyone | A bounded queue or budget is full. Retrying later may succeed. |
| 3 | `Timeout` | anyone | A deadline expired. |
| 8 | `BadSelector` | stack | The stream head was not a valid selector frame. |
| 9 | `UnsupportedSelector` | stack | The selector is not shared on this connection. |
| 10 | `Closing` | stack | The view is closing. |
| 11 | `ControlViolation` | stack | The control stream protocol was violated. |
| 12 | `NoSharedProtocols` | stack | No local protocol accepted the peer. |
| 13 | `Duplicate` | stack | Another connection to the same peer was kept. |

Values 0 to 3 are shared: either endpoint MAY send them on any stream,
and protocols SHOULD use them instead of defining their own equivalents,
so that senders can handle overload and refusal uniformly.
Values 4 to 7 are unassigned and decode as `Unspecified`.
Values 8 to 31 are sent only by the stack.
Values 14 to 31 are reserved for future stack codes, keeping every stack code to one byte.

A receiver MUST treat an unknown stack value as `Unspecified`.
When a reset carries a defined stack-only value,
`ResetError.Code` preserves it for the receiving protocol.
If a protocol passes that code to a stream cancellation method, the wrapper sends `Unspecified`;
only stack-owned paths send stack-only values.
A protocol-namespace code on the control stream is a control violation.

### 5.3. Protocol codes

Each protocol defines its code values per selector in its own specification.
Value `0` in the protocol namespace means "no protocol-specific reason".
The stack does not keep per-selector code tables:
it preserves every received protocol value and binds it to the stream's selector.
The receiving protocol applies its own fallback for values it does not define.

### 5.4. Keeping codes small

Protocol specifications SHOULD follow these rules:

- Give the most frequent outcomes the lowest values, so they encode in one byte.
- Keep each selector's code set below 32 values.
  Do not use sparse, bit-field, or category-range layouts.
- Never give value `0` a meaning.
- Use codes for why a stream ended, not for data.
  Send a frame before FIN when the peer needs a count, an identifier, or a position.
- Publish each selector's code table, add values only at the end,
  and never renumber or reuse a value.
- Use the shared stack codes for refusal, overload, and timeouts.

## 6. Datagrams

Datagram routing follows [007](007-datagrams.md), with two refinements.
The shared selectors come from the `Hello` exchange of section 4.2.
Selector `0x2f` is an ordinary selector for datagrams as it is for streams.

## 7. Connection admission

A view is admitted when both `Hello` messages have been exchanged.
Admission validates the Hello record and evaluates protocol policies before taking the table mutex.
Policies may inspect Connections and must return promptly.
An invalid record is rejected with `ControlViolation`; if no protocol accepts,
the view is rejected with `NoSharedProtocols`.
Thus a view that matches no protocol never displaces a working view.

Under the mutex, admission checks that the stack is open and
that an outbound dial has not been abandoned by Disconnect
(section 10.5), rejecting with `Closing` otherwise.
It then applies the duplicate rule (section 10.4), closing the loser with `Duplicate`.
When replacing a view, its peer contexts are cancelled
and its `PeerDown(Duplicate)` events are queued before the winner's `PeerUp` events.

Only then do protocols learn about the peer.
A view that loses the duplicate rule or matches no protocol never produces a peer handle.

## 8. Layering and runtime

```text
quic-go                       packet I/O, QUIC state machines
transport.SharedTransport     endpoint, accept loop, per-connection dispatcher:
                                stream classification and selector reads,
                                control stream, datagram receipt
ethp2p.Stack                  registration, connection table, delivery queues
                                (no goroutines)
protocols (broadcast, ...)    their own state and loops, fed by Next
application                   Connect, Disconnect, shutdown order
```

The dispatcher delivers into the stack through this interface bound to the ethp2p endpoint:

```go
// In package transport. Methods must never block on protocols. They may close
// other views, which waits at most the GoAway timeout.
type Sink interface {
	Admit(conn Conn, hello Hello) (code protocol.Code, ok bool)
	Stream(conn Conn, sel protocol.Selector, s ReceiveStream) // takes ownership
	Closed(conn Conn, code protocol.Code)
}
```

The transport owns every connection pump and stream-loop goroutine.
Accepted views wait for Hello and call Admit in the pump.
Dialed views wait for Hello and call Admit inside Dial, in its caller's goroutine.
Dial returns only after admission, starting the transport-owned stream loops on success.
Rejection closes the view with the returned code, with no Stream or Closed calls.
Closed is called exactly once for an admitted view, after both stream loops finish.
Its code is the ViewClosedError code, otherwise Closing for transport shutdown,
Timeout for a timeout error, or Unspecified.

The root stack starts no persistent goroutines.
Its close fan-out goroutines are joined before Close returns.
Connect waits for a dial or a shared attempt; Disconnect and Close wait for release.
Admission may close an evicted view after unlocking, in the transport's caller.

## 9. Delivery to protocols

### 9.1. Registration

```go
sub, err := stack.Register("broadcast", []protocol.Selector{BCAST, SESS, CHUNK}, ethp2p.SubsystemConfig{
	Policy: func(peer *ethp2p.Peer) bool { ... }, // optional; nil accepts any intersection
})
sub.Notify(wake) // chan struct{} with capacity >= 1, supplied by the protocol
```

Registration and configuration MUST finish before `Start`.

### 9.2. Events

```go
type Event struct {
	Kind     EventKind
	Peer     *Peer
	Selector protocol.Selector // StreamIn (also DatagramIn in unit 6)
	Stream   ReceiveStream     // StreamIn; bidi streams also implement Stream
	Code     protocol.Code     // PeerDown
	// Payload []byte          // DatagramIn, added in unit 6
}

for ev, ok := sub.Next(); ok; ev, ok = sub.Next() {
	switch ev.Kind {
	case ethp2p.PeerUp:     // ev.Peer
	case ethp2p.StreamIn:   // ev.Peer, ev.Selector, ev.Stream (bidirectional streams implement ethp2p.Stream)
	case ethp2p.DatagramIn: // ev.Peer, ev.Selector, ev.Payload
	case ethp2p.PeerDown:   // ev.Peer, ev.Code
	}
}
```

`Next` never blocks.
After it returns `ok == false`, the protocol waits on its wake channel and drains again.
The stack sends on the wake channel without blocking after every enqueue,
so a wake is never lost as long as the protocol drains until `Next` reports empty.
Protocols MAY share one wake channel across several subsystems to run them in one loop.

The stack guarantees:

- `PeerUp` precedes every other event for that peer.
- `PeerDown` is the last event for a peer.
  Streams still queued for the peer when its view closes are reset by the stack with `Closing`
  and never delivered.
- A peer whose view closes before its `PeerUp` was returned produces no events.
- A peer's streams are returned in the order the dispatcher finished classifying them.
- `Next` serves peers round-robin, one event at a time,
  so a peer with a large backlog cannot delay other peers' events.

Ownership of a delivered stream passes to the protocol when `Next` returns it.
The peer's context ends before `PeerDown` is queued.

### 9.3. Bounds

QUIC supplies stream backpressure; the stack does not count streams:

- **Queued streams.**
  Every queued stream holds QUIC stream credit until the protocol reads it to completion
  or cancels it.
  This includes a stream ending with its selector, because classification only peeks at it
  (section 3.2).
  Each connection's incoming stream limits therefore bound its queued streams,
  and QUIC flow control bounds their buffered bytes.
  When a protocol falls behind, the peer's stream opens block instead of streams being dropped.
  The stack never resets a stream because a queue is full:
  a unidirectional sender may already have closed successfully, making that loss silent.
- **Shared credit.**
  Protocols share each connection's stream credit.
  One that stops draining eventually blocks the others on that connection.
  Protocols shed load by taking a stream and cancelling it with `Overloaded`.
- **Datagrams per peer and selector.**
  A ring buffer that discards the oldest datagram, as 007 requires.
- **Peer state.**
  Memory is proportional to live connections plus `PeerDown` events not yet drained.

The datagram ring's default is an implementation constant.

### 9.4. The peer handle

```go
type Peer struct{ /* unexported */ }

func (p *Peer) ID() transport.PeerID
func (p *Peer) Record() *enr.Record              // latest known record, or nil
func (p *Peer) Selectors() []protocol.Selector   // this protocol's shared selectors
func (p *Peer) Context() context.Context          // ends when the peer goes down
func (p *Peer) OpenUniStream(ctx context.Context, sel protocol.Selector) (ethp2p.SendStream, error)
func (p *Peer) OpenStream(ctx context.Context, sel protocol.Selector) (ethp2p.Stream, error)
func (p *Peer) SendDatagram(sel protocol.Selector, payload []byte) error
func (p *Peer) MaxDatagramPayload(sel protocol.Selector) int
```

The open and send methods refuse selectors outside `Selectors()`,
and write the selector frame themselves, so protocols never write selector frames.
Protocols never receive the raw `transport.Conn`.

### 9.5. Streams and codes

`protocol` owns the code vocabulary.
`protocol.Code` is comparable, and its zero value is `protocol.Unspecified`.
A protocol creates selector-bound values with `Selector.Code`:

```go
package protocol

type Code struct{ /* selector Selector; value uint64; protocol bool */ }
func (s Selector) Code(value uint16) Code // panics for selector 0

func (c Code) WireFor(sel Selector) uint64
func ParseCode(sel Selector, wire uint64) Code
```

`WireFor` encodes `value<<1 | namespace`.
It panics when a protocol code is sent on a different selector,
and maps stack-only values to `Unspecified`.
`ParseCode` preserves protocol-namespace values for the stream selector
and maps unknown stack values to `Unspecified`.
The shared codes `Unspecified`, `Refused`, `Overloaded`, and `Timeout` are valid on any selector.
Stack-only codes are exported from `protocol` so protocols can inspect them after a reset;
only stack-owned send paths encode their wire values.

The root `ethp2p` package owns the stream interfaces and wrappers:

```go
package ethp2p

type SendStream interface {
	CancelWrite(protocol.Code)
}
type ReceiveStream interface {
	CancelRead(protocol.Code)
}
type Stream interface {
	CancelRead(protocol.Code)
	CancelWrite(protocol.Code)
}
type ResetError struct {
	Code protocol.Code
}
func (e Event) Cancel(protocol.Code) // StreamIn only; other kinds are no-ops
func (e Event) Reject() // deliberate refusal; sends Refused for StreamIn
```

The wrappers are unexported and retain the stream selector locally.
Their cancellation methods call `Code.WireFor` with that selector.
They expose no numeric code API.
`Stream` has no `Reset` method: callers cancel each side with its outcome.
Reads that end in a reset return `*ethp2p.ResetError`, whose `Unwrap` returns the transport error.

Cancelling a side is idempotent: the first code is the one sent, and later calls have no effect.
A failed `Read`, `Write`, or `Close` cancels its side before returning the error,
with `Timeout` if a deadline expired and `Unspecified` otherwise.
Callers may therefore cancel with their own code after any error.
The call takes effect only when no I/O failed, for example on a malformed frame,
so protocols need not tell stream errors apart from decode errors.
A peer reset ends the read side; `Read` returns it as `ResetError`, and nothing is sent in reply.
Deadline expiry is terminal, unlike Go's usual deadlines, which can be extended to resume I/O.
A protocol that stops disposes of the events it has not handled with
`Event.Cancel(protocol.Unspecified)`; `Event.Reject` is reserved for deliberate refusals.

Broadcast keeps its protocol-specific codes beside its selectors:

```go
var (
	Reconstructed = SESS.Code(1)
	Redundant = CHUNK.Code(1)
)

stream.CancelWrite(broadcast.Reconstructed)
stream.CancelRead(protocol.Overloaded)
```

This gives protocols typed codes without generic types in the stack's API.
A protocol that wants compile-time checks can wrap its own streams in local types.

## 10. Connection management

### 10.1. Lifetime

```go
stack, err := ethp2p.NewStack(shared.Ethp2p(), ethp2p.Config{})
// Register protocols and call Notify here.
err = stack.Start()    // installs the sink and starts listening; reports listener errors synchronously
...
err = stack.Close()    // sends GoAway(Closing), releases every view, resets queued streams
```

`Start` fails if called twice or after `Close`.
`Close` is idempotent and returns once every view the stack admitted has been released.
The stack never closes the endpoint or the UDP socket.

The application's shutdown order is: `Stack.Close`, then the libp2p host,
then `SharedTransport.Close`, then the socket.

### 10.2. Connecting by node record

```go
err := stack.Connect(ctx, rec)   // rec *enr.Record
```

The record supplies the peer's identity and its QUIC endpoints
(`ip`/`quic`, then `ip6`/`quic6`, the keys libp2p nodes already publish).
ethp2p introduces no record key of its own: dialing offers the ethp2p ALPN first,
and a peer without ethp2p negotiates libp2p and is handed to libp2p.

`Connect`:

- returns `nil` at once if an admitted connection to the record's peer exists;
- otherwise dials each QUIC endpoint in order, requiring the record's identity, until one succeeds;
- returns `nil` once the view is admitted
  (section 7),
  or the error that prevented admission: a dial error, `transport.ErrDialLegacyPeer`,
  or an admission rejection such as `NoSharedProtocols`.

Connect returns nil when the peer has an active view at the end of its attempt,
including a view that beat its own under the duplicate rule.
It never returns Duplicate when the peer ends up connected.
An attempt cancelled by Disconnect returns `ErrDisconnected` if no active view remains;
stack closure returns `ErrStackClosed`.

A legacy result never creates a table entry and never triggers a retry.

Concurrent `Connect` calls for the same peer share one dial attempt,
driven by the first caller's goroutine.
If the driving caller's context ends, the attempt is abandoned,
and one of the remaining waiters whose context is still live starts a new attempt.
No goroutine outlives its caller.

The stack keeps the highest-sequence record it has seen for each connected peer,
from `Connect` or from the peer's `Hello`.

### 10.3. Identity

`ConnID` is a local, monotonically allocated connection identifier.
It never appears on the wire.
Peer identity is the `PeerID` authenticated by the TLS handshake.

### 10.4. Duplicates

At most one ethp2p view per peer is admitted.
When a view is admitted while another is active for the same peer:

- if they were dialed in opposite directions,
  keep the one dialed by the peer with the lower `PeerID`, compared byte-wise;
- if they were dialed in the same direction, keep the newer one.

Both endpoints apply the same rule, so simultaneous dials converge on one physical connection,
and a restarted peer that re-dials replaces its stale connection.
The losing view is closed with `Duplicate`.

When the higher-ID peer restarts and re-dials a connection that the lower-ID peer dialed,
the stale connection wins until it closes.
Stateless resets (section 4.6) close it within one round trip; without them,
the QUIC idle timeout bounds the delay.

### 10.5. Disconnecting

```go
err := stack.Disconnect(peerID)
```

`Disconnect` abandons any dial attempt for the peer, sends `GoAway(Closing)` on the peer's view,
releases it, and returns once the view has been released.
A per-peer generation counter prevents an outbound dial
that completes afterwards from being admitted.
The gate applies after policy evaluation, before committing the outbound view.
Generation entries remain only while a view or dial record exists:
any stale admission still has its driver's dial record carrying the old generation.
Disconnecting an unknown peer is not an error.
The peer learns of the disconnection from `GoAway`,
even when libp2p keeps the physical connection open.

### 10.6. Inspection

```go
type ConnInfo struct {
	ID       ConnID
	Peer     transport.PeerID
	Outbound bool
	Since    time.Time
}

func (s *Stack) Connections() []ConnInfo
func (s *Stack) Traffic() (sent, received uint64)
```

`Connections` returns snapshots.
`Traffic` returns cumulative QUIC byte counts over every connection the stack admitted,
including closed ones.
On shared connections the counts include libp2p traffic.

### 10.7. Out of scope

Connection limits, discovery, retries, peer scoring,
and a shared dial owner for libp2p and ethp2p are not part of this design.
They can be layered on `Connect`, `Disconnect`, and admission policies later.

## 11. Migration

**Broadcast.** `Engine.Register(stack)` registers BCAST, SESS, and CHUNK before Start,
with a policy requiring all three selectors and a wake channel owned by the engine.
The engine drains `Next` from its event loop, routing events to `PeerConn` directly:
`PeerUp` creates a binding, `StreamIn` routes to an existing binding or is refused,
and `PeerDown` closes the binding.
The two notification channels, `Serve`,
and the logic for a stream that arrives before its peer disappear.
`PeerConn` keeps its own bounded per-peer queues for SESS and CHUNK streams that arrive
before its BCAST handshake completes,
because that ordering is broadcast's rule rather than the stack's.
Broadcast defines its code tables in spec 002, for example:

| Selector | Value | Name | Meaning |
| -------- | ----: | ---- | ------- |
| `SESS` | 1 | `Reconstructed` | The sender has reconstructed the message. |
| `CHUNK` | 1 | `Redundant` | The receiver no longer needs this chunk. |

Overload of broadcast's own per-peer limits uses the shared `Overloaded`.
In Go, `Reconstructed` and `Redundant` are declared in `broadcast/protocol.go`;
shared stack codes are referenced from `protocol` directly.

**Simulation and tests.**
They build records for their nodes with `enr.Sign`, call `Start`, `Connect`, and `Close`,
and read bandwidth from `Traffic`.
Their connection lists, accept loops, and serving goroutines disappear.

**Specifications.**
Spec 002 describes the BCAST rationale, the stream head, and the code tables.
Spec 006 points to this document for connection ownership.
Spec 007 drops the `0x2f` reservation.
`transport/doc.go` describes the dispatcher's new responsibilities.

**Wire compatibility.**
This design replaces the selector advertisement stream with the control stream
and changes stream reset codes. ethp2p has no deployed network,
so no compatibility period is planned.

## 12. Implementation sequence

| Unit | Result | Depends on |
| ---- | ------ | ---------- |
| Outcome codes | `protocol.Code`, the wire layout, stream wrappers, and broadcast's code tables | — |
| Dispatcher routing | First-byte classification, full selector reads under one deadline, bounded concurrency per direction, the pre-`Hello` queue | — |
| Control stream | `Hello` and `GoAway` replace the advertisement stream; stateless reset key | Dispatcher routing |
| Delivery | `Register`, `Notify`, `Next`, the peer handle, the bounds, and broadcast's migration | Outcome codes, control stream |
| Connection management | `NewStack`, `Start`, `Close`, the table, `Connect` by record, duplicates, `Disconnect`, inspection, and the caller migration | Delivery, the `enr` package |
| Datagram delivery | 007 on top of delivery | Delivery and a first consumer |

Each unit must leave the full test suite passing,
including the race detector and the integration and short simulation suites.

## 13. Open questions

**Dynamic registration.** `Hello` fixes the shared selectors for a view's lifetime.
A later `Update` message could announce selector changes if protocols need to attach after start.

**Priorities.**
The stack does not prioritize between protocols.
Stream and datagram priorities from spec 001 need their own design.
Protocols share each connection's stream credit
(section 9.3), so per-protocol fairness belongs to that design.

**Subsystem shutdown.**
A protocol cannot unregister while the stack runs.
If one needs to, `Subsystem.Close` would reset its queued streams and stop admitting it
for new views.

**Shared dial ownership.** libp2p still dials on its own.
Guaranteeing one physical connection per peer across both stacks needs a shared dial owner.
