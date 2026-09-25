# The ethp2p stack: routing, delivery, and connections

Status: units 1–5 implemented.
Units 6–8 (protocols, admission in the event loop, stream credit) specified here and pending;
datagram delivery remains deferred.
Supersedes the connection-ownership proposal in [006](006-stack-connection-management.md)
and the selector advertisement stream.

## 0. Introduction

The shared QUIC transport has landed.
It authenticates peers, shares one endpoint and one set of connections with libp2p,
gives each stack an independent view of a connection,
and runs one dispatcher per physical connection.
The original layers above it were built while the transport was deliberately minimal.
Applications owned accept loops and connection lists, the stack ran routing goroutines,
and protocols received peers and streams on separate unordered channels.
The implementation described here moves connection pumps into transport
and gives protocols ordered delivery queues.

This document redesigns the layers above QUIC around four principles.

1. **ethp2p is a library with minimal runtime and state.**
   The stack is a meeting point between connections and protocols.
   It owns registration, a connection table, and delivery queues bounded by stream credit.
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

**Protocol.**
A registered consumer of streams and datagrams, such as broadcast.
A protocol has a numeric identifier
(section 4.3),
a set of selectors with a stream allowance for each (section 9.3), and one table of outcome codes
(section 5.3).
A protocol is not a selector: it owns its selectors, and they are negotiated as one unit.

**Selector.**
The unsigned integer identifying one kind of a protocol's streams and datagrams on the wire.
Each selector belongs to exactly one protocol.
Selector `0` identifies the stack's control stream.

**Selector frame.**
An unsigned-varint length `n` between 1 and 10,
followed by the selector's minimal unsigned-varint encoding, which occupies exactly `n` bytes.

**Shared protocols.**
The protocols both endpoints announced on a connection with the same identifier
and the same selectors (section 4.3).
The shared selectors are the selectors of the shared protocols.

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
Each queued stream counts against its selector's allowance until the protocol finishes it,
so allowances bound the queue and push back on the peer (sections 4.4 and 9.3).
The dispatcher never resets an ethp2p stream because a queue is full.
A classifier releases its slot as soon as it enqueues the stream.

Because classification depends on the length byte rather than the first payload byte,
selector `0x2f` is no longer reserved.

### 3.3. Routing

After classification, the dispatcher routes the stream by selector:

- Selector `0` on a unidirectional stream is the peer's control stream (section 4).
  A second one, or selector `0` on a bidirectional stream, is a control violation (section 4.8).
- A selector that does not belong to a shared protocol is reset with `UnsupportedSelector`.
- A stream that exceeds its selector's allowance is a control violation (section 4.8).
- A stream that arrives before the peer's `Hello` waits and is routed when the `Hello` is processed.
  Until then it is bounded by QUIC stream credit only.
- Otherwise, the stream is delivered to the protocol that owns the selector (section 9).

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
    Hello         hello          = 1;
    GoAway        go_away        = 2;
    Credit        credit         = 3;
    ProtocolClose protocol_close = 4;
  }
}

message Hello {
  // Protocols this endpoint serves, strictly ascending by id.
  repeated Protocol protocols = 1;
  // Optional: the sender's current node record (EIP-778 RLP encoding).
  bytes record = 2;
}

message Protocol {
  // Registered protocol identifier (section 4.3), never zero.
  uint64 id = 1;
  // The protocol's selectors, strictly ascending by selector.
  repeated SelectorLimit selectors = 2;
}

message SelectorLimit {
  uint64 selector = 1;
  // Initial stream allowance for this selector (section 4.4), at least 1.
  uint64 max_streams = 2;
}

message GoAway {
  // A stack-namespace code value (section 5.2).
  uint64 code = 1;
}

message Credit {
  uint64 selector = 1;
  // New cumulative stream allowance for this selector (section 4.4).
  uint64 max_streams = 2;
}

message ProtocolClose {
  uint64 protocol = 1;
  // A code in the wire layout of section 5.1.
  uint64 code = 2;
}
```

A receiver MUST ignore a `Control` message whose `message` field is unset or unknown,
so that later revisions can add messages.

### 4.2. Hello

`Hello` MUST be the first frame on the control stream, and each endpoint MUST send it exactly once.
A receiver that does not receive a valid `Hello` within 5 seconds of the view's creation MUST close
the view with `ControlViolation`.

A `Hello` is invalid if:

- its protocols are not strictly ascending by id, or an id is `0`;
- a protocol has no selectors, its selectors are not strictly ascending,
  or a selector is `0`;
- a selector appears in more than one protocol, or the protocols list more than 1024 selectors;
- a `max_streams` is `0`;
- `record` is present and fails verification,
  or the record's identity differs from the identity authenticated by the TLS handshake.

The transport validates the protocol list on both send and receive.
The stack validates each non-empty record.
An invalid `Hello` is a control violation.

After both `Hello` messages have been exchanged,
the shared protocols are fixed for the lifetime of the view.

### 4.3. Protocols

A protocol is identified on the wire by a numeric identifier.
Identifiers are assigned in this table.
An incompatible revision of a protocol takes a new identifier; `Hello` carries no version.

| Id | Protocol | Specification |
| -: | -------- | ------------- |
| 0 | reserved | |
| 1 | Erasure-coded broadcast | [002](002-ec-broadcast.md) |

A protocol is shared on a view
when both `Hello` messages list its identifier with the same selectors.
The allowances may differ; each endpoint applies the other's.
A protocol listed by both endpoints with different selectors is not shared, and is not an error.
There is no partial match: a view never carries some of a protocol's selectors.

### 4.4. Stream credit

QUIC stream credit covers a whole connection, so it cannot keep one protocol, or one selector,
from exhausting it for the others. ethp2p therefore grants credit per selector on top of QUIC.

Each endpoint grants its peer, for every selector of every shared protocol, a cumulative allowance:
the number of streams with that selector the peer may open on the view.
The initial allowance is the `max_streams` of that selector in the receiver's `Hello`.
The receiver raises it with `Credit` as streams finish.
Allowances only increase; a `Credit` that does not raise one is ignored.

On the sending side, an endpoint MUST NOT open a stream with a selector once it has opened as many
as the peer's current allowance.
Protocol streams are opened only after admission
(section 7), so the sender always knows the peer's initial allowances.

On the receiving side, a stream counts against its selector's allowance from classification
until the protocol has finished with it: it has read the stream to its end or cancelled reading,
and, for a bidirectional stream, closed or cancelled writing too.
A stream the stack resets itself also finishes.
The receiver SHOULD send `Credit` for a selector once the finished streams it has not
yet credited reach half of that selector's `max_streams`, rounded up.
The new allowance is the number of finished streams plus `max_streams`.

A stream that arrives after the peer has used its allowance for
that selector is a control violation.

An endpoint MUST advertise allowances whose sum, plus one for the control stream,
does not exceed its QUIC incoming limit for either stream direction.
A conforming peer then never runs out of QUIC credit
because of ethp2p streams. libp2p streams on a shared connection still share QUIC's bidirectional
credit.

### 4.5. Closing a protocol

An endpoint that stops serving one protocol on a view
while keeping the view open sends `ProtocolClose` with the protocol's identifier and a code.
The code is either a stack-namespace code, or a protocol-namespace code of that protocol.

After sending it, the endpoint resets the protocol's queued
and new incoming streams with `UnsupportedSelector`, stops sending `Credit` for its selectors,
and treats the protocol as no longer shared on the view.
On receipt, the peer ends its protocol's handle for the view with the code
(section 9.2) and treats the protocol as no longer shared.
A `ProtocolClose` for a protocol that is not shared is ignored.

When no shared protocol remains, the endpoint closes the view with `NoSharedProtocols`
(section 4.6).

### 4.6. Closing a view

To close its view of a connection, an endpoint SHOULD send `GoAway` with a reason code,
then send FIN on its control stream, then release its view.
An endpoint that receives `GoAway` or FIN on the peer's control stream MUST treat the peer's view
as closed: it stops delivering new streams and datagrams for the connection to protocols,
ends every peer handle for the connection, and releases its own view.

A stack-namespace reset of the control stream closes the peer view with the reset's code.

A protocol-namespace reset is a control violation.

FIN without a preceding `GoAway` closes the peer view with `Unspecified`.

The endpoint answers a peer's closure with FIN on its own control stream, without `GoAway`.

`GoAway` remains informative and best-effort; it requires no reply or negotiation.
When releasing the ethp2p view also closes the physical connection,
the endpoint MUST send the same stack code in the QUIC application `CONNECTION_CLOSE`,
encoded with the wire layout in section 5.1.
A receiver MUST interpret a peer's connection close carrying a recognized stack-namespace
application code as a remote view closure with that code, just as if `GoAway` had arrived.
Whichever arrives first wins with the same result.
Odd or unknown application codes, QUIC transport errors,
timeouts and stateless resets retain their ordinary connection-error handling.

When libp2p releases the last view, its connection-close code remains libp2p's code.
A normal zero-code close means `Unspecified` to an ethp2p view still open at the receiver
(for example, when talking to an older peer).
It does not override a view already closed by `GoAway`.
Listener shutdown uses `Closing`.

This signal is the only way a peer learns that an ethp2p view closed
while libp2p keeps the physical connection open.

### 4.7. Sending order

An endpoint SHOULD open its control stream and send `Hello`
before opening any other stream on the view.
The receiver does not depend on stream order
(section 3.3), but early streams consume the receiver's bounded pre-`Hello` queue.

### 4.8. Control violations

On a control violation the endpoint sends `GoAway(ControlViolation)` and FIN on its control stream.
It cancels reading the peer's control stream with `ControlViolation` and releases its view.

Resetting its own stream could let the peer discard the `GoAway` frame.

### 4.9. Stateless resets

An endpoint SHOULD configure a QUIC stateless reset key derived deterministically from its identity
key, for example with HKDF-SHA256 over the private key and the label `ethp2p stateless reset`.
A restarted endpoint then answers packets on its previous connections with stateless resets,
and its peers close those connections within one round trip.
That shortens the window in which a stale connection wins a duplicate check (section 10.4).

## 5. Outcome codes

Stream resets (`RESET_STREAM`, `STOP_SENDING`), `GoAway`,
and an ethp2p-last application `CONNECTION_CLOSE` carry codes.

### 5.1. Wire layout

A QUIC application error code is a varint. ethp2p splits it into a value and a one-bit namespace:

```text
code = value << 1 | namespace        namespace 0: stack, 1: protocol
```

A protocol-namespace code is interpreted by the protocol
that owns the selector of the stream it resets, or by the protocol a `ProtocolClose` names.
The protocol is not encoded, because both endpoints already know it.

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
| 11 | `ControlViolation` | stack | The control stream protocol or a stream allowance was violated. |
| 12 | `NoSharedProtocols` | stack | No protocol is shared on the view, or none remains. |
| 13 | `Duplicate` | stack | Another connection to the same peer was kept. |

Values 0 to 3 are shared: either endpoint MAY send them on any stream,
and protocols SHOULD use them instead of defining their own equivalents,
so that senders can handle overload and refusal uniformly.
Values 4 to 7 are unassigned and decode as `Unspecified`.
Values 8 to 31 are sent only by the stack.
Values 14 to 31 are reserved for future stack codes, keeping every stack code to one byte.

A receiver MUST treat an unknown stack value on a stream reset or in `GoAway` as `Unspecified`.
An unknown connection-close value remains a connection error (section 4.6).
When a reset carries a defined stack-only value,
`ResetError.Code` preserves it for the receiving protocol.
If a protocol passes that code to a stream cancellation method, the wrapper sends `Unspecified`;
only stack-owned paths send stack-only values.
A protocol-namespace reset of the control stream is a control violation.

### 5.3. Protocol codes

Each protocol defines one table of code values in its own specification,
shared by all of its selectors.
Value `0` in the protocol namespace means "no protocol-specific reason".
The stack does not keep protocol code tables:
it preserves every received protocol value and hands it to the protocol that owns the stream.
The receiving protocol applies its own fallback for values it does not define.

### 5.4. Keeping codes small

Protocol specifications SHOULD follow these rules:

- Give the most frequent outcomes the lowest values, so they encode in one byte.
- Keep each protocol's code set below 32 values.
  Do not use sparse, bit-field, or category-range layouts.
- Never give value `0` a meaning.
- Use codes for why a stream ended, not for data.
  Send a frame before FIN when the peer needs a count, an identifier, or a position.
- Publish the protocol's code table, add values only at the end,
  and never renumber or reuse a value.
- Use the shared stack codes for refusal, overload, and timeouts.

## 6. Datagrams

Datagram routing follows [007](007-datagrams.md), with two refinements.
The shared selectors come from the shared protocols of section 4.3.
Selector `0x2f` is an ordinary selector for datagrams as it is for streams.

## 7. Connection admission

A view is admitted when both `Hello` messages have been exchanged.
Admission validates the Hello record and matches the shared protocols
(section 4.3) before taking the table mutex.
An invalid record is rejected with `ControlViolation`; if no protocol is shared,
the view is rejected with `NoSharedProtocols`.
Thus a view that shares no protocol never displaces a working view.
Admission calls no protocol code.
A protocol that does not want the peer refuses it after `PeerUp`, in its own loop (section 9.2).

Under the mutex, admission checks that the stack is open and
that an outbound dial has not been abandoned by Disconnect
(section 10.5), rejecting with `Closing` otherwise.
It then applies the duplicate rule (section 10.4), closing the loser with `Duplicate`.
When replacing a view, its peer contexts are cancelled
and its `PeerDown(Duplicate)` events are queued before the winner's `PeerUp` events.

Only then do protocols learn about the peer: each shared protocol gets `PeerUp`.
A view that loses the duplicate rule or shares no protocol never produces a peer handle.

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
	Admit(conn Conn, hello Hello) (code wire.Code, ok bool)
	Stream(conn Conn, sel wire.Selector, s ReceiveStream) // takes ownership
	Closed(conn Conn, code wire.Code)
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
package wire

type ProtocolID uint64 // assigned in section 4.3; 0 is reserved
```

```go
package ethp2p

type SelectorSpec struct {
	Selector    wire.Selector
	MaxInbound  int // streams each peer may have unfinished at this node (section 4.4); at least 1
	MaxOutbound int // streams this node keeps open to each peer at once; at least 1
}

type ProtocolSpec struct {
	ID        wire.ProtocolID
	Selectors []SelectorSpec // strictly ascending by selector
}

func (s *Stack) Register(spec ProtocolSpec) (*Protocol, error)
func (p *Protocol) Notify(wake chan<- struct{}) error // capacity >= 1, supplied by the protocol
func (p *Protocol) Next() (Event, bool)
```

```go
proto, err := stack.Register(ethp2p.ProtocolSpec{
	ID: broadcast.ProtocolID,
	Selectors: []ethp2p.SelectorSpec{
		{Selector: broadcast.BCAST, MaxInbound: 1, MaxOutbound: 1},
		{Selector: broadcast.SESS, MaxInbound: 64, MaxOutbound: 64},
		{Selector: broadcast.CHUNK, MaxInbound: 128, MaxOutbound: 128},
	},
})
proto.Notify(wake)
```

Registration and `Notify` MUST finish before `Start`.
`Register` fails for identifier `0`, a registered identifier, no selectors,
selectors that are unsorted, zero, or registered by another protocol, and allowances below 1.
`Start` fails when the allowances of all protocols, plus one for the control stream,
exceed the endpoint's QUIC incoming limit for either stream direction (section 4.4).

### 9.2. Events

```go
type Event struct {
	Kind     EventKind
	Peer     *Peer
	Selector wire.Selector // StreamIn (also DatagramIn when datagram delivery lands)
	Stream   ReceiveStream // StreamIn; bidi streams also implement Stream
	Code     wire.Code     // PeerDown
	// Payload []byte      // DatagramIn, added with datagram delivery
}

for ev, ok := proto.Next(); ok; ev, ok = proto.Next() {
	switch ev.Kind {
	case ethp2p.PeerUp:
		if !wanted(ev.Peer) {
			ev.Peer.Close(wire.Refused)
		}
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
Protocols MAY share one wake channel to run several protocols in one loop.

The stack guarantees:

- `PeerUp` precedes every other event for that peer.
- Every `PeerUp` is followed by exactly one `PeerDown`, the last event for that peer.
  Its code says why the handle ended: the view's closing code,
  the code of the peer's `ProtocolClose`, or the code the protocol passed to `Peer.Close`.
  Streams still queued for the handle when it ends are reset by the stack and never delivered:
  with `Closing` when the view closes, and with `UnsupportedSelector` otherwise.
- A peer whose view closes before its `PeerUp` was returned produces no events.
- A peer's streams are returned in the order the dispatcher finished classifying them.
- `Next` serves peers round-robin, one event at a time,
  so a peer with a large backlog cannot delay other peers' events.

Ownership of a delivered stream passes to the protocol when `Next` returns it.
The peer's context ends before `PeerDown` is queued.

A protocol decides whether it wants a peer in its own loop, with its own state:
admission calls no protocol code (section 7).
To refuse a peer, or to stop serving one later, it calls `Peer.Close`.

### 9.3. Bounds

QUIC and the per-selector allowances of section 4.4 supply stream backpressure:

- **Queued streams.**
  Every unfinished incoming stream counts against its selector's allowance,
  including one ending with its selector, because classification only peeks at it (section 3.2).
  A peer that has used an allowance waits in `OpenStream`
  or `OpenUniStream` instead of streams being dropped.
  The stack never resets a stream because a queue is full:
  a unidirectional sender may already have closed successfully, making that loss silent.
- **Isolation.**
  A protocol that stops draining exhausts only its own selectors' allowances.
  Other protocols on the connection, and its other selectors, keep their credit.
  Protocols shed load by taking a stream and cancelling it with `Overloaded`.
- **Dependencies between streams.**
  A protocol MUST NOT keep a stream unfinished while it waits
  for another stream with the same selector from the same peer.
  If streams of one selector can wait for streams of another selector from the same peer,
  and the reverse, the protocol MUST keep the waiting streams of at least one of the two below
  that selector's allowance.
  Otherwise both allowances can fill with streams that wait for each other.
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
func (p *Peer) Context() context.Context          // ends when the handle ends
func (p *Peer) OpenUniStream(ctx context.Context, sel wire.Selector) (ethp2p.SendStream, error)
func (p *Peer) OpenStream(ctx context.Context, sel wire.Selector) (ethp2p.Stream, error)
func (p *Peer) Close(code wire.Code)
func (p *Peer) SendDatagram(sel wire.Selector, payload []byte) error
func (p *Peer) MaxDatagramPayload(sel wire.Selector) int
```

A peer handle carries all of its protocol's selectors, because protocols are negotiated whole.
The open and send methods panic for a selector the protocol does not own,
and write the selector frame themselves, so protocols never write selector frames.

`OpenStream` and `OpenUniStream` wait until the selector has room under both the local `MaxOutbound`
and the peer's current allowance.
A stream counts against `MaxOutbound` until this node has finished with it:
closed or cancelled writing and, for a bidirectional stream, read to its end or cancelled reading.
They return the context's error if `ctx` ends first, and fail once the handle has ended.

`Close` ends this protocol's handle for the view.
The stack sends `ProtocolClose` with the code
(section 4.5), resets the handle's queued streams, and queues `PeerDown` with the code.
When no protocol holds the view any more, the stack closes it with `NoSharedProtocols`.
`Close` is idempotent, and has no effect once the handle has ended.
Streams the protocol already owns remain its own to finish.

Protocols never receive the raw `transport.Conn`.

### 9.5. Streams and codes

`wire` owns the code vocabulary.
`wire.Code` is comparable, and its zero value is `wire.Unspecified`.
A protocol declares its codes with `ProtocolCode`:

```go
package wire

type Code struct{ /* value uint64; protocol bool */ }
func ProtocolCode(value uint16) Code

func (c Code) Wire() uint64
func ParseCode(raw uint64) Code
```

`Wire` encodes `value<<1 | namespace`.
`ParseCode` preserves protocol-namespace values and maps unknown stack values to `Unspecified`.
A protocol-namespace `Code` means whatever the protocol that owns the stream defines;
the same value can mean different things to different protocols.
The shared codes `Unspecified`, `Refused`, `Overloaded`, and `Timeout` are valid on any stream.
Stack-only codes are exported from `wire` so protocols can inspect them after a reset;
only stack-owned send paths encode their wire values.

The root `ethp2p` package owns the stream interfaces and wrappers:

```go
package ethp2p

type SendStream interface {
	CancelWrite(wire.Code)
}
type ReceiveStream interface {
	CancelRead(wire.Code)
}
type Stream interface {
	CancelRead(wire.Code)
	CancelWrite(wire.Code)
}
type ResetError struct {
	Code wire.Code
}
func (e Event) Cancel(wire.Code) // StreamIn only; other kinds are no-ops
func (e Event) Reject() // deliberate refusal; sends Refused for StreamIn
```

The wrappers are unexported.
Their cancellation methods send a stack-only code as `Unspecified`.
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
`Event.Cancel(wire.Unspecified)`; `Event.Reject` is reserved for deliberate refusals.

Broadcast keeps its code table beside its selectors:

```go
var (
	Reconstructed = wire.ProtocolCode(1)
	Redundant     = wire.ProtocolCode(2)
)

stream.CancelWrite(broadcast.Reconstructed)
stream.CancelRead(wire.Overloaded)
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
Stateless resets (section 4.9) close it within one round trip; without them,
the QUIC idle timeout bounds the delay.

### 10.5. Disconnecting

```go
err := stack.Disconnect(peerID)
```

`Disconnect` abandons any dial attempt for the peer, sends `GoAway(Closing)` on the peer's view,
releases it, and returns once the view has been released.
A per-peer generation counter prevents an outbound dial
that completes afterwards from being admitted.
The gate applies after protocol matching, before committing the outbound view.
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
They can be layered on `Connect`, `Disconnect`, and protocols' `PeerUp` handling later.

## 11. Migration

**Broadcast.** `Engine.Register(stack)` registers protocol 1 with BCAST, SESS,
and CHUNK before Start, with the allowances of spec 002 and a wake channel owned by the engine.
The engine drains `Next` from its event loop, routing events to `PeerConn` directly:
`PeerUp` creates a binding, `StreamIn` routes to an existing binding or is refused,
and `PeerDown` closes the binding.
The two notification channels, `Serve`,
and the logic for a stream that arrives before its peer disappear.
`PeerConn` keeps its own bounded per-peer queues for SESS and CHUNK streams that arrive
before its BCAST handshake completes,
because that ordering is broadcast's rule rather than the stack's.
Broadcast defines its code table in spec 002:

| Value | Name | Meaning |
| ----: | ---- | ------- |
| 1 | `Reconstructed` | The sender has reconstructed the message. |
| 2 | `Redundant` | The receiver no longer needs this chunk. |

Overload of broadcast's own per-peer limits uses the shared `Overloaded`.
In Go, the codes and `ProtocolID` are declared in `broadcast/protocol.go`;
shared stack codes are referenced from `wire` directly.

**Simulation and tests.**
They build records for their nodes with `enr.Sign`, call `Start`, `Connect`, and `Close`,
and read bandwidth from `Traffic`.
Their connection lists, accept loops, and serving goroutines disappear.

**Specifications.**
Spec 002 describes the BCAST rationale, the stream head, the code table, and the allowances.
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
| 1. Outcome codes | `Code`, the wire layout, stream wrappers, and broadcast's code tables | — |
| 2. Dispatcher routing | First-byte classification, full selector reads under one deadline, bounded concurrency per direction, the pre-`Hello` queue | — |
| 3. Control stream | `Hello` and `GoAway` replace the advertisement stream; stateless reset key | Dispatcher routing |
| 4. Delivery | `Register`, `Notify`, `Next`, the peer handle, the bounds, and broadcast's migration | Outcome codes, control stream |
| 5. Connection management | `NewStack`, `Start`, `Close`, the table, `Connect` by record, duplicates, `Disconnect`, inspection, and the caller migration | Delivery, the `enr` package |
| 6. Protocols | Package `wire`; `ProtocolID` and the registry; `Hello` announcing protocols; `Protocol` and `ProtocolSpec` replacing `Subsystem`; one code table per protocol; broadcast's migration | 1–5 |
| 7. Admission in the event loop | No admission callback; `Peer.Close`; `ProtocolClose`; the `PeerUp`/`PeerDown` pairing | 6 |
| 8. Stream credit | `SelectorSpec` allowances, `Credit`, receive-side accounting and violations, waiting opens, the `Start` limit check; broadcast's allowances | 7 |
| 9. Datagram delivery | 007 on top of delivery | 8 and a first consumer |

Each unit must leave the full test suite passing,
including the race detector and the integration and short simulation suites.

## 13. Open questions

**Dynamic registration.** `Hello` fixes the shared protocols for a view's lifetime.
A later `Update` message could announce protocols added after start;
`ProtocolClose` already covers removing one from a view.

**Priorities.**
The stack does not prioritize between protocols.
Stream and datagram priorities from spec 001 need their own design.
Allowances isolate protocols from each other's backlog
(section 9.3), but do not order streams that have credit; that belongs to the priorities design.

**Protocol shutdown.**
A protocol cannot unregister while the stack runs.
It can end every handle with `Peer.Close`, but new views still share it.
If one needs to stop entirely,
`Protocol.Close` would close every handle and stop announcing the protocol in `Hello`.

**Shared dial ownership.** libp2p still dials on its own.
Guaranteeing one physical connection per peer across both stacks needs a shared dial owner.
