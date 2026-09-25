# The ethp2p stack: routing, delivery, and connections

Status: units 1–5 implemented.
Family registration, local peer closure, and per-protocol queue limits are specified here and pending;
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
   It owns registration, a peer table, and delivery queues with local protocol limits.
   It runs one supervisor goroutine per connected peer, which alone changes that peer's state.
2. **One runtime per connection, where one already exists.**
   The transport's per-connection dispatcher already reads the first bytes of every stream.
   It now reads the whole selector, runs the stack's control stream, and receives datagrams.
   The stack adds only the per-peer supervisor above it.
3. **Families pull.**
   The stack queues events for each Family and signals its supplied wake channel.
   The consumer drains the events in whatever loop shape suits it.
4. **Outcomes are explicit.**
   Every stream reset and view closure carries a code in a defined namespace,
   so a peer can tell "unsupported", "refused", and "done" apart.

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

**Family.**
A set of protocols registered together, with one event queue and wake channel, such as broadcast.
A Family is shared with a peer only when the peer supports all of its protocols.
It has no wire identifier; sharing is derived from the selectors in `Hello` (section 4.3).

**Protocol.**
The wire behavior identified by one selector, with an associated local queue limit.
BCAST, SESS, and CHUNK are the protocols grouped by broadcast's Family.

**Selector.**
The unsigned integer identifying a protocol's streams and datagrams on the wire.
Each registered selector belongs to exactly one local Family.
Selector `0` identifies the stack's control stream.

**Selector frame.**
An unsigned-varint length `n` between 1 and 10,
followed by the selector's minimal unsigned-varint encoding, which occupies exactly `n` bytes.

**Shared protocols.**
The protocols of the Families shared on a connection (section 4.3).

**Peer.**
A Family's handle for one connected peer.
The same physical connection yields one peer handle per shared Family.

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
Every queued stream is an open QUIC stream, so QUIC's incoming stream limit bounds the queue.
Per-protocol queue limits are described in section 9.3.
A classifier releases its slot as soon as it enqueues the stream.

Because classification depends on the length byte rather than the first payload byte,
selector `0x2f` is no longer reserved.

### 3.3. Routing

After classification, the dispatcher routes the stream by selector:

- Selector `0` on a unidirectional stream is the peer's control stream (section 4).
  A second one, or selector `0` on a bidirectional stream, is a control violation (section 4.8).
- A selector that is not shared, or whose local Family handle has ended, is reset with `UnsupportedSelector`.
- A stream that arrives before the peer's `Hello` waits and is routed when the `Hello` is processed.
  QUIC's incoming stream limit bounds how many can wait.
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
    Hello  hello   = 1;
    GoAway go_away = 2;
  }
}

message Hello {
  // Supported protocol selectors, strictly ascending.
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

A `Hello` is invalid if:

- its selectors are not strictly ascending, a selector is `0`, or it lists more than 1024 selectors;
- `record` is present and fails verification,
  or the record's identity differs from the identity authenticated by the TLS handshake.

The transport validates the selector list on both send and receive.
The stack validates each non-empty record.
An invalid `Hello` is a control violation.

After both `Hello` messages have been exchanged,
the shared protocols are fixed for the lifetime of the view.

### 4.3. Protocols

A local Family is shared on a view when the peer's `Hello` lists every selector of the Family.
A Family is never partially shared:
a peer handle guarantees that the peer announced all of the Family's protocols.
A Family with any selector missing gets no peer handle, and its selectors are not shared.
There is no Family identifier or registry on the wire.
Changing a Family's set of protocols is an incompatible change.
A protocol specifies how it versions incompatible changes, for example with a new selector
or a version exchanged on its streams.

### 4.4. Stream limits

QUIC supplies stream limits and flow control for the connection.
ethp2p does not exchange per-protocol credit or advertise local limits in `Hello`.
Local queue limits are described in section 9.3.
Protocols share QUIC capacity, including bidirectional capacity used by libp2p.
The stack does not guarantee capacity isolation between protocols.

### 4.5. Local Family closure

Ending a Family's local peer handle sends no stack control message.
Protocols handle remote lifecycle signalling themselves when needed.
The local cleanup is specified in section 9.4.
When no local Family holds the view, the endpoint closes the view with `NoSharedProtocols`
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
(section 3.3), but early streams consume space in the receiver's pre-`Hello` queue.

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
identified by the selector of the stream it resets.
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
| 11 | `ControlViolation` | stack | The control stream protocol was violated. |
| 12 | `NoSharedProtocols` | stack | No Family is shared on the view, or no local Family holds it. |
| 13 | `Duplicate` | stack | Another connection to the same peer was kept. |

Values 0 to 3 are shared: either endpoint MAY send them on any stream,
and protocols SHOULD use them instead of defining their own equivalents,
so that senders can handle overload and refusal uniformly.
Values 4 to 7 are unassigned and decode as `Unspecified`.
Values 8 to 31 describe stack outcomes; callers should use them only for those outcomes.
Values 14 to 31 are reserved for future stack codes, keeping every stack code to one byte.

A receiver MUST treat an unknown stack value on a stream reset or in `GoAway` as `Unspecified`.
An unknown connection-close value remains a connection error (section 4.6).
When a reset carries a defined stack-only value,
`ResetError.Code` preserves it for the receiving protocol.
Cancellation methods encode the supplied code without rewriting it.
A protocol-namespace reset of the control stream is a control violation.

### 5.3. Protocol codes

Each protocol defines its code values in its specification.
Related protocols may share a code table, as BCAST, SESS, and CHUNK do in spec 002.
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

## 7. Connecting a peer

A view becomes usable after both `Hello` messages have been exchanged and the following checks pass.
The stack validates the Hello record and matches the shared Families
(section 4.3) before taking the table mutex.
An invalid record is rejected with `ControlViolation`; if no Family is shared,
the view is rejected with `NoSharedProtocols`.
Thus a view that shares no Family never displaces a working view.
The peer's supervisor runs these checks; they call no consumer code.
A Family's consumer that does not want the peer closes its handle after `PeerUp`, in its own loop (section 9.2).

Under the mutex, the stack checks that it is open and
that an outbound dial has not been abandoned by Disconnect
(section 10.5), rejecting with `Closing` otherwise.
It then applies the duplicate rule (section 10.4), closing the loser with `Duplicate`.
When replacing a view, its peer contexts are cancelled
and its `PeerDown(Duplicate)` events are queued before the winner's `PeerUp` events.

Only then does each shared Family get `PeerUp`.
A view that loses the duplicate rule or shares no Family never produces a peer handle.

## 8. Layering and runtime

```text
quic-go                       packet I/O, QUIC state machines
transport.SharedTransport     endpoint, accept loop, per-connection dispatcher:
                                stream classification and selector reads,
                                control stream and Hello, datagram receipt
ethp2p.Stack                  registration, peer table, one supervisor goroutine per peer
Family consumers (broadcast)  their own state and loops, fed by Next
application                   Connect, Disconnect, shutdown order
```

The transport owns the wire: classification, the control stream, `Hello`, and `GoAway`.
It hands the stack an ethp2p view as a concrete `*transport.Conn`
only after the peer's `Hello` has arrived and been validated,
from `Ethp2pTransport.Accept` or `Ethp2pTransport.Dial`.
There is no callback interface between the two.

```go
package transport

func (t *Ethp2pTransport) Accept(ctx context.Context) (*Conn, error)
func (t *Ethp2pTransport) Dial(ctx context.Context, addr net.Addr, expect PeerID) (*Conn, error)
func (t *Ethp2pTransport) Close() // stops delivering views; queued and later ones close with Closing

func (c *Conn) PeerHello() Hello                                   // the peer's validated Hello
func (c *Conn) Streams() <-chan struct{}                           // wake: classified streams are waiting
func (c *Conn) NextStream() (ReceiveStream, wire.Selector, bool)   // never blocks
func (c *Conn) Done() <-chan struct{}                              // closed when the view is released
func (c *Conn) CloseCode() wire.Code                               // why; valid once Done is closed
```

`NextStream` returns classified streams in the order classification finished,
unidirectional and bidirectional alike; a bidirectional stream also implements `Stream`.
`Streams` is signalled without blocking after every enqueue,
so a caller that drains until `NextStream` reports empty never misses a stream.

The stack runs one goroutine that accepts views, and one supervisor goroutine per connected peer.
The supervisor is the only code that changes that peer's state:
its view, its routes, and its peer handles.
It receives every view for its peer, dialed or accepted, and applies the checks in section 7
and the duplicate rule in section 10.4.
It drains the view's streams and routes each one to its Family (section 9.3).
It handles `Peer.Close`, `Disconnect`, stack closure, and the view's release.
Opening outgoing streams does not go through the supervisor:
`Peer.OpenStream` calls the view directly, so a blocked open never delays incoming delivery.
A supervisor exits once its peer has no view and no pending work.
Closing a view waits up to the `GoAway` timeout,
so the supervisor does it in a goroutine joined by `Stack.Close`.

## 9. Delivery to protocols

### 9.1. Registration

```go
package ethp2p

type ProtocolSpec struct {
	Selector  wire.Selector
	MaxQueued int // per peer: incoming streams not yet returned by Next; at least 1
}

func (s *Stack) Register(protocols []ProtocolSpec, wake chan<- struct{}) (*Family, error)
func (s *Family) Next() (Event, bool)
```

```go
family, err := stack.Register([]ethp2p.ProtocolSpec{
	{Selector: broadcast.BCAST, MaxQueued: 1},
	{Selector: broadcast.SESS, MaxQueued: 64},
	{Selector: broadcast.CHUNK, MaxQueued: 128},
}, wake)
```

Registration MUST finish before `Start`.
`Register` fails for an empty protocol list, selectors that are unsorted, zero,
or already registered, limits below 1, or a nil or unbuffered wake channel.
The wake channel has capacity at least 1 and is supplied at registration;
there is no separate notification setup step.
Families may share a wake channel.
Queue limits are local; they are not advertised to peers.

### 9.2. Events

```go
type Event struct {
	Kind     EventKind
	Peer     *Peer
	Selector wire.Selector // StreamIn (also DatagramIn when delivery lands)
	Stream   ReceiveStream // StreamIn; bidi streams also implement Stream
	Code     wire.Code     // PeerDown
	// Payload []byte      // DatagramIn, added with datagram delivery
}

for ev, ok := family.Next(); ok; ev, ok = family.Next() {
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
After it returns `ok == false`, the consumer waits on its Family's wake channel and drains again.
The stack sends on the wake channel without blocking after every enqueue,
so a wake is never lost as long as the consumer drains until `Next` reports empty.

The stack guarantees:

- `PeerUp` precedes every other event for that peer.
- Every `PeerUp` is followed by exactly one `PeerDown`, the last event for that peer.
  Its code says why the local handle ended: the view's closing code
  or the code its consumer passed to `Peer.Close`.
  Streams still queued for the handle when it ends are reset by the stack and never delivered:
  with `Closing` when the view closes, and with `UnsupportedSelector` otherwise.
- A peer whose view closes before its `PeerUp` was returned produces no events.
- A peer's streams are returned in the order the dispatcher finished classifying them.
- `Next` serves peers round-robin, one event at a time,
  so a peer with a large backlog cannot delay other peers' events.

Ownership of a delivered stream passes to the protocol when `Next` returns it.
The peer's context ends before `PeerDown` is queued.

A Family's consumer decides whether it wants a peer in its own loop, with its own state.
The checks in section 7 call no consumer code.
To refuse a peer, or to stop serving one later, it calls `Peer.Close`.

### 9.3. Bounds

Protocols are expected to actively consume data from their incoming streams.
When a protocol no longer needs incoming data, it cancels the read side of the stream.
This is a protocol responsibility, not a guarantee enforced by the stack.
Unread data can exhaust shared QUIC connection flow-control capacity and delay other protocols.

QUIC supplies connection-wide stream limits and backpressure.
The stack adds one local bound per protocol and does not guarantee progress between protocols.

- **Queued streams per peer and protocol.**
  `MaxQueued` bounds the incoming streams waiting for `Next` to return them.
  A stream beyond the bound is reset with `Unspecified` and never delivered.
  The code does not say why, so that a peer cannot probe local queue state.
  A stream stops counting when `Next` returns it; the protocol bounds the streams it owns.
- **Dependencies between streams.**
  Consumers must avoid holding all available stream capacity while waiting for another
  stream to make progress.
  Broadcast retains its separate live-session and parked-CHUNK limits.
- **Datagrams per peer and protocol.**
  A ring buffer discards the oldest datagram, as 007 requires.
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

A peer handle belongs to a Family and provides access to its shared protocols.
Open and send methods panic for selectors the Family does not own.
They return an error when the handle has ended.
They write the selector frame themselves.

`OpenStream` and `OpenUniStream` wait for QUIC's stream limit; waiting is context-cancellable.
Every protocol of a shared Family is supported by the peer.
A stream of a shared Family reset with `UnsupportedSelector` is an error for that peer:
the peer broke its `Hello`, or ended its own handle (section 4.5).

`Close` ends this Family's local handle for the view.
It resets queued streams with `UnsupportedSelector`, ends the handle's context,
and queues a local `PeerDown` with the supplied code.
New streams for that handle's protocols are reset with `UnsupportedSelector`.
Other Families continue using the view.
When no Family holds the view, the stack closes it with `NoSharedProtocols`.
`Close` is idempotent and has no effect once the handle has ended.
Streams already delivered remain the consumer's responsibility to finish.
No stack control message is sent for the local handle closure, and the remote handle
does not end merely because this local handle ended.
Any required remote signalling belongs to the protocols.

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
Stack outcome codes are exported from `wire` so consumers can inspect and send them.
Cancellation methods encode the supplied code without rewriting it.

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
Their cancellation methods preserve the supplied code.
They expose no numeric code API.
`Stream` has no `Reset` method: callers cancel each side with its outcome.
Reads that end in a reset return `*ethp2p.ResetError`, whose `Unwrap` returns the transport error.

Cancelling a side is idempotent: the first code is the one sent, and later calls have no effect.
A failed `Read`, `Write`, or `Close` returns its error without automatically cancelling the side.
Deadlines retain the underlying stream's behavior; callers can extend a deadline and resume
I/O when the stream remains usable.
Consumers explicitly cancel sides they abandon, including after decode or I/O errors.
A peer reset is returned as `ResetError`; no reset is sent in reply.

A Family's consumer that stops disposes of the events it has not handled with
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
// Register Families with their protocols and wake channels here.
err = stack.Start()    // sets Hello, starts listening and accepting; reports listener errors synchronously
...
err = stack.Close()    // sends GoAway(Closing), releases every view, resets queued streams
```

`Start` fails if called twice or after `Close`.
`Close` is idempotent and returns once every view the stack accepted has been released
and every supervisor has exited.
It calls `Ethp2pTransport.Close`, so views arriving afterwards are closed with `Closing`.
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

- returns `nil` at once if an accepted connection to the record's peer exists;
- otherwise dials each QUIC endpoint in order, requiring the record's identity, until one succeeds;
- returns `nil` once the view is accepted
  (section 7),
  or the error that prevented the connection: a dial error, `transport.ErrDialLegacyPeer`,
  or a rejection such as `NoSharedProtocols`.

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

At most one ethp2p view per peer is accepted.
When a view is accepted while another is active for the same peer:

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
that completes afterwards from being accepted.
The supervisor applies the gate after protocol matching, before committing the outbound view.
Generation entries remain only while a view or dial record exists:
any stale connection attempt still has its driver's dial record carrying the old generation.
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
`Traffic` returns cumulative QUIC byte counts over every connection the stack accepted,
including closed ones.
On shared connections the counts include libp2p traffic.

### 10.7. Out of scope

Connection limits, discovery, retries, peer scoring,
and a shared dial owner for libp2p and ethp2p are not part of this design.
They can be layered on `Connect`, `Disconnect`, and protocols' `PeerUp` handling later.

## 11. Migration

**Broadcast.** `Engine.Register(stack)` registers one Family containing the BCAST, SESS,
and CHUNK protocols before Start, with the local limits of spec 002 and the engine's wake channel.
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
In Go, the codes and selectors are declared in `broadcast/protocol.go`;
shared stack codes are referenced from `wire` directly.

**Simulation and tests.**
They build records for their nodes with `enr.Sign`, call `Start`, `Connect`, and `Close`,
and read bandwidth from `Traffic`.
Their connection lists, accept loops, and serving goroutines disappear.

**Specifications.**
Spec 002 describes the BCAST rationale, the stream head, the code table, and the local limits.
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
| 1. Outcome codes | `Code`, wire layout, stream wrappers, and broadcast code tables | — |
| 2. Dispatcher routing | Classification, selector reads under a deadline, bounded concurrency, pre-`Hello` handling | — |
| 3. Control stream | `Hello` selector list and `GoAway`; stateless reset key | Dispatcher routing |
| 4. Delivery | `Family`, `ProtocolSpec`, registration with a wake channel, `Next`, peer handles, and broadcast migration | Outcome codes, control stream |
| 5. Connection management | Stack lifecycle, connection table, record-based dialing, duplicates, disconnect, inspection | Delivery, `enr` |
| 6. Simplification | Replace `Subsystem` with `Family`, shared only when complete; replace the transport `Sink` with per-peer supervisors and a concrete `transport.Conn`; remove policy callbacks; local `Peer.Close`; preserve supplied codes and ordinary I/O-error/deadline behavior | 1–5 |
| 7. Queue limits | Per-protocol `MaxQueued`; streams beyond it are reset with `Unspecified` | 6 |
| 8. Datagram delivery | 007 on top of Family delivery | 7 and a first consumer |

Units 1–5 have an existing implementation; the revised API and behavior above still require migration.
Each unit must leave the full test suite passing, including the race detector and short simulation suite.
Use the migrated e2e scenarios in place of the removed broadcast integration package.

## 13. Out of scope

Dynamic registration, Family unregistration while running, protocol priorities,
and shared dial ownership between libp2p and ethp2p are out of scope.
