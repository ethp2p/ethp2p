---
status: implemented
requires:
  - 01-quic-and-tls.md
  - ../00-foundations/06-outcome-codes.md
---

# Streams

This document defines how a stream names its protocol, how bytes on a stream are framed, how two peers agree on the protocols they share, the bounds on streams nobody has read yet, and how a stream ends.
It builds on [QUIC and TLS](01-quic-and-tls.md); the protocols a peer serves travel in `Hello` ([Control protocol](../05-control/01-control.md)), and each protocol's frames are defined by its spec, for example [Broadcast framework](../03-broadcast/01-broadcast-framework.md).
Assigned numbers are collected in [Registries](../00-foundations/04-registries.md).
Notation and the meaning of MUST and SHOULD are in [Conventions](../00-foundations/02-conventions.md); defined terms are in [Terminology](../00-foundations/03-terminology.md).

## Goals

- A stream names its protocol in its first bytes, with no negotiation round trip.
- A reader finds where each frame ends and what it carries without decoding its payload.
- Two peers learn which protocols they share from one message each.
- Streams nobody has read stay bounded in memory, whether the peer is slow or hostile.
- A peer can tell why each stream ended.

## Stream header

Every ethp2p stream, unidirectional or bidirectional, begins with a stream header: the stream's protocol id, as one minimal uvarint.
Every byte after it belongs to the protocol the header names.

```text
stream        = stream_header || protocol_bytes
stream_header = uvarint(protocol)   # minimal and at most 32 bits: 1 to 5 bytes
```

The protocol id is the stream's selector.
Protocols are the only registered, announced and negotiated unit: there is no grouping of protocols on the wire.
Protocol id 19 is reserved and never assigned, because its encoding, `0x13`, is the first byte of a libp2p stream ([libp2p compatibility](04-libp2p-compat.md)).

```text
00                       protocol 0 (control)
01                       protocol 1 (broadcast)
ac 02                    protocol 300
ff ff ff ff 0f           protocol 2^32-1: the longest header
```

These headers are rejected with `BadSelector`:

```text
13                       protocol 19: reserved
80 00                    protocol 0 encoded in two bytes: not minimal
80 80 80 80 10           protocol 2^32: above 32 bits
                         FIN before the header: truncated
```

### Reading a stream header

A receiver MUST read and validate the whole stream header before it hands the stream to a protocol.
A field that is not a minimal uvarint, a field above 32 bits, protocol 19, or FIN before a bidirectional stream's header ends is `BadSelector`.
A unidirectional stream whose FIN arrives before its header ends is discarded silently, with nothing sent to its sender.
A receiver MUST NOT read past the header, so the protocol receives every byte after it.

A stream MUST finish its stream header within `CLASSIFY_TIMEOUT` (5 seconds) of when the receiver starts reading it; otherwise the receiver resets it with `Timeout`.
A receiver MUST NOT read more than `MAX_STREAMS_PENDING_CLASSIFY` (4) stream headers at once per direction on one connection, and the two directions MUST NOT share these slots.
A receiver MUST release a slot as soon as its stream is handed over or reset.
A stream whose header is not yet being read MUST wait unread, holding its stream credit.

Reading stream headers is the one place where a receiver works on a stream before any protocol owns it, so it has bounds of its own.
Streams beyond the slots wait unread, and the peer sees its stream limit rather than resets.
Separate slots keep a slow stream in one direction from delaying the other.
The bounds cover sustained packet loss as well as malicious peers: a review found one incomplete stream header blocking a valid later stream for longer than the timeout.

## Frames

After the stream header, a protocol's bytes are a sequence of frames:

```text
frame = uvarint(type) || prefixed(payload)   # type || length || payload
```

The type names what the payload carries; types are numbered from 0 within each protocol, so the same number means different frames on different protocols.
Each frame type's payload is one protobuf message, which its spec names, unless the spec defines raw bytes, as `DATA` on a broadcast chunk stream does ([Broadcast protocol](../03-broadcast/02-broadcast-protocol.md#chunk-kind)).
Frames are carried in a stream's bytes and are unrelated to QUIC frames, which carry those bytes ([RFC 9000]).

For example, a control `GOAWAY` frame carrying `ControlViolation`:

```text
01                       type 1 (GOAWAY)
02                       length: 2 bytes follow
08 0b                    GoAway field 1 (code): 11
```

These frames are malformed:

```text
80 00 00                 type 0 encoded in two bytes: not minimal
00 80 00                 length 0 encoded in two bytes: not minimal
00 02 aa                 type 0, length 2, then FIN after one byte: truncated
00                       type 0, then FIN: truncated
```

### Reading a frame

A receiver MUST treat a frame as malformed if its type or length is not a minimal uvarint, or if FIN arrives after its first byte and before it ends.
A receiver MUST compare the length with the protocol's payload limit before it allocates for the payload, and MUST treat a longer frame as malformed.
A receiver MUST NOT read past the end of a frame.
Each protocol defines its payload limit and what a malformed frame causes.

A minimal type and length give each frame exactly one encoding.
Checking the length before allocating means a peer cannot make a receiver hold more than the limit by announcing a large frame.
The bytes after a frame belong to the next frame, so a reader that consumed them would hand them to the wrong reader.

### Frame types

Each protocol's spec lists its frame types and the order in which they may appear on a stream.
A receiver MUST treat a frame whose type the protocol does not define, or whose type is not allowed at its position on the stream, as an error; the protocol's spec names the outcome code.
A payload that does not decode as the message its type names is malformed.

## Protocols

A protocol is the wire behaviour of the streams with one protocol id, named on the wire by that id.
The spec that defines a protocol assigns its id ([Registries](../00-foundations/04-registries.md)).
Broadcast is protocol 1: one protocol whose streams it classifies itself with a kind byte after the stream header ([Broadcast protocol](../03-broadcast/02-broadcast-protocol.md)).

Control is protocol 0, the [control protocol](../05-control/01-control.md).
Every node serves it, and it is shared on every view without being announced.
It cannot be announced, because `Hello` travels on it: a peer reading a `Hello` already speaks the control protocol, so listing protocol 0 could tell it nothing.

### Announcing protocols

A node MUST list in its `Hello` the id of every protocol it serves other than the control protocol, and MUST NOT list a protocol it does not serve.

The list in `Hello.protocols` is strictly ascending, contains neither 0 nor 19 and has at most `MAX_PROTOCOLS` (1024) entries; any other form is a control violation ([Control protocol](../05-control/01-control.md)).
A set of protocols therefore has exactly one valid encoding.

### Shared protocols

Once it has accepted the peer's `Hello`, a node MUST determine its shared protocols as follows:

1. The control protocol is shared.
2. Any other protocol the node serves is shared if the peer's `Hello.protocols` lists its id.
3. Any other protocol is not shared.

A node MUST determine the shared protocols once per view and MUST NOT change them for the life of the view.
If no protocol other than the control protocol is shared, the node MUST close the view with `NoSharedProtocols`, before it applies the duplicate rule ([Connection lifecycle](03-connection-lifecycle.md)).

For example, node A serves broadcast (1) and a protocol X (7), and node B serves broadcast only:

```text
A's Hello.protocols   1 7
B's Hello.protocols   1
A shares              broadcast (X is not shared: 7 is not in B's list)
B shares              broadcast
```

Because each protocol's id is fixed by its spec, a protocol is shared at one node exactly when both nodes serve it.
Both nodes therefore compute the same shared protocols from the two `Hello`s, with no further exchange.
A protocol can rely on its own internal structure being present at both ends: broadcast's kinds are its own classification, not separately negotiated units, so a broadcast session never finds its chunk streams unroutable on a peer that shares broadcast.

## Routing streams

A receiver MUST reset a stream with `UnsupportedProtocol` if its protocol is not shared.

The stack routes and bounds protocols only; it MUST NOT read past the stream header, so how a protocol classifies its own streams is the protocol's business, not the stack's.
Broadcast, for example, reads a kind after the header and refuses kinds it does not define itself ([Broadcast protocol](../03-broadcast/02-broadcast-protocol.md)).

The control protocol is the exception, because its streams are the view's own ([Control protocol](../05-control/01-control.md)): a bidirectional stream of protocol 0, and a second control stream, are control violations.

### Stopping a protocol

A node MAY stop serving a protocol other than the control protocol to one peer.
It then resets the protocol's unread streams, and new streams for the protocol, with `UnsupportedProtocol`, and closes the view with `NoSharedProtocols` once it serves the peer only the control protocol.
Stopping a protocol sends no control message: the protocol signals its own lifecycle, and the reset tells the peer when it next opens a stream.

A peer that receives `UnsupportedProtocol` on a stream of a shared protocol SHOULD treat it as the protocol having stopped, not as a violation.

### Unread streams

A receiver MUST NOT reset a stream only because no protocol has read it yet, including a stream that arrives before the peer's `Hello`.
The stream keeps its credit until it is read, so QUIC's stream limit and windows apply backpressure to the peer.
A fixed queue that reset streams past its capacity would add a second limit that disagrees with QUIC's.

A protocol MAY bound the unread streams it accepts from one peer, and MUST reset streams over its bound with `Unspecified`.
An overload code would tell the peer how loaded the receiver is.

## Ending a stream

Each direction of a stream ends on its own, with FIN or with a reset that carries an outcome code ([Outcome codes](../00-foundations/06-outcome-codes.md)).
The receiving side of either direction may also send `STOP_SENDING` with an outcome code.

| Stream | Each sending direction carries |
| ------ | ------------------------------ |
| Unidirectional | stream header, frames, then FIN or `RESET_STREAM` |
| Bidirectional, opener to peer | stream header, frames, then FIN or `RESET_STREAM` |
| Bidirectional, peer to opener | frames, then FIN or `RESET_STREAM` |

A protocol MUST end every stream it holds, finishing it or resetting each direction it abandons, including after a decode or I/O error.
A stream that is never ended keeps the peer's credit, which all protocols on the connection share.
The protocol chooses each reset's code, because only the protocol knows what the failure was; the first reset of a direction is the one sent.
A protocol MAY refuse a stream with `Refused`, and SHOULD use `Unspecified` for a stream it drops without refusing it, such as on shutdown.

### Resets by the receiver

Before a protocol reads a stream, the receiver resets it in these cases and no others, both directions of a bidirectional stream with one code:

| Condition | Effect on the stream |
| --------- | -------------------- |
| Stream header not finished within `CLASSIFY_TIMEOUT` | Reset with `Timeout`. |
| Invalid stream header | Reset with `BadSelector`; the read side only on a unidirectional stream, and nothing for a unidirectional stream ended by FIN before its header. |
| Protocol not shared, or protocol stopped | Reset with `UnsupportedProtocol`. |
| Over a protocol's bound on unread streams | Reset with `Unspecified`. |
| Unread when the view closed, or arrived after it closed | Reset with `Closing`. |
| Protocol 0 on a bidirectional stream | Reset with `ControlViolation`; the view closes ([Control protocol](../05-control/01-control.md)). |
| A second control stream, on a unidirectional stream | Read cancelled with `ControlViolation`; the view closes. |
| Peer reset a bidirectional stream during its stream header | Write side reset with `Unspecified`. |

No reset is sent once the connection is closed.

## Changes

The stream header and the frame layout have no extension points, and a malformed frame or an unknown frame type is an error, not a case to skip.
A change to the stream header or the frame layout ships with a new application protocol name ([QUIC and TLS](01-quic-and-tls.md)).
A protocol versions itself with a new protocol id; a protocol adds a frame type with a new type number, and classifies its streams in a new way with a new kind number where it defines kinds.
A peer that does not know the new number resets the stream with `UnsupportedProtocol`, or treats the frame as its protocol's error, so either change needs the peer to opt in first, for example through a new protocol id.

QUIC treats a frame of unknown type as a connection error ([RFC 9000, Section 12.4][RFC 9000 frames]), and ethp2p frames follow it.
HTTP/3 instead ignores unknown frame types so that extensions can be deployed without negotiation ([RFC 9114, Section 9][RFC 9114 extensions]), as [RFC 9170] recommends.
Rejecting them is safe here because the only peers that reach an ethp2p endpoint are the ones that selected `ethp2p_0`, and every protocol's streams and frames are fixed by its spec, so an unrecognised frame is a bug or an attack.

## Constants

| Name | Value |
| ---- | ----- |
| `CLASSIFY_TIMEOUT` | 5 seconds |
| `MAX_STREAMS_PENDING_CLASSIFY` | 4 per direction on one connection |
| `MAX_PROTOCOLS` | 1024 |

## Failure modes

### Slowloris

A peer that opens streams and never finishes a stream header holds at most `MAX_STREAMS_PENDING_CLASSIFY` (4) slots per direction on one connection, each for at most `CLASSIFY_TIMEOUT` (5 seconds) ([Reading a stream header](#reading-a-stream-header)).
Further streams wait unread, holding credit, so the attack spends the peer's stream credit, not the receiver's work.

### Silent peer

A peer can open streams and then neither send nor read.
Every waiting stream is an open QUIC stream holding credit, so the incoming stream limit and the windows bound them: under `Interop`, 256 streams per direction, 10 MiB per stream and 15 MiB per connection ([QUIC and TLS](01-quic-and-tls.md)).

### Unannounced protocols

A stream for a protocol the receiver does not share is reset before any protocol sees it ([Routing streams](#routing-streams)), so a peer cannot reach a protocol it did not announce.

### A protocol that stops reading

Its unread streams hold credit that all protocols on the connection share, so it can starve the others; the rule to end every held stream exists for this reason ([Ending a stream](#ending-a-stream)).

### Misreading `UnsupportedProtocol`

A peer that treats it as a violation on a shared protocol punishes a node that merely stopped serving that protocol.

## Rationale

### Numbers instead of names

A stream names its protocol with no round trip.
A peer announces its protocol ids once, in its `Hello`, so a stream carries only one small number and opening one costs nothing more.
Per-stream negotiation, as in multistream-select, costs a round trip on every stream, and broadcast opens thousands of streams per slot.

### One header, two layers

The stack routes and bounds protocols, and a protocol classifies its own streams.
With one field on the wire, the shared protocols are the intersection of two lists, and everything below the protocol — broadcast's kinds, for example — needs no stack registration, no announcement and no stack-imposed bound of its own.
The stack cannot impose per-kind queues whose relative priority it cannot know; a protocol that wants its own streams ordered drains them itself, with full knowledge of what they carry.

### A bare stream header

A reader finds where the header ends without decoding it.
A uvarint marks its own end, so a length prefix in front of the header would delimit nothing; it would add a byte and a second encoding to reject.
The one thing a prefix did was keep ethp2p streams apart from libp2p streams, and reserving protocol 19 does that instead ([libp2p compatibility](04-libp2p-compat.md)).

### Typed frames

A reader knows what each frame carries without decoding its payload, so it can reject a frame out of order before it parses it.
QUIC and HTTP/3 type their frames for the same reason.
The alternative, fixing each frame's message by its position and wrapping choices in a protobuf `oneof`, made every protocol define its own way to tell messages apart.

## Design notes

### No per-protocol limits

This is a known limit, not a goal.
Backpressure comes only from QUIC stream credit, which all protocols on a connection share, so one protocol that opens many streams can use up the pool and starve the others.

## Reference implementation

Package `wire` encodes stream headers and frames, package `transport` holds unread streams, and package `ethp2p` reads stream headers and routes streams to protocols ([wire docs], [transport docs], [ethp2p docs]).
Tests that check this spec: `wire/wire_test.go`, `transport/credit_test.go`, `transport/uni_backpressure_test.go`, and `header_test.go`, `control_test.go`, `connection_test.go`, `delivery_test.go` at the repository root.
The byte examples were produced with `wire.AppendSelector` and `wire.AppendFrame`, and the rejected inputs were checked against `wire.ParseSelector` and `wire.ReadFrame`.

[RFC 9000]: https://www.rfc-editor.org/rfc/rfc9000
[RFC 9000 frames]: https://www.rfc-editor.org/rfc/rfc9000#section-12.4
[RFC 9114 extensions]: https://www.rfc-editor.org/rfc/rfc9114#section-9
[RFC 9170]: https://www.rfc-editor.org/rfc/rfc9170
[wire docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p/wire
[transport docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p/transport
[ethp2p docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p
