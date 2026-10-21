---
status: implemented
requires:
  - ../01-transport/02-streams.md
  - ../00-foundations/05-identity.md
  - ../00-foundations/06-outcome-codes.md
---

# Control protocol

This document defines the control protocol, protocol 0, which runs on every ethp2p view.
Each peer sends `HELLO` once, announcing the protocols it serves and optionally its node record, and may send `GOAWAY` once, stating why it is closing the view.
It builds on [Streams](../01-transport/02-streams.md), which defines stream headers, frames and what the protocols in `Hello` mean; where `HELLO` and closing fit in a connection's life is in [Connection lifecycle](../01-transport/03-connection-lifecycle.md).
Notation and the meaning of MUST and SHOULD are in [Conventions](../00-foundations/02-conventions.md); defined terms are in [Terminology](../00-foundations/03-terminology.md).

## Goals

- Each peer learns the protocols the other serves from one message, before any protocol stream is used.
- Each peer can tell the other why it is leaving, in a form the other can read.
- A peer that sends nothing, or too much, costs bounded time and memory.

## Wire format

### Control stream

Each peer opens one unidirectional stream with stream header `00` (protocol 0) on each view and writes to it; it reads the one its peer opens.
Two unidirectional streams, not one bidirectional stream, let both peers open at once with no rule to break the tie.

```text
control_stream = stream_header(0) || HELLO || [GOAWAY]   # then FIN
```

Frames follow [Frames](../01-transport/02-streams.md#frames), with a payload limit of `MAX_CONTROL_FRAME` (16384), so a frame length takes one to three bytes.

| Type | Name | Payload | Allowed |
| ---: | ---- | ------- | ------- |
| 0 | `HELLO` | `Hello` | First frame, exactly once |
| 1 | `GOAWAY` | `GoAway` | After `HELLO`, at most once, last frame |

From `control/control.proto`:

```protobuf
syntax = "proto3";
package ethp2p.control;

message Hello {
  // Protocols this endpoint serves besides control: strictly ascending, at most
  // 1024, neither 0 (control) nor 19 (reserved).
  repeated uint32 protocols = 1;
  // Optional: the sender's current node record (EIP-778 RLP encoding).
  bytes record = 2;
}

message GoAway {
  // A stack-namespace code value.
  uint64 code = 1;
}
```

`Hello` and `GoAway` have no fields other than those above.

### `HELLO`

`HELLO` announces the protocols the sender serves and, optionally, its node record.

| Field | Constraint |
| ----- | ---------- |
| `Hello.protocols` | Strictly ascending, neither 0 nor 19, at most `MAX_PROTOCOLS` (1024) entries ([Streams](../01-transport/02-streams.md#announcing-protocols)). |
| `Hello.record` | Empty, or one node record of at most `MAX_RECORD_SIZE` (300) bytes. |

### `GOAWAY`

`GOAWAY` states why the sender is closing its view.

| Field | Constraint |
| ----- | ---------- |
| `GoAway.code` | A stack code value, not its wire code; `ControlViolation` is 11, not 22. |

### Node record

The `Hello.record` field is an [EIP-778] record with the `v4` identity scheme, in its RLP encoding.
It tells the peer where the sender can be reached.
No key is specific to ethp2p; it reads the ones libp2p nodes already publish:

| Key | Value |
| --- | ----- |
| `id` | `v4` |
| `secp256k1` | The 33-byte compressed identity public key ([Identity](../00-foundations/05-identity.md)) |
| `ip`, `quic` | IPv4 address (4 bytes) and its UDP port |
| `ip6`, `quic6` | IPv6 address (16 bytes) and its UDP port |

### Reader states

| State | Permitted | Next state |
| ----- | --------- | ---------- |
| Awaiting `HELLO` | `HELLO` | Open |
| Open | `GOAWAY` | Closed |
| Open | FIN | Closed |

Anything else is a control violation ([Violations](#violations)).

### Examples

A control stream for a node that serves broadcast, closed with `ControlViolation`:

```text
00                       stream header: protocol 0 (control)
00                       type 0 (HELLO)
03                       length: 3 bytes follow
0a 01                    Hello field 1 (protocols, packed), length 1
01                       protocol 1
01                       type 1 (GOAWAY)
02                       length: 2 bytes follow
08 0b                    GoAway field 1 (code): 11, ControlViolation
```

A `HELLO` for protocol 1 with a 135-byte record is a 144-byte frame:

```text
00                       type 0 (HELLO)
8d 01                    length: 141 bytes follow
0a 01 01                 Hello field 1 (protocols, packed): 1
12 87 01                 Hello field 2 (record), length 135
f8 85 b8 40 ...          the record: an RLP list of 133 bytes, starting with the 64-byte signature
```

### Constants

| Name | Value |
| ---- | ----- |
| `MAX_CONTROL_FRAME` | 16384 bytes |
| `MAX_PROTOCOLS` | 1024 |
| `MAX_RECORD_SIZE` | 300 bytes |
| `HELLO_TIMEOUT` | 5 seconds |
| `GOAWAY_TIMEOUT` | 1 second |

## Behavior

### Opening

A peer MUST open its control stream as its first unidirectional stream on the view.
The control stream holds one of the peer's incoming unidirectional slots for the life of the view, so a limit of N leaves N-1 for protocols.
A peer SHOULD send `HELLO` before opening any other stream; streams that arrive first wait, holding credit, until the `Hello` is accepted ([Streams](../01-transport/02-streams.md#unread-streams)).

### Sending `HELLO`

A peer MUST send exactly one `HELLO`, as its first control frame, and MUST NOT send one that breaks the constraints above.
The stream header and the `HELLO` MAY go in one write.
A node with no `Hello` to send MUST NOT dial a view, and MUST close a view it accepts with `Closing`.
A node that sends a record MUST sign it with its own identity key.

### Receiving `HELLO`

A peer MUST close the view with a control violation unless a valid `HELLO` arrives within `HELLO_TIMEOUT` (5 seconds) of the view's creation.
The deadline covers the arrival of the stream, its stream header and the frame.
A peer MAY finish writing its own `HELLO` before it reads the other's; that write counts against the same deadline.
A peer whose `HELLO` write has not finished by the deadline resets its control stream with `Timeout` and closes the view with `Timeout`.
A `HELLO` is at most `MAX_CONTROL_FRAME` (16384) bytes, so its write blocks only when the other peer grants less stream or connection credit than that.
A peer MUST NOT let a protocol use the view until the `Hello` is accepted.

### Validating the record

A receiver MUST reject a non-empty record unless all of these hold, so a record that reaches the node is authentic and belongs to the peer:

1. It is at most `MAX_RECORD_SIZE` (300) bytes, checked before parsing.
2. It is canonical RLP with no trailing bytes, and a valid [EIP-778] record with `id` `v4`: a 64-byte low-S signature, a sequence number, and keys in strictly ascending order.
3. The signature verifies under its `secp256k1` key.
4. The peer ID derived from that key equals the peer ID the TLS handshake authenticated.

A rejected record is a control violation.
A receiver MUST skip an endpoint that is malformed, unspecified, multicast, an IPv4-mapped IPv6 address, or has a port of 0 or above 65535, and keeps the rest of the record.
The record adds endpoints and never decides identity.

### Closing a view

To close its view, a peer SHOULD send `GOAWAY` with a stack code, then FIN, and MUST finish both within `GOAWAY_TIMEOUT` (1 second), so a peer that stops reading cannot stall the close.
A peer whose `HELLO` was never written MUST NOT send `GOAWAY`, so the peer never reads a `GOAWAY` where it expects `HELLO`.
A peer MUST NOT send `GOAWAY` in reply to the other's closure; it sends FIN only.
The first cause of a closure is the one sent; later causes are ignored.
When a peer closes its view, it cancels reading the other's control stream with `ControlViolation` for a violation and `Closing` otherwise.

### Receiving a closure

A peer that receives `GOAWAY`, FIN, or a reset of the other's control stream MUST treat the other's view as closed and close its own.
It reads no further frames after `GOAWAY`.
The closure means:

| Signal | Code |
| ------ | ---- |
| `GOAWAY` with code `v` | Stack code `v`; a value that is not a defined stack code, or does not fit in 63 bits, is `Unspecified`. |
| FIN after `HELLO`, no `GOAWAY` | `Unspecified` |
| Reset with a stack code | That code; an unknown value is `Unspecified`. |
| Reset with a protocol code | A control violation. |

Whichever of these and a connection close arrives first decides the code ([Connection lifecycle](../01-transport/03-connection-lifecycle.md)).

### Violations

These are control violations:

1. No valid `HELLO` within `HELLO_TIMEOUT` (5 seconds).
2. A malformed frame ([Frames](../01-transport/02-streams.md#reading-a-frame)), including one over `MAX_CONTROL_FRAME`.
3. A first frame other than `HELLO`, or a second frame other than `GOAWAY`; an unknown type is one of these.
4. An invalid `Hello`, or a rejected record.
5. A `Hello` or `GoAway` with a field its message does not define.
6. A second control stream, or a bidirectional stream of protocol 0.
7. A reset of the control stream with a protocol code.
8. Any other read failure on the control stream, including FIN before `HELLO`.

On a violation a peer MUST send `GOAWAY` with `ControlViolation` and FIN, MUST cancel reading the other's control stream with `ControlViolation`, and MUST close its view.
It MUST NOT reset its own control stream, because a reset can make the other discard the `GOAWAY`.

### Not violations

The end of the QUIC connection (application close, transport error, stateless reset, idle timeout) is not a violation, even when it interrupts a read on the control stream.

### Changes

The control protocol has no extension points: no frame types to ignore, no reserved fields and no optional messages.
An unknown frame type, or a message with a field it does not define, is a violation.
A change ships with a new application protocol name in place of `ethp2p_0` ([QUIC and TLS](../01-transport/01-quic-and-tls.md)).
A record extends itself through new keys, which need no change here.

## Failure modes

### No `HELLO`

A peer that never sends `HELLO` holds a view for at most `HELLO_TIMEOUT` (5 seconds), and the streams it opens meanwhile are bounded by the stream limits.

### Large frames

Every frame length is checked against `MAX_CONTROL_FRAME` (16384) before memory is allocated, and a record against `MAX_RECORD_SIZE` (300) before it is parsed, so a peer cannot make the receiver allocate or parse more.

### Peer that stops reading

A closing peer stops writing after `GOAWAY_TIMEOUT` (1 second), so the other cannot stall its close.

### Foreign record

A record must name the identity TLS authenticated, so a peer cannot present another node's record as its own.

### Forged closure codes

An unknown `GOAWAY` code reads as `Unspecified`, so a peer cannot make a view close with a code the receiver did not define.
A violation ends the violator's own view and no other.

### Disclosure

The protocol list reveals which protocols the sender serves, and the record its reachable endpoints; both are visible only to the peer.

## Rationale

### Two unidirectional streams

To let both peers send `HELLO` at once, each writes its own stream.
A single bidirectional stream needs an opener, and two peers that open at once need a rule to discard one.

### No extension points

Tolerating unknown messages is the usual defence against ossification, but a new application protocol name already tells each peer which version the other speaks.
Tolerance would let a mismatched peer continue with a half-understood exchange, while a violation fails fast.
This departs from [RFC 9170]; it is safe because the only peers that reach the control protocol are the ones that selected `ethp2p_0`.

### No reset on a violation

To make sure the violator reads why it was dropped, the closing peer sends `GOAWAY` and FIN and cancels only the other's stream.
Resetting its own stream could make the other discard the `GOAWAY`.

### The record never decides identity

A signed record proves who made it, not that its sender holds the connection's TLS key; anyone can replay another node's record.
So identity comes from TLS, and the record must match it.

### RLP only

A record has one encoding, so a receiver can verify the signature over exactly the bytes it received.

## Reference implementation

Package `ethp2p` runs the control protocol and checks the record against the peer, and package `enr` decodes records ([ethp2p docs], [enr docs]).
Tests that check this spec: `control_test.go` and `connection_test.go` at the repository root, and `enr/enr_test.go`.
The examples were produced with `wire.AppendSelector`, `wire.AppendFrame` and the `pb` encoder; the record's first bytes come from `enr.Sign`.

[EIP-778]: https://eips.ethereum.org/EIPS/eip-778
[RFC 9170]: https://www.rfc-editor.org/rfc/rfc9170
[ethp2p docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p
[enr docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p/enr
