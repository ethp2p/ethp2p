---
status: proposed
requires:
  - 02-streams.md
  - ../05-control/01-control.md
---

# Datagrams

Some Ethereum messages are small, frequent, and lose their value quickly; attestations are the canonical example.
Streams are the wrong tool for them: each costs an open, a stream header, flow-control state and a close, and a lost packet stalls every byte behind it.
QUIC datagrams ([RFC 9221]) travel inside the encrypted, congestion-controlled connection but are never retransmitted and never block other data.
This document proposes extending stream routing to datagrams: a datagram starts with a protocol header, then a kind for protocols that define kinds, and the receiver hands the rest to the protocol the header names.
It defines no protocol that uses datagrams.
Nothing here is implemented: only the QUIC negotiation of datagram support exists in the code.
Notation and the meaning of MUST and SHOULD are in [Conventions](../00-foundations/02-conventions.md); defined terms are in [Terminology](../00-foundations/03-terminology.md).

## Wire format

Datagrams are unidirectional and unacknowledged and need no stream.
After the `Hello` exchange either endpoint may send one at any time, and no reply follows.

```text
datagram = protocol_header || [kind] || payload   # kind only for protocols that define kinds; payload runs to the end of the DATAGRAM frame and may be empty
```

| Field | Width | Constraint |
| ----- | ----- | ---------- |
| `protocol_header` | 1 to 5 bytes | MUST be a valid stream header: one protocol id ([Streams](02-streams.md#stream-header)) |
| `kind` | 1 or more bytes | Present only for protocols that define kinds; a minimal uvarint owned by that protocol |
| `payload` | rest of the DATAGRAM frame | owned by the protocol the protocol header names |

The stack MUST NOT read past the protocol header: reading the kind, where one exists, is the receiving protocol's work.
For protocol ids below 128 the protocol header is one byte.
No protobuf message is defined: announcing a protocol in `Hello` announces both its streams and its datagram use.

| Name | Value | Meaning |
| ---- | ----- | ------- |
| `max_datagram_frame_size` | QUIC transport parameter, non-zero | Both endpoints must send it for datagram routing to be available. |
| Protocol 0 | reserved | Control protocol; a datagram of protocol 0 has no consumer. |
| Minimum QUIC path MTU | 1200 bytes | Reference size for payloads, before packet overhead and the protocol header. |

Examples (bytes in hexadecimal):

```text
07 aa bb                 protocol 7 (no kinds), payload aa bb
01 03 de ad              protocol 1 (broadcast), kind 3 (chunk), payload de ad
ac 02                    protocol 300 (no kinds), empty payload
```

## Behavior

**Availability.**
An endpoint that supports ethp2p datagrams MUST send a non-zero `max_datagram_frame_size`.
Datagram routing is available on a connection only when both endpoints sent a non-zero value, and an endpoint MUST NOT send datagrams otherwise.
Support is negotiated once per physical connection, so an endpoint sharing a connection with libp2p MUST still send a non-zero value.
A datagram protocol MUST define its behavior when datagrams are unavailable: fall back to streams of the same protocol, or treat the peer as not supporting the protocol.

**Sending.**
A sender MUST NOT send a datagram before its `HELLO` exchange with the peer has completed, nor for a protocol that is not shared.
Implementations MUST expose the current maximum protocol payload size for a protocol on a connection; it changes as path MTU discovery progresses.
Sending is best-effort: QUIC MAY drop a datagram locally when its send queue is full or congestion control does not allow timely sending.
Datagrams are never acknowledged or retransmitted; a protocol that needs reliability or ordering MUST use streams.

**Size.**
A DATAGRAM frame is never fragmented.
A sender MUST NOT send a DATAGRAM frame larger than the peer's `max_datagram_frame_size`, or one that does not fit in a single QUIC packet on the current path.
A protocol SHOULD keep payloads within a packet at the 1200-byte minimum path MTU, minus packet overhead and the protocol header.
A protocol whose payloads can exceed that MUST define what it does with one that is too large.

**Receiving.**
For each received DATAGRAM frame the receiver MUST:

1. Discard it if the ethp2p side of the connection is closed.
2. Apply the per-peer receive limit and discard it if over the limit.
3. Parse the protocol header and discard the datagram if it is invalid.
4. Discard it if its protocol is 0 or not shared, or its protocol has no local datagram consumer.
5. Otherwise deliver the rest of the datagram to the protocol the protocol header names, identified with the connection it arrived on; that protocol reads its kind first, where it defines kinds.

Receiving a datagram MUST NOT close the connection, reset a stream or signal the peer, whatever the outcome.
Misbehavior detected from datagram contents is the protocol's concern.

**Before the `HELLO` exchange.**
A peer may send datagrams before this endpoint has read its `Hello`.
A receiver MUST NOT deliver a datagram before its own `Hello` exchange with that peer completes.
It MAY buffer a small, bounded number of them, counted against the receive limit, and process them when the exchange completes; otherwise it MUST discard them.

**No backpressure.**
A receiver MUST NOT apply backpressure to QUIC packet processing while waiting for a protocol to consume datagrams.
When a protocol's receive buffer is full, the receiver MUST discard datagrams for that protocol.
Discarding the arriving datagram or the oldest buffered one is an implementation choice; protocols whose messages age quickly SHOULD request discarding the oldest.

**Receive limits.**
A receiver MUST enforce a per-peer limit on received datagrams, counting datagrams and bytes, before parsing protocol headers or invoking protocols; a token bucket per connection suffices.
Implementations SHOULD let each datagram protocol request an additional per-protocol limit.
Datagrams over a limit MUST be discarded.
The reputation effect of sustained excess is out of scope.
Implementations SHOULD count discarded datagrams per connection and per reason: view closed, receive limit, invalid protocol header, protocol not shared or protocol not consumed, full protocol buffer, exchange not yet complete.

**Shared connections.**
All DATAGRAM frames on a shared connection belong to ethp2p, so the dispatcher does not classify them.
While the ethp2p side is closed the receiver discards datagrams and libp2p keeps using the connection.

**Shared numbering.**
Announcing a protocol in `Hello` announces every stream and datagram use it defines.
A protocol that later adds datagram payloads under an existing protocol id changes its wire contract and MUST version the change like any other, for example with a new protocol id or a version exchanged on one of its streams.

**Ordering.**
Datagrams are unordered with respect to each other and to stream data, including the control streams that carry `Hello`.

**Changes.**
Datagram routing adds no frame type or field to the control stream.
A change to the datagram framing ships with a new application protocol name.

## Failure modes

- **Flood.**
  A malicious peer can send datagrams at any rate with any content.
  The per-peer receive limit bounds the cost before any protocol code runs.
  Protocol header parsing is constant-time and allocation-free: the header is at most 5 bytes and is validated on the received buffer.
- **Slow protocol.**
  Protocol buffers are bounded and discard on overflow, so a slow protocol cannot stall QUIC.
- **Early datagrams.**
  Datagrams before the `HELLO` exchange are discarded or held in a small bounded buffer.
- **Malformed or unroutable datagrams.**
  They are discarded and never close the connection, so a peer cannot use datagrams to end another protocol's view.
- **Replay and duplicates.**
  Datagrams are protected by the connection's keys, so the sender is the authenticated peer, and QUIC prevents replay of packets.
  A protocol may still receive the same message more than once, for example from different peers; deduplication is the protocol's concern.
- **Disclosure.**
  Sizes and timing stay visible to an observer, so protocols carrying sensitive traffic SHOULD consider padding.
  Today the only exposure is the negotiation: every connection sends a non-zero `max_datagram_frame_size`, and no code reads the DATAGRAM frames a peer may send.

## Rationale

Routing by protocol header lets several protocols share a connection's datagram channel without interfering, and the stack never reads past that header: a protocol classifies its own datagrams by kind, where it defines kinds, just as it classifies its own streams.
Announcing a protocol in `Hello` announces both its stream and its datagram use, so there is no negotiation beyond QUIC's own transport parameter.

**Raw datagram passthrough was removed.** `SendDatagram`, `RecvDatagram` and `SupportsDatagrams` had no caller outside transport tests and sent raw payloads with no protocol header, bypassing this routing; commit `27f9937d` dropped them.
Datagrams stay negotiated (`EnableDatagrams`).

Open questions:

- **First consumer.**
  No protocol uses datagrams yet.
  The delivery API should be validated against the first consumer before it is fixed, so delivery stays deferred.
- **Priority.**
  QUIC defines no priority between datagrams and stream data.
  Whether ethp2p should favor datagrams, such as attestations, over bulk streams under congestion is open.
- **Stack use.**
  Protocol 0 datagrams are discarded; a future liveness probe could define them.
- **Maximum size discovery.**
  Whether polling the exposed maximum is enough, or protocols need notification when it changes, is open.

## Code

Nothing in this topic is implemented: no code sends or receives QUIC datagrams, and there is no datagram API, limit or counter.
Related code: `transport/config.go` enables datagrams in the QUIC configuration; `wire/wire.go` holds the stream header codec; `control/control.go` holds the control protocol id; `control/control.proto` holds `Hello`; `transport/endpoint.go` is the dispatcher.
`transport/listener_test.go` checks that both sides of a shared connection negotiate datagrams.

What a first implementer must know:

- Datagrams are already enabled, so every connection sends a non-zero `max_datagram_frame_size` and the peer may already send DATAGRAM frames that no code reads.
  Decide how received datagrams are drained and bounded before enabling any consumer.
- The control protocol is a constant in `wire`, and `Hello` validation rejects protocol 0, so protocol
  0 datagrams can be discarded without a new reservation.
- `wire.ParseSelector` parses a protocol header from a buffer without allocating; on a datagram, a header it reports incomplete is invalid.
- `Hello.protocols` (at most 1024, strictly ascending) is the shared set, so no new
  announcement field is needed.
- The dispatcher classifies streams only; a datagram reader would sit beside it, not inside it.
- Delivery is pull-based and the protocol's event queue carries streams only.
  A datagram consumer fits the same shape with a bounded per-protocol buffer that discards on overflow.

[RFC 9221]: https://www.rfc-editor.org/rfc/rfc9221
