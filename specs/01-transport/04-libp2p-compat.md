---
status: implemented
requires:
  - 01-quic-and-tls.md
  - 02-streams.md
  - 03-connection-lifecycle.md
---

# libp2p compatibility

This document defines how ethp2p and libp2p share one QUIC connection: what each application protocol name means, how a receiver tells their streams apart, and when the shared connection closes and with which code.
It builds on [QUIC and TLS](01-quic-and-tls.md), [Streams](02-streams.md) and [Connection lifecycle](03-connection-lifecycle.md), which describe the ethp2p side alone.
Notation and the meaning of MUST and SHOULD are in [Conventions](../00-foundations/02-conventions.md); defined terms are in [Terminology](../00-foundations/03-terminology.md).

## Goals

- A stock libp2p peer that has never heard of ethp2p connects unchanged.
- Two nodes that both run ethp2p and libp2p use one QUIC connection for both.
- Each stack sees only its own streams, so neither can starve or reset the other's.
- A routine failure in one stack does not close the other's connection.

## Wire format

**Connection forms.**
The negotiated application protocol name decides the form of a connection:

| Negotiated name | Form | Streams |
| --------------- | ---- | ------- |
| `ethp2p_0` | An ethp2p view and a libp2p view on one connection | ethp2p and libp2p streams, told apart as below |
| `libp2p` | A libp2p connection, no ethp2p view (a legacy peer) | libp2p streams only |
| anything else | None | None; the listener closes with application code 1 |

**multistream-select.**
A libp2p stream is always bidirectional and opens with [multistream-select] negotiation.
Each message is `prefixed(name)`, where `name` is a protocol name ending in a newline.
The opener sends the header `/multistream/1.0.0\n`, then the protocol it wants; the other side echoes the protocol to accept it, or answers `na`.
Every libp2p stream therefore begins with the header's length, `0x13` (19):

```text
13                       length: 19 bytes follow
2f 6d 75 6c 74 69 73 74  "/multist"
72 65 61 6d 2f 31 2e 30  "ream/1.0"
2e 30 0a                 ".0\n"
```

**Stream header.**
An ethp2p stream begins with its stream header ([Streams](02-streams.md#stream-header)), whose first byte is the protocol id's first uvarint byte.
Protocol 19 is reserved, so no ethp2p stream header begins with `0x13`, and the first byte of a stream says which stack owns it.

**Connection close codes.**
Application codes a node puts in the `CONNECTION_CLOSE` of a shared connection:

| Code | Reason phrase | Sent when |
| ---: | ------------- | --------- |
| A stack code | The code's name | The ethp2p view closes last ([Connection lifecycle](03-connection-lifecycle.md)). |
| Whatever libp2p chose | Whatever libp2p chose | The libp2p view closes last. libp2p uses 0 for a normal close. |
| 0 | `unclaimed` | The node does not take the libp2p view or the legacy connection. |
| 1 | `delivery queue full` | The node cannot take the libp2p view or the legacy connection now. |
| 1 | `unsupported negotiated protocol` | The negotiated name is neither `ethp2p_0` nor `libp2p`. |
| 20 (`Closing`) | `listener closed` | libp2p stopped listening before taking the view. |

## Behavior

**Classifying a bidirectional stream.**
On an `ethp2p_0` connection, a receiver MUST apply these tests to each incoming bidirectional stream, in order:

1. If the first byte is `0x13`, the stream is libp2p; the receiver MUST hand it to libp2p with that byte unread, so libp2p reads an intact multistream-select header.
2. Otherwise the stream is ethp2p, and the receiver reads its stream header ([Streams](02-streams.md#reading-a-stream-header)).

The receiver does not check the rest of a libp2p stream's head; libp2p rejects a bad multistream-select header itself.
No libp2p stream is unidirectional, so every unidirectional stream on an `ethp2p_0` connection is ethp2p, and one that begins with `0x13` is rejected as protocol 19 with `BadSelector`.
Classification has bounds of its own, with the values of [Streams](02-streams.md#reading-a-stream-header): the first byte MUST arrive within `CLASSIFY_TIMEOUT` (5 seconds) of the stream's acceptance, and a receiver MUST NOT wait on more than `MAX_STREAMS_PENDING_CLASSIFY` (4) first bytes at once on one connection.
An ethp2p stream's header read starts after classification and has its own deadline and slots.
A libp2p stream that arrives after the libp2p view closed is reset with `Closing`.

**Waiting libp2p streams.**
A receiver MUST NOT reset a classified libp2p stream because libp2p has not taken it yet, so a stock libp2p peer sees the backpressure it would see without ethp2p.

**Closing a shared connection.**
Each view closes on its own.
The connection MUST stay open until both views are closed, and MUST then close with the code of the last view to close; closing it earlier tears down the other stack's streams.
While the libp2p view remains, an ethp2p view's close is carried only by `GoAway` and FIN ([Control protocol](../05-control/01-control.md)).
A node that runs no libp2p MUST close the libp2p view as soon as the connection forms, so the ethp2p view's close is the last and its code reaches the peer.

**Reading a libp2p close.**
A close with code 0 or 1 is libp2p's, not a stack code, and reads as plain connection loss ([Connection lifecycle](03-connection-lifecycle.md)).

**Legacy peers.**
A dialer that offered `ethp2p_0` and negotiated `libp2p` has a libp2p connection: it MUST NOT open ethp2p streams on it and hands it to libp2p.
A dialer that wants ethp2p with a peer MUST dial it as ethp2p, because classification is fixed at the handshake and a libp2p dial never offers `ethp2p_0`.

**Changes.**
A change to the connection forms or to classification ships with a new application protocol name in place of `ethp2p_0` ([QUIC and TLS](01-quic-and-tls.md)).

## Failure modes

**Misattribution.**
Ownership follows from the first byte alone, and a malformed ethp2p stream header is reset before any protocol sees it, so a peer cannot make one stack read the other's streams.
A stream that begins with `0x13` but is not valid multistream-select reaches libp2p, which resets it as it would without ethp2p.

**One stack's failure.**
An ethp2p control violation closes the ethp2p view; libp2p keeps the connection until it closes its own view, so healthy libp2p traffic continues.

**Idle libp2p streams.**
This is a known limit.
A go-libp2p node can open a stream lazily, sending the multistream-select header only with its first write, so the stream carries no first byte until then.
Such a stream still reaches the receiver if its opener sends on a later stream, because using a stream ID opens every lower one of its type ([RFC 9000, Section 2.1][RFC 9000 streams]).
It then holds a classification slot until a byte arrives or `CLASSIFY_TIMEOUT` (5 seconds) expires, when it is reset with `Timeout`; four such streams fill the bidirectional slots of one connection and delay that connection's later bidirectional streams by up to that timeout.
A stock go-libp2p receiver also resets a stream that does not finish negotiating, after its default negotiation timeout of 10 seconds, so an idle stream fails against either receiver; here it fails after 5 seconds instead of 10.

**Legacy peer loop.**
A dial that meets a libp2p-only peer ends that ethp2p dial; a node that retries without remembering the peer dials it as libp2p again each time ([Legacy peers](../09-roadmap/01-legacy-peers.md)).

## Rationale

**A reserved protocol instead of a prefix.**
To let a stock libp2p peer connect unchanged, ethp2p streams must be told apart from multistream-select streams by their first bytes.
Every multistream-select stream begins with `0x13`, so reserving protocol 19, whose stream header begins with the same byte, makes the first byte decide at the cost of one protocol id.
The alternative was to wrap the stream header in a length prefix, as multistream-select wraps its messages, and keep that length below 19; it cost a byte on every stream and gave a second encoding to reject.
The receiver checks only the first byte of a libp2p stream: libp2p validates the rest of its own header and resets a stream whose header is bad, so a second check would only duplicate it.

**One connection, two views.**
To use one connection per peer for both stacks, the connection is split rather than duplicated: one handshake, one congestion controller, one set of NAT mappings.
Each stack sees only its own streams, so neither can starve or reset the other's.

**Only the second close closes.**
A routine ethp2p failure must not disconnect healthy libp2p traffic on the same connection, and the reverse.
Closing the connection whenever either view closed would be simpler and would do exactly that.

**Classified libp2p streams wait.**
To keep a stock libp2p peer unchanged, a classified libp2p stream behaves as it would without ethp2p, and stock libp2p does not reset a stream because its accept queue is full.

## Reference implementation

Package `transport` ([docs]).
Tests that check this spec: `interop_test.go` (stock libp2p), `selector_test.go` (classification), `lifecycle_test.go` (view release, physical close), `transport_test.go` (legacy peer), all in `transport`.
The multistream-select header was produced with go-multistream v0.6.1.

[multistream-select]: https://github.com/multiformats/multistream-select
[RFC 9000 streams]: https://www.rfc-editor.org/rfc/rfc9000#section-2.1
[docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p/transport
