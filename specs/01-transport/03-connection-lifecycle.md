---
status: implemented
requires:
  - 01-quic-and-tls.md
  - 02-streams.md
  - ../05-control/01-control.md
---

# Connection lifecycle

This document follows an ethp2p view from the handshake to its close, in the order a peer observes.
Each phase links to the spec that owns its messages; this document owns the two rules no other spec does: which view survives when two peers hold two, and how a close is carried and read.
How the view shares its QUIC connection with libp2p is in [libp2p compatibility](04-libp2p-compat.md).
Notation and the meaning of MUST and SHOULD are in [Conventions](../00-foundations/02-conventions.md); defined terms are in [Terminology](../00-foundations/03-terminology.md).

## Goals

- Two peers keep at most one ethp2p view between them, and both agree which one.
- A peer learns why a view closed, whichever signal reaches it first.
- A view that fails its checks never displaces a working one.

## Wire format

```text
dialer                                      listener
  |== QUIC handshake, ALPN ethp2p_0, TLS ==|   QUIC and TLS
  |-- 00, HELLO -------------------------->|   Control protocol
  |<-------------------------- 00, HELLO --|   Control protocol
  |   records checked, protocols matched   |   Control protocol, Streams
  |   duplicate rule                       |   this document
  |<========== protocol streams ==========>|   Streams
  |-- GOAWAY(code), FIN ------------------>|   Control protocol
  |-- CONNECTION_CLOSE(code, name) ------->|   this document
```

**Connection close.**
When ethp2p closes a QUIC connection, the `CONNECTION_CLOSE` frame (type `0x1d`) carries the view's stack code as its application error code and the code's name as its reason phrase ([Outcome codes](../00-foundations/06-outcome-codes.md)).

For example, a view closed as a duplicate, with nothing else using the connection, ends it with error code 26 (`Duplicate`, value 13) and reason phrase `Duplicate`.

## Behavior

**Phases.**
A view passes through these phases in order:

1. Handshake: QUIC and TLS authenticate both peers ([QUIC and TLS](01-quic-and-tls.md)).
2. `HELLO`: each peer sends its own and accepts the other's within `HELLO_TIMEOUT` (5 seconds) ([Control protocol](../05-control/01-control.md)).
3. Checks: the record must name the authenticated peer, or the view closes with `ControlViolation`; at least one protocol other than control must be shared, or it closes with `NoSharedProtocols` ([Streams](02-streams.md#shared-protocols)).
4. Duplicates: the rule below decides whether this view or an existing one survives.
5. Open: protocols use streams ([Streams](02-streams.md)).
6. Close: `GOAWAY` and FIN, then, when nothing else uses the connection, `CONNECTION_CLOSE`.

A node MUST run the checks of phase 3 before the duplicate rule, so a view that fails them never displaces a working one.
A node MAY close a view at any phase with `Closing`, for example when it shuts down.

**Duplicates.**
A node keeps at most one view per peer.
When a view passes its checks while another view to the same peer exists:

- If the two were dialed in opposite directions, the view dialed by the peer with the lower peer ID, compared byte-wise, is kept.
- If both were dialed in the same direction, the newer view is kept.

The other view MUST be closed with `Duplicate`.
Both peers MUST apply the same rule; it is not negotiated, and peers that applied different rules could each keep a different view.

**Closing the connection.**
When the last user of a connection is an ethp2p view that closes, the node MUST close the connection with the view's stack code, as above.
It MUST close the connection before it resets streams no protocol has read, so the resets do not hand the peer credit to open more ([QUIC and TLS](01-quic-and-tls.md)).

**Receiving a connection close.**
An application close whose error code is an even wire code of a defined stack code closes the view with that code, as if `GOAWAY` had arrived.
Any other application code, a transport error, a stateless reset and an idle timeout end the view as plain connection loss: `Timeout` for an idle timeout and `Unspecified` otherwise.
A close reaching a peer that already received `GOAWAY` does not change the code `GOAWAY` gave.

**Changes.**
The duplicate rule cannot change in place: peers on different rules keep different views.
A change ships with a new application protocol name ([QUIC and TLS](01-quic-and-tls.md)).

## Failure modes

**Simultaneous dials.**
Two peers that dial each other at once both apply the opposite-direction rule and keep the same view, so they converge on one connection.

**Re-dial churn.**
A peer that keeps re-dialing in the same direction replaces its own view each time and cannot add views, so the cost is one view's setup per handshake.

**Stale view after a restart.**
If the higher-ID peer restarts and re-dials a peer that had dialed it, the old view, dialed by the lower ID, wins until a stateless reset or the idle timeout ends it.
A reset key derived from the identity key makes the restarted peer reset the old connection within one round trip ([QUIC and TLS](01-quic-and-tls.md)).

**Bad `HELLO`.**
A view whose record or protocols fail closes before the duplicate rule, so a peer cannot evict another view, its own or another peer's, with a bad `Hello`.

**Forged codes.**
A connection close with an odd or unknown code reads as plain connection loss, so a peer cannot make a view close with a code the receiver did not define.

**Disclosure.**
The reason phrase is the code's name, never a local detail.

## Rationale

**One rule both sides compute.**
To keep one view and agree on which, both peers need the same answer from the same facts.
Keeping the first view to arrive locally fails: two simultaneous dials arrive in opposite orders at the two ends.
The dialer's peer ID is known to both, so preferring the view dialed by the lower ID gives one answer.

**Newer wins in the same direction.**
Two views dialed by the same peer have no shared fact to order them: connection IDs are local.
Letting the newer one win handles the common case, a restarted peer re-dialing, at the cost of the churn above.

**The code in `CONNECTION_CLOSE`.**
A `GOAWAY` is best-effort: the peer may close the connection before it is read.
Carrying the same code in `CONNECTION_CLOSE` lets a peer that never read `GOAWAY` still learn the reason.
Only an even, defined code counts, because libp2p closes with 0 and 1 for its own reasons.

**Close before resetting.**
A reset returns stream credit at once; closing first denies the peer credit to open more.

## Reference implementation

Package `transport` closes and reads connection closes, and package `ethp2p` runs `HELLO` and `GOAWAY` and applies the duplicate rule ([transport docs], [ethp2p docs]).
Tests that check this spec: `transport/lifecycle_test.go`, and `control_test.go`, `connection_test.go` at the repository root.

[transport docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p/transport
[ethp2p docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p
