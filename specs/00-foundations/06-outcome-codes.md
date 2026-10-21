---
status: implemented
requires:
  - ../01-transport/01-quic-and-tls.md
---

# Outcome codes

A physical connection is shared between protocol stacks, and streams belong to protocols, so a peer needs to tell "unsupported", "refused", "timed out" and "done" apart.
Every reset, `GOAWAY` and connection close that ethp2p sends therefore carries an outcome code: a QUIC application error code with a defined layout.
This document defines the code space, the stack codes, and how receivers and protocol authors use codes.
Notation and the meaning of MUST and SHOULD are in [Conventions](02-conventions.md); defined terms are in [Terminology](03-terminology.md).

## Goals

- A peer can tell why a stream, a view or a connection ended.
- Every code fits in one byte.
- A new code never breaks an older peer.

## Wire format

An outcome code is a QUIC application error code with this layout:

```text
wire_code = value << 1 | namespace   # namespace 0: stack, 1: protocol
```

A wire code is a QUIC variable-length integer.
A value below 32 encodes in one byte and a value below 8192 in two.

| Signal | Carries |
| ------ | ------- |
| `GOAWAY` on the control stream | The stack value alone: the wire code shifted right by one. |
| `RESET_STREAM` or `STOP_SENDING` | A full wire code, either namespace. |
| Application `CONNECTION_CLOSE` | A full wire code; the reason string is the code's name. |

A protocol code is interpreted by the protocol that owns the stream it resets, as named by its stream header; the protocol is not encoded, because both endpoints already know it.
Protocol code `0` means "no protocol-specific reason".

Stack codes (namespace 0):

| Value | Wire code | Name | Sent by | Meaning |
| ----: | --------: | ---- | ------- | ------- |
| 0 | 0 | `Unspecified` | anyone | No specific reason, including ordinary cancellation. |
| 1 | 2 | `Refused` | anyone | The receiver will not process this stream. |
| 2 | 4 | `Timeout` | anyone | A deadline expired. |
| 3-7 | 6-14 | unassigned | none | |
| 8 | 16 | `BadSelector` | stack | The stream header was invalid. |
| 9 | 18 | `UnsupportedProtocol` | stack | The stream's protocol is not shared on this view. |
| 10 | 20 | `Closing` | stack | The view is closing. |
| 11 | 22 | `ControlViolation` | stack | The control stream protocol was violated. |
| 12 | 24 | `NoSharedProtocols` | stack | No protocol other than control is shared on the view. |
| 13 | 26 | `Duplicate` | stack | Another connection to the same peer was kept. |
| 14-31 | 28-62 | reserved | none | Future stack codes, each one byte. |

Application codes of a closing physical connection that are not stack codes:

| Code | Meaning |
| ---: | ------- |
| 0 | Normal release by libp2p, or a connection nobody claims. |
| 1 | Generic failure, or a connection nobody claims because its delivery queue is full. |

Examples: `ControlViolation` is value 11, wire code 22 (`0x16`).
The same value 10 is wire code 20 in the stack namespace and 21 in the protocol namespace.

## Behavior

1. A receiver MUST read a stack value that is unassigned or unknown, on a stream reset, as `Unspecified`.
   An unknown value in a `CONNECTION_CLOSE` is a plain connection error.
2. A stream or view rejected because a local queue or budget is full MUST be reset or closed with `Unspecified`.
   There is no overload code, so the peer does not learn the receiver's load.
3. Protocols SHOULD use the shared stack codes `Refused` and `Timeout` instead of defining equivalents, so senders handle refusal and timeouts uniformly.
   Values 0 to 2 are shared: either endpoint MAY send them on any stream.
   Values 8 to 31 describe stack outcomes and SHOULD be used only for those.
4. The stack MUST preserve every received protocol value and hand it to the protocol that owns the
   stream, which applies its own fallback for values it does not define.
5. A protocol specification SHOULD:
   - give the most frequent outcomes the lowest values, so they encode in one byte;
   - keep its code set below 32 values, without sparse, bit-field or category-range layouts;
   - never give value `0` a meaning;
   - use codes for why a stream ended, not for data,
     and send a frame before FIN when the peer needs a count, an identifier or a position;
   - publish its code table, add values only at the end, and never renumber or reuse a value.
6. The code space is extended in place.
   New stack codes take the reserved values 14 to 31 or the unassigned values 3 to 7.
   A protocol adds protocol codes at the end of its own table.

Each protocol defines its code values in its own specification, for example broadcast's table
([Broadcast framework](../03-broadcast/01-broadcast-framework.md#wire-format)).

## Failure modes

- **Unknown value.**
  A new stack code never breaks an older peer: it reads as `Unspecified`.
- **Load probing.**
  Because overflow carries `Unspecified`, a peer cannot learn the receiver's load from resets.
- **Wrong namespace.**
  `GOAWAY` carries a stack value only; a protocol-namespace code would lose its namespace in the
  shift, so the sender of `GOAWAY` MUST pass a stack code ([Control protocol](../05-control/01-control.md)).

## Rationale

**Small codes.**
Keeping stack and protocol values below 32 keeps every code to one byte, and the guidance to protocol authors derives from this.

**The protocol is not encoded in a code.**
Both endpoints already know the protocol from the stream's header, so a protocol code needs only a value.

**No overload code.**
A code that said "a local queue or budget is full" would tell the peer how loaded the receiver is.

## Reference implementation

Package `wire` ([docs]).
Tests that check this spec: `wire/wire_test.go`.
The values were produced with `wire.Code.Wire` and `wire.ParseCode`.

[docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p/wire
