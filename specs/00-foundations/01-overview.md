---
status: implemented
requires: []
---

# Overview

ethp2p is a peer-to-peer networking stack purpose-built for Ethereum.
It runs on QUIC beside libp2p, on the same UDP socket and the same connections, and carries Ethereum's broadcast traffic with erasure coding.
This document names the layers and the principles the other specs follow.
Notation and the meaning of MUST and SHOULD are in [Conventions](02-conventions.md); defined terms are in [Terminology](03-terminology.md).

## Layers

```text
broadcast            channels, sessions, chunks          03-broadcast
control protocol     HELLO, GOAWAY                       05-control/01-control
streams              stream headers, protocols, frames   01-transport/02-streams
QUIC and TLS         streams, credit, identity           01-transport/01-quic-and-tls
UDP socket           shared with libp2p                  01-transport/04-libp2p-compat
```

A connection starts with a QUIC handshake that authenticates both peers by their identity keys ([Identity](05-identity.md)).
Each peer then sends `HELLO` on its control stream, listing the protocols it serves.
From then on every stream names its protocol in its stream header, and every reset and close carries an outcome code ([Outcome codes](06-outcome-codes.md)).
[Connection lifecycle](../01-transport/03-connection-lifecycle.md) follows one connection through these steps.

## Principles

**QUIC-native.**
The design uses QUIC's streams, credit and connection close directly and does not abstract over transports.
Each layer knows what the layer below does.

**No per-stream negotiation.**
A peer announces the protocols it serves once, and each stream carries only one number, so opening a stream costs no round trip.
That matters at thousands of streams per slot.

**Object-specific broadcast.**
The broadcast protocol owns the propagation of broadcast objects, and its coding scheme is a pluggable strategy.

**Explicit outcomes.**
Every reset and close carries a code in a defined namespace, so a peer can tell "unsupported", "refused", "timed out" and "done" apart.

**Failure stays contained.**
A routine ethp2p failure on a connection does not disconnect libp2p traffic on it, and one protocol's failure does not end a peer's other protocols.
