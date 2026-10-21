---
status: implemented
requires:
  - ../01-transport/02-streams.md
---

# Registries

Every number and name the specs assign, in one place.
Each table links to the spec that owns its entries; that spec defines what an entry means, and this page only collects them so that two specs never assign the same value.
A spec that assigns a new value adds it here.

## Application protocol names

The names a TLS handshake negotiates ([QUIC and TLS](../01-transport/01-quic-and-tls.md)).

| Name | Meaning | Spec |
| ---- | ------- | ---- |
| `ethp2p_0` | An ethp2p view and a libp2p view on one connection | [libp2p compatibility](../01-transport/04-libp2p-compat.md) |
| `libp2p` | A libp2p connection with no ethp2p view | [libp2p compatibility](../01-transport/04-libp2p-compat.md) |

## Protocols

The protocol id that is the stream header ([Streams](../01-transport/02-streams.md#stream-header)).
Protocols are the only registered, announced and negotiated unit.

| Id | Protocol | Spec |
| --: | -------- | ---- |
| 0 | control | [Control protocol](../05-control/01-control.md) |
| 1 | broadcast | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| 19 | reserved: `0x13` begins every libp2p stream | [libp2p compatibility](../01-transport/04-libp2p-compat.md) |

## Kinds and frame types

The broadcast protocol's kinds, written after the protocol header on every broadcast stream and datagram, and the frame types of each kind, numbered within each kind ([Streams](../01-transport/02-streams.md#frames)).
This table is owned by the broadcast protocol.
The control protocol defines no kinds; its frame types are `HELLO` (0) and `GOAWAY` (1) ([Control protocol](../05-control/01-control.md)).

| Protocol | Kind | Number | Frame types | Spec |
| -------- | ---- | -----: | ----------- | ---- |
| broadcast (1) | control | 1 | `HANDSHAKE` (0), `SUBSCRIBE` (1), `UNSUBSCRIBE` (2) | [Broadcast protocol](../03-broadcast/02-broadcast-protocol.md#control-kind) |
| broadcast (1) | session | 2 | `OPEN` (0), `UPDATE` (1) | [Broadcast protocol](../03-broadcast/02-broadcast-protocol.md#session-kind) |
| broadcast (1) | chunk | 3 | `HEADER` (0), `DATA` (1) | [Broadcast protocol](../03-broadcast/02-broadcast-protocol.md#chunk-kind) |

## Outcome codes

Stack codes are listed in [Outcome codes](06-outcome-codes.md).
Protocol codes are listed by the spec of the protocol that owns the stream; broadcast's are in [Broadcast framework](../03-broadcast/01-broadcast-framework.md#wire-format).
