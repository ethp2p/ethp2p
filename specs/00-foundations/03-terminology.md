---
status: implemented
requires: []
---

# Terminology

Every term the specs define, in one place.
Each entry gives a short definition and links to the spec that owns the concept.
A spec links here for a term and does not define it again.
A spec that introduces a new term adds an entry here.

## Identity and QUIC

| Term | Meaning | Spec |
| ---- | ------- | ---- |
| Identity key | the secp256k1 key pair that identifies a node. | [Identity](05-identity.md) |
| Peer ID | the binary form of the libp2p `peer.ID` of the identity key, not the base-encoded string. | [Identity](05-identity.md) |
| Signed key | the certificate extension that binds the identity key to the certificate key. | [Identity](05-identity.md) |
| Certificate key | the throwaway P-256 key pair whose public key the TLS certificate carries. | [QUIC and TLS](../01-transport/01-quic-and-tls.md) |
| Application protocol name | the ALPN identifier the TLS handshake negotiates. ethp2p uses `ethp2p_0` and `libp2p`. | [QUIC and TLS](../01-transport/01-quic-and-tls.md) |
| QUIC frame | a unit of a QUIC packet, such as `STREAM` or `CONNECTION_CLOSE` ([RFC 9000]). The specs always say "QUIC frame" or name its type; "frame" alone means an ethp2p frame. | [QUIC and TLS](../01-transport/01-quic-and-tls.md) |
| Stream credit | the number of streams of one kind a peer may still open, as limited by the receiver's `MAX_STREAMS` QUIC frames. | [QUIC and TLS](../01-transport/01-quic-and-tls.md) |
| Profile | the QUIC parameters that vary by deployment: stream limits, receive windows, keep-alive, idle timeout, path MTU discovery. | [QUIC and TLS](../01-transport/01-quic-and-tls.md) |
| Outcome code | a QUIC application error code that states why a stream, a view or a connection ended: a value plus a one-bit namespace. | [Outcome codes](06-outcome-codes.md) |
| Stack code | an outcome code in the stack namespace, for example `Closing` or `Refused`. | [Outcome codes](06-outcome-codes.md) |
| Protocol code | an outcome code in the protocol namespace; the protocol that owns the stream gives it meaning. | [Outcome codes](06-outcome-codes.md) |

## Protocols and connections

| Term | Meaning | Spec |
| ---- | ------- | ---- |
| View | one stack's use of a QUIC connection. An `ethp2p_0` connection has one ethp2p view and one libp2p view, and closes when both are closed. | [libp2p compatibility](../01-transport/04-libp2p-compat.md) |
| Stream header | the byte or bytes that begin every ethp2p stream: its protocol id, one uvarint. Example: `01` begins a broadcast stream. | [Streams](../01-transport/02-streams.md) |
| Selector | one protocol id, which a stream header carries. Example: 1 is broadcast. | [Streams](../01-transport/02-streams.md) |
| Protocol id | the number that names a protocol on the wire, in stream headers and in `Hello.protocols`. Id 19 is reserved. Example: broadcast is protocol 1. | [Streams](../01-transport/02-streams.md) |
| Protocol | the wire behavior of the streams with one protocol id. Protocols are the only registered, announced and negotiated unit. | [Streams](../01-transport/02-streams.md) |
| Kind | a protocol's own number naming what a stream or datagram carries, written after the protocol header. Example: broadcast kind 3 is chunk. | [Broadcast protocol](../03-broadcast/02-broadcast-protocol.md) |
| Frame | `uvarint(type) \|\| prefixed(payload)`: the unit a reader delimits on a stream after the stream header. Example: a `Hello` travels in one `HELLO` frame. | [Streams](../01-transport/02-streams.md) |
| Frame type | the uvarint at the start of a frame that names what its payload carries, numbered within each protocol. | [Streams](../01-transport/02-streams.md) |
| Payload limit | the largest frame payload a protocol accepts, such as `MAX_CONTROL_FRAME`. | [Streams](../01-transport/02-streams.md) |
| `Hello` | the message of the first control frame, `HELLO`; it lists the protocols the sender serves and may carry its node record. | [Control protocol](../05-control/01-control.md) |
| `GoAway` | the message of `GOAWAY`, the only control frame allowed after `HELLO`; it states why the sender is closing its view. | [Control protocol](../05-control/01-control.md) |
| Control violation | any departure from the control protocol; the receiver closes the view with `ControlViolation`. | [Control protocol](../05-control/01-control.md) |
| Node record | a signed [EIP-778] record; ethp2p reads the peer's QUIC endpoints from it. | [Control protocol](../05-control/01-control.md) |
| Duplicate | a second view between the same two peers; one of the two closes with `Duplicate`. | [Connection lifecycle](../01-transport/03-connection-lifecycle.md) |
| Classification | reading the first byte of an incoming stream to decide whether ethp2p or libp2p owns it, and the stream header of an ethp2p stream. | [libp2p compatibility](../01-transport/04-libp2p-compat.md) |
| Legacy peer | a peer that negotiated `libp2p` instead of `ethp2p_0`. Its connection has no ethp2p view. | [libp2p compatibility](../01-transport/04-libp2p-compat.md) |
| Datagram | the payload of one QUIC DATAGRAM frame that carries ethp2p traffic. | [Datagrams](../01-transport/05-datagrams.md) |

## Broadcast

| Term | Meaning | Spec |
| ---- | ------- | ---- |
| Channel | an interest shared by peers, bound to one broadcastable object type and one coding scheme. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Object | an application object published to a channel, identified by `(channel, object_id)`. Example: one execution payload. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Session | the activity for one object on one channel at one node. It owns the strategy, the participating peers and the buffered work. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Chunk | a unit produced by the coding scheme; its `HEADER` frame carries a strategy-specific identifier and its `DATA` frame carries its bytes. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Preamble | session-scoped, scheme-specific metadata the origin sends before any chunk. A relay builds its strategy from it. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Routing update | scheme-specific state a peer advertises about the chunks it holds. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Reconstruction | recovering the object from enough valid chunks and delivering it. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Origin | the session role of the node that published the object. It never accepts chunks for it. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Relay | the session role of a node that received a session open for an unknown object. It forwards work before reconstructing and keeps serving afterwards. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Subscriber | the application that receives a channel's reconstructed objects. A property of the channel, not of a session. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Binding | the state broadcast keeps for one peer from peer-up to peer-down. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Strategy | the per-session logic of a coding scheme: encoding, decoding, verification and the policy for what to send to whom. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Verdict | the framework's classification of each processed chunk. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Dedup handle | the handle with which a strategy cancels in-flight duplicate reads of one chunk. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |
| Parked stream | an unread inbound chunk stream that arrived before its session. | [Broadcast protocol](../03-broadcast/02-broadcast-protocol.md#chunk-kind) |
| Observer | a local sink for broadcast events; not on the wire. | [Broadcast framework](../03-broadcast/01-broadcast-framework.md) |

## Reed-Solomon

| Term | Meaning | Spec |
| ---- | ------- | ---- |
| Shard | an RS chunk with an index from 0 to n-1; indices below k are data, the rest parity. | [Reed-Solomon](../03-broadcast/05-reed-solomon.md) |
| Bitmap | the RS routing update; bit i is set when the sender holds shard i. | [Reed-Solomon](../03-broadcast/05-reed-solomon.md) |
| Allocation count | per shard, the sends started and not undone; the shard with the lowest count is served first. | [Reed-Solomon](../03-broadcast/05-reed-solomon.md) |
| Forward count | per shard, the confirmed successful sends by a relay, bounded by the forward multiplier. | [Reed-Solomon](../03-broadcast/05-reed-solomon.md) |
| Forward multiplier | the configuration value that bounds every shard's forward count on a relay. | [Reed-Solomon](../03-broadcast/05-reed-solomon.md) |

## RLNC

| Term | Meaning | Spec |
| ---- | ------- | ---- |
| Generation | a partition of the message that is coded independently. | [RLNC](../03-broadcast/06-rlnc.md) |
| Basis | the linearly independent chunks a node holds for a generation, in echelon form. | [RLNC](../03-broadcast/06-rlnc.md) |
| Coefficient vector | per chunk, the scalars recording the combination that produced it; its length is the generation size. | [RLNC](../03-broadcast/06-rlnc.md) |
| Rank | the size of a node's basis for a generation; at the generation size it decodes. | [RLNC](../03-broadcast/06-rlnc.md) |
| Recoding | producing a new random combination from a generation's basis. | [RLNC](../03-broadcast/06-rlnc.md) |
| Have-list | the RLNC routing update: the sender's rank for each generation it has progressed in. | [RLNC](../03-broadcast/06-rlnc.md) |

[EIP-778]: https://eips.ethereum.org/EIPS/eip-778
