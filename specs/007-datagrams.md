# Datagram routing

Status: draft, not implemented.

## 0. Introduction

Some Ethereum messages are small, frequent, and lose their value quickly.
Attestations are the canonical example: a message that arrives late is worth little,
and a retransmission that arrives later still is worth less.
Streams are the wrong tool for this traffic.
Each stream costs an open, a selector, flow-control state, and a close,
and a lost packet stalls every byte behind it on that stream until the retransmission arrives.

QUIC datagrams ([RFC 9221](https://www.rfc-editor.org/rfc/rfc9221)) fit this traffic.
They are carried inside the encrypted, congestion-controlled QUIC connection,
but they are never retransmitted and never block other data.
A datagram is either delivered whole or not at all.

ethp2p already routes streams to protocols by selector.
This document extends the same routing to datagrams,
so that several protocols can share a connection's datagram channel without interfering with one
another.

## 1. About this document

This document specifies how ethp2p endpoints negotiate datagram support,
how a datagram identifies the protocol it belongs to, and how a receiver processes, limits,
and discards datagrams.
It does not define any protocol that uses datagrams.
Each such protocol specifies its own payloads under its own selector.

The document assumes the ethp2p selector frame and selector advertisement defined by the stack.
Where this document says "the stack", it means the ethp2p component that owns selector registration,
advertisement, and routing on a connection.

Sections 3 through 7 use RFC 2119 language such as MUST, SHOULD, and MAY.

## 2. Terminology

**Selector.**
The unsigned integer identifying an ethp2p protocol on the wire.
Selector `0` is reserved for the stack's control stream.
Selector `0x2f` is reserved because its one-byte encoding is `/`,
which the shared QUIC dispatcher routes to libp2p.

**Selector frame.**
An unsigned-varint length `n`, followed by the selector's minimal unsigned-varint encoding,
which occupies exactly `n` bytes.
Every ethp2p stream begins with one selector frame.

**Shared selectors.**
The intersection of the selectors both endpoints advertised on a connection.

**Datagram protocol.**
A protocol whose specification defines datagram payloads under its selector.
A protocol MAY use streams, datagrams, or both under a single selector.

## 3. Negotiation

### 3.1. QUIC support

An endpoint that supports ethp2p datagrams MUST send a non-zero `max_datagram_frame_size` transport
parameter.
Datagram routing is available on a connection only when both endpoints sent a non-zero value.
An endpoint MUST NOT send datagrams on a connection where the peer did not.

A datagram protocol MUST define its behavior when datagrams are unavailable on a connection.
It MAY fall back to streams under the same selector,
or treat the peer as not supporting the protocol.

### 3.2. Selector namespace

Streams and datagrams share one selector namespace.
There is no separate datagram advertisement.
Advertising a selector advertises support for every stream and datagram usage
that the protocol's specification defines under that selector.

A protocol that later adds datagram payloads under an existing selector changes
that protocol's wire contract.
It MUST version that change the same way it versions any other change,
for example with a new selector or a version exchanged on one of its streams.

## 4. Wire format

The payload of a QUIC DATAGRAM frame carrying ethp2p traffic is:

```text
+-----------------+------------------+
| selector frame  | protocol payload |
+-----------------+------------------+
```

The protocol payload extends to the end of the DATAGRAM frame.
Its length is the DATAGRAM frame payload length minus the length of the selector frame.
A protocol payload MAY be empty.

The selector frame uses the same encoding and validation rules as on streams:
the length MUST be between 1 and 10,
and the selector's minimal unsigned-varint encoding MUST occupy exactly that many bytes.
For selectors below 128, the selector frame is two bytes.

### 4.1. Size

A DATAGRAM frame is never fragmented.
A sender MUST NOT send a datagram whose DATAGRAM frame payload exceeds the peer's
`max_datagram_frame_size`, or that does not fit in a single QUIC packet on the current path.

The largest protocol payload a sender can currently send depends on the path MTU,
the connection's packet overhead, and the selector frame length.
It can change during a connection as path MTU discovery progresses.
Implementations MUST expose the current maximum protocol payload size
for a selector on a connection.

A datagram protocol SHOULD keep its payloads within the size
that fits a QUIC packet at the minimum QUIC path MTU of 1200 bytes,
minus packet overhead and the selector frame.
A protocol whose payloads can exceed
that size MUST define how it handles a payload too large to send as a datagram.

## 5. Sending

A sender MUST NOT send a datagram for a selector
before its selector exchange with the peer has completed,
and MUST NOT send a datagram for a selector that is not shared.

Sending is best-effort.
The QUIC stack MAY drop a datagram locally when its send queue is full or
when congestion control does not permit sending it in time.
Delivery is not acknowledged to the protocol, and datagrams are never retransmitted.
A protocol that needs reliable or ordered delivery MUST use streams for that data.

Datagrams are unordered with respect to each other and with respect to stream data.
A datagram sent after a stream write MAY arrive before that stream data.

## 6. Receiving

### 6.1. Processing

For each received DATAGRAM frame, the receiver MUST:

1. Discard the datagram if the ethp2p side of the connection is closed.
2. Apply the per-peer receive limit
   (section 6.3), and discard the datagram if it exceeds that limit.
3. Parse the selector frame, and discard the datagram if the frame is malformed.
4. Discard the datagram if its selector is `0`, is not shared, or has no local datagram consumer.
5. Otherwise, deliver the protocol payload to the protocol registered for the selector,
   identified with the connection it arrived on.

Receiving a datagram MUST NOT close the connection, reset a stream, or otherwise signal the peer,
whatever the outcome.
Protocol-level misbehavior detected from datagram contents is the protocol's concern.

### 6.2. No backpressure

A receiver MUST NOT apply backpressure to QUIC packet processing while waiting
for a protocol to consume datagrams.
When a protocol's receive buffer is full, the receiver MUST discard datagrams for that protocol.
Whether it discards the arriving datagram or the oldest buffered one is an implementation choice.
Protocols whose messages age quickly SHOULD request discarding the oldest.

### 6.3. Receive limits

QUIC congestion control bounds how fast a well-behaved peer sends,
but a misbehaving peer can ignore it.
A receiver MUST enforce a per-peer limit on received datagrams, counting both datagrams and bytes,
before parsing selectors or invoking protocols.
A token bucket per connection is sufficient.
Implementations SHOULD let each datagram protocol request an additional per-protocol limit.

Datagrams over the limit MUST be discarded.
How sustained excess affects peer reputation is outside this document.

### 6.4. Datagrams that arrive before the selector exchange

Datagrams are unordered with respect to the streams carrying the selector exchange.
A peer that has finished reading this endpoint's advertisement may send datagrams
before this endpoint has read the peer's advertisement.

A receiver MUST NOT deliver a datagram before its own selector exchange with
that peer has completed.
It MAY buffer a small, bounded number of such datagrams, counting them against the receive limit,
and process them when the exchange completes.
Otherwise it MUST discard them.

### 6.5. Accounting

Implementations SHOULD count discarded datagrams per connection and per reason: view closed,
receive limit, malformed selector frame, selector not shared or not consumed, full protocol buffer,
and exchange not yet complete.
Counters let operators distinguish network loss from local discarding.

## 7. Shared connections

On a QUIC connection shared with libp2p, all DATAGRAM frames belong to ethp2p.
The libp2p QUIC transport does not use QUIC datagrams,
so the shared dispatcher does not classify them.
When the ethp2p side of a shared connection is closed,
the receiver discards datagrams while libp2p continues to use the connection.

An ethp2p endpoint sharing a connection with libp2p MUST still send a non-zero
`max_datagram_frame_size`, because datagram support is negotiated once for the physical connection.

## 8. Security considerations

**Authentication.**
Datagrams are protected by the QUIC connection's keys,
so the sender is the peer authenticated by the connection's TLS handshake.
QUIC's packet protection prevents replay of individual packets,
but a protocol MAY still receive the same application message more than once,
for example from different peers.
Deduplication is the protocol's concern.

**Resource exhaustion.**
The receive limit in section 6.3 bounds the cost a peer can impose before any protocol code runs.
Selector parsing is constant-time and allocation-free.
Buffers are bounded per protocol, and overflow discards data instead of growing memory.

**Traffic analysis.**
Datagrams are encrypted, but their sizes and timing remain visible to an observer.
Protocols carrying sensitive traffic SHOULD consider padding.

## 9. Open questions

**First consumer.**
No protocol uses datagrams yet.
The shape of the delivery API should be validated against the first consumer before it is fixed.

**Priority.**
QUIC does not define priorities between datagrams and stream data.
Whether ethp2p should favor datagrams, for example attestations,
over bulk stream traffic during congestion is open.

**Stack use of datagrams.**
Selector `0` datagrams are discarded.
A future stack feature, such as liveness probes, could define them.

**Maximum size discovery.**
Protocols may want notification when the maximum payload size changes.
Whether polling the exposed maximum is sufficient is open.

## Implementation notes

This section is not normative.

The delivery API depends on the stack's runtime model, which is still under discussion.
Under the current push model, datagram delivery would follow stream delivery:
a subsystem registers a datagram destination, the stack runs one receive loop per connection,
and delivery never blocks that loop.
Under a pull model, the per-connection receive loop would sit in the transport's connection
dispatcher, which already runs for each connection, and protocols would pull from per-selector
buffers.
In both models, the send path is a single call on the peer handle that prepends the selector frame,
which the stack already writes for streams opened through the peer handle.
