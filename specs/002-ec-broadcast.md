# Erasure-coded broadcast framework

Status: draft, implemented in `broadcast/`.

## 0. Introduction

As Ethereum increases gas limits and shortens slot times,
the network must propagate larger payloads within tighter deadlines.
Store-and-forward broadcast (e.g. gossipsub) scales poorly in this regime.
Each node must receive and validate the full payload before forwarding it,
which increases latency with message size, underuses available network capacity,
wastes bandwidth due to the amplification factor necessary to retain security,
and requires bookkeeping to contain that amplification.

Erasure-coded broadcast addresses this by splitting a message into smaller chunks,
adding redundancy through erasure coding,
and disseminating those chunks in parallel across the network.
Nodes reconstruct the original message after collecting a sufficient number of valid chunks.
The origin produces `k` source chunks and `m` repair chunks,
and distributes them across peers with fanout `D`.
Receiver nodes relay coded chunks using code-specific strategies.
Every node can reconstruct the original once it has received `k` valid chunks.

Coding adds some computation at the origin,
but it meaningfully improves network-wide dissemination of large payloads compared with
store-and-forward.
The reason is simple: parallel network paths carry useful chunks concurrently,
rather than relying on propagation to execute as a sequential chain of full-message forwards.

Not all erasure coding schemes are suitable for network broadcast.
The ones that are differ in structure, assumptions, and operational properties.
This framework is scheme-agnostic, but is not fully modular:
it simply exposes calculated flexibility in peering, routing, verification, and gossip,
in order to enable experimentation and tuning across different object types and configurations,
during both design time and runtime.

The two schemes currently supported are Reed-Solomon and Random Linear Network Coding (RLNC).
Additional strategies can be added without changing the core framework,
or adjusting it only minimally.

## 1. About this document

This document specifies the broadcast framework itself.
It covers key concepts, semantics, interfaces, wire messages, and protocol expectations.

Strategy specifics, including the logic for encoding, decoding, verification, chunk routing,
and concrete wire types, are defined in companion documents.

The broadcast framework assumes peer discovery, authentication,
and routing are ambient concerns handled by ethp2p.
They are currently implemented minimally and will be completed as the ethp2p stack matures.

Sections 4 through 6 use RFC 2119 language such as MUST, SHOULD, and MAY.

## 2. Key concepts

**Channel.**
An interest shared by peers and bound to a specific broadcastable object type
(e.g. execution payload, blob, BALs, zkEVM proofs, etc).
Peers subscribe to channels according to their protocol duties.
A channel carries messages of one type over time, with each message handled by its own session,
which is explicitly delimited by open and close semantics.

**Message.**
An Ethereum object whose serialized version is published to a channel via the framework.
A message is identified by the pair `(channel, message_id)`.

**Session.**
An active broadcast transaction within the network.
All chunks pertaining to a message are grouped under the Session.
The Session accumulates intermediate state, such as buffered chunks, participating peers,
local and remote havelists, and more.
There are two kinds of sessions: an *origin session*,
created by the publisher who seeds the message to the network
(e.g. builder, prover, etc.),
and a *relay session*, created by everyone else to participate in dissemination
and reconstruction of the message.
Sessions are associated 1:1 with Messages, and also identified by `(channel, message_id)`.
Sessions are demarcated by open and close semantics,
and disposing of a Session stops all activity and deallocates all state objects associated with it.
In strictly timed protocols like Ethereum,
explicit Session disposal prevents network activity
for objects relevant in one slot from leaking into the next slot
(a known problem with gossipsub today).

**Strategy.**
The pluggable component that implements the specific logic of coding, decoding, recoding,
chunk verification, and chunk routing, for a given erasure coding scheme.
The framework/strategy boundary is the main load-bearing interface, and is defined in Section 8.

**Chunk.**
A unit of data produced by the coding scheme.
Chunks are smaller than the original message, and are explicitly separated into a header and data.
The header carries a strategy-specific chunk identifier,
such as a shard index in Reed-Solomon or a generation number plus coefficient vector in RLNC.
As explained later, this separation is useful to suppress duplicates.

**Preamble.**
Session-scoped, code-dependent metadata sent before any chunks.
Created by the origin, it contains all parameters needed
for relay nodes to initialize their strategy for that Session, such as coding scheme configuration,
chunk and generation counts, commitments, and/or signatures.

**Routing update.**
Metadata exchanged between peers to gossip about their current state.
Its schema is strategy-specific, such as a shard havelist in RS,
or a list of generation ranks in RLNC.
Strategies use routing updates to keep a live view of their vicinity,
and decide what chunks to send, to whom, in what order, and when to stop.

**Reconstruction.**
The operation to recover the original message from a sufficient set of valid chunks.
Once a relay has enough chunks, it decodes the message and delivers it to the application.
A Session continues serving chunks to peers that have not yet signaled successful reconstruction,
making sure the network as a whole stays cooperative.

## 3. Protocol overview

The protocol separates three concerns across three stream types.
Broadcast messages use Protobuf, with unsigned-varint lengths on the wire.
Each BCAST, SESS, and CHUNK protobuf frame has a uvarint length prefix followed by its serialized
protobuf payload.
The maximum protobuf payload per frame is 1 MiB.
For CHUNK streams, the raw chunk data follows the framed `Chunk.Header`.

**BCAST stream.**
The per-connection control stream.
It carries handshake, subscribe, and unsubscribe messages.
It opens after the stack delivers `PeerUp` for broadcast and remains open
while the broadcast binding is active.
It is implemented as two unidirectional streams, one in each direction,
each carrying length-prefixed protobuf frames.
It is the per-connection control stream;
a `SESS` stream can also remain open for the lifetime of a session.

**SESS stream.**
A per-session unidirectional stream.
It carries the session-open message, including the preamble to configure the coder,
and optional initial routing state.
Session-open is followed by zero or more routing updates over the lifetime of the session.
Each peer opens one `SESS` stream toward the other for a given session.
Under normal operation, a pair of peers collaborating to reconstruct a Message will initiate a pair
of `SESS` streams for the given Session, one in each direction.
When its strategy reports enough chunks to decode,
the opener resets its outbound `SESS` write side to tell the other peer
that no more chunks are needed.

**CHUNK stream.**
An ephemeral unidirectional stream to dispatch a single chunk.
First, the chunk header is written, followed by the chunk data.
The header specifies channel, message ID, the chunk identification,
and the length of the chunk data.
The data follows the header.
This decoupling enables the receiver to reject the chunk by resetting the stream, if not needed
(e.g. redundant, or already reconstructed).
Ephemeral streams are cheap in QUIC, and this stream pattern mimics HTTP/3.
It results in better parallelization and flow control, and more compact signaling mechanics.
We can also lean into QUIC features to optimize further via stream prioritization
and congestion control fine-tuning.

Every ethp2p stream, unidirectional or bidirectional, begins with a selector frame:
a uvarint payload length followed by the selector codepoint as a uvarint.
Stack reads the selector before routing an incoming stream.
Streams opened through `Peer.OpenUniStream`
or `Peer.OpenStream` have their selector written by Stack
before the stream is returned to the Family's consumer.

Broadcast registers one local Family containing the BCAST, SESS, and CHUNK protocols.
The `Hello` exchange advertises their selectors ([008](008-stack.md), section 4.3).
The Family is shared only with a peer that announces all three,
so a broadcast peer handle always supports BCAST, SESS, and CHUNK.
There is no broadcast Family identifier on the wire.
The broadcast package declares these stable selectors and local per-peer queue limits
(008, section 9.3):

| Selector | Protocol | `MaxQueued` |
| -------: | ------ | ----------: |
| `0x01` | `BCAST` | 1 |
| `0x02` | `SESS` | 64 |
| `0x03` | `CHUNK` | 128 |

These limits are local and are not announced to peers.
QUIC limits and flow control apply across the connection; no capacity is reserved per protocol.

Broadcast's stream cancellation outcomes use the protocol namespace
(`value<<1 | 1` on the wire), in one table for all three selectors.
Value `0` means no protocol-specific reason.

| Value | Name | Meaning |
| ----: | ---- | ------- |
| 1 | `Reconstructed` | The sender has reconstructed the message. |
| 2 | `Redundant` | The receiver no longer needs this chunk. |

The Go values are declared beside the selectors in `broadcast/protocol.go`:
`Reconstructed = wire.ProtocolCode(1)` and `Redundant = wire.ProtocolCode(2)`.
Shared stack outcomes are referenced from `wire` directly.
The stack resets streams whose selector broadcast does not own.

## 4. BCAST: control protocol

```protobuf
// Bcast is the control frame exchanged on the per-peer BCAST stream.
// Carries handshake, subscribe, and unsubscribe messages.
message Bcast {
  oneof message {
    Handshake peer_handshake = 1;
    Subscribe channel_subscribe = 2;
    Unsubscribe channel_unsubscribe = 3;
  }

  // Handshake is the first message exchanged on a BCAST stream.
  message Handshake {
    uint32 version = 1;
    repeated string channels = 2;
  }

  // Subscribe requests the remote peer to include us in a channel.
  message Subscribe {
    string channel = 1;
  }

  // Unsubscribe requests removal from a channel.
  message Unsubscribe {
    string channel = 1;
  }
}
```

### 4.1. Handshake

When the stack delivers `PeerUp` for broadcast, both sides perform a symmetric handshake.
Each side concurrently opens an outbound `BCAST` stream and accepts the peer's inbound stream.
The handshake is not complete until both sides have sent and received a `Bcast.Handshake` frame.

The two unidirectional streams are deliberate.
Both peers open their streams at the same time, and nothing needs resolving:
neither side takes an initiator or responder role, and there is no tie-break.
Each side writes on the stream it opened and reads on the stream it accepted.
A single bidirectional stream would force both sides to agree on which of them opens it.
Each side opens exactly one outbound `BCAST` stream per connection,
so a receiver cancels any second inbound `BCAST` stream.

Stack writes the `BCAST` selector on the outbound stream.
The broadcast binding then writes a `Bcast.Handshake` frame containing:

- `version`, the protocol version, currently `1`
- `channels`, the set of subscribed channel identifiers

Stack reads the inbound `BCAST` selector and routes the stream to the broadcast binding.
The binding reads the `Bcast.Handshake` frame and negotiates its protocol version.
With local version `1`, it accepts peer version `1` or higher and uses version `1`;
version `0` fails the handshake.
If the versions are incompatible, the broadcast handshake fails and its BCAST streams are cancelled;
the application still owns the underlying connection.

After a successful handshake, both sides know the remote identity authenticated by the transport's
TLS handshake, the protocol version, and the initial channel set.
Incoming `SESS` and `CHUNK` streams wait until the handshake completes in bounded local queues.
The stack resets streams beyond a protocol's `MaxQueued` with `Unspecified` (008, section 9.3).
If it fails, those queued streams are cancelled.

### 4.1.1. Capacity and progress

Protocols are expected to actively consume incoming streams and cancel reads they no longer need.
QUIC capacity is shared, so broadcast must avoid exhausting it with streams waiting for other streams.

CHUNK payloads for an existing session are independent:
header-reader concurrency and session payload-reader concurrency delay reads instead of resetting
them.
The engine and channel actors enqueue work without waiting for a reader slot.
A reader completion schedules the next queued payload read.

SESS is long-lived and depends on CHUNK progress,
and a CHUNK with no session depends on a missing SESS.
Broadcast bounds parked CHUNK streams to at most 32 per message
and 64 per peer across channels and messages.
These local bounds do not reserve QUIC capacity or by themselves guarantee progress.
Excess parked chunks are reset with `Unspecified`.
The existing 30-second cleanup tick cancels all remaining parked groups with `Refused`.

Ending SESS does not release the live session itself.
`EngineConfig.MaxLiveSessionsPerPeer`
therefore separately bounds relay sessions first opened by a peer, across channels.
The default is 32768, covering about 90 new sessions per second over TTL plus cleanup interval
(300 + 30 = 330 seconds, about 29700 resident), with roughly 10% headroom for bursts and scheduling.
TTL is not a strict residence bound; applications expecting higher rates MUST raise the limit.
Excess SessionOpen requests are reset with `Unspecified` on SESS.
Creator departure disposes that binding's creator sessions in every channel,
including channels it unsubscribed from; the count is released on disposal, including TTL expiry.
Local publishes are not charged.
BCAST permits one active inbound stream per connection as a local protocol rule.

Session lifecycle commands use a per-peer FIFO so a channel actor never waits
for the peer's control loop to open or write SESS.
The FIFO preserves lifecycle order and elides a queued open when its final close arrives.
Enrollment and attachment are idempotent per peer binding; removal retires the outbound lifecycle.
Adjacent close operations share one FIFO node, preserving their order.
FIFO nodes per peer are at most twice that peer's live attachments plus one;
close batches also retain already-open attachments awaiting retirement.
Total lifecycle memory is O(peers × per-peer session cap), plus local publishes.
Peer shutdown discards queued commands and cancels streams already opened.
Routing updates retain their separate bounded, nonblocking queue and may be dropped.

Every channel-inbox producer observes channel termination as well as its own cancellation.
A channel-lifetime registry owns inbound streams before enqueueing:
termination atomically rejects new registrations and cancels the registered streams,
including events still buffered in the inbox.
Thus a send racing termination cannot leave a stream without an owner,
even if its select chooses the send after termination.
Registry cancellation is local work and does not await remote credit.

### 4.2. Channel subscription

After handshake, a peer MAY subscribe to
or unsubscribe from channels at any time by sending `Bcast.Subscribe`
or `Bcast.Unsubscribe` on the outbound `BCAST` stream.

When a peer receives `Bcast.Subscribe`,
it SHOULD begin including the remote peer in sessions for that channel.
When it receives `Bcast.Unsubscribe`,
it SHOULD exclude that peer from new sessions for the channel and remove it from existing ones.

If a session finishes before a subscription is processed,
the new subscriber cannot be enrolled in that session.
A peer that attaches a new channel locally queues `Bcast.Subscribe` for each connected peer.
The notification is dropped if that peer's control queue is full.

A peer that connects or subscribes while a broadcast is in flight is enrolled into active sessions
for that channel.
The framework opens a `SESS` stream with the current routing state for the new peer;
it includes routing state when available and does not replay earlier `CHUNK` streams.

## 5. Session protocol (SESS streams)

```protobuf
// Sess is the frame type carried on per-session SESS streams.
// The first frame MUST be Open (which may carry initial routing
// state inline); subsequent frames are Update.
message Sess {
  oneof frame {
    Open   session_open   = 1;
    Update routing_update = 2;
  }

  // Open is the first frame on a SESS stream, establishing the session.
  message Open {
    string channel    = 1;
    string message_id = 2;
    bytes  preamble   = 3;
    bytes  initial_update = 4;
  }

  // Update carries an opaque routing state update (e.g. bitmap, have list).
  message Update {
    bytes data = 1;
  }
}
```

### 5.1. Session establishment

When a peer wants to participate in a session with a remote peer,
it opens a new unidirectional stream through `Peer.OpenUniStream` with selector `SESS`.
Stack writes the selector, and the broadcast binding writes a `Sess.Open` frame containing:

- `channel`, the channel identifier
- `message_id`, the message identifier
- `preamble`, the strategy-specific session metadata
- `initial_update`, optional code-specific routing state in the same format as `Sess.Update.data`

The receiver routes the frame to the active local channel handler named by `channel`.
If no handler is attached for that channel, it cancels the stream.
If a session for `(channel, message_id)` already exists, it ignores the duplicate open.

If `initial_update` is present,
the receiver MUST process it before handling any chunk data for that session.
Bundling the first routing update into `Sess.Open` removes a round trip
that could otherwise cause redundant chunk transmission in the interim.

### 5.2. Routing updates

After `Sess.Open`, the sender MAY write zero or more `Sess.Update` frames at any time.
Each frame carries a `data` field whose structure is strategy-specific,
such as a Reed-Solomon shard bitmap or an RLNC generation-rank list.

The framework MUST deliver routing data to the strategy without interpreting it.
Receivers SHOULD process routing updates promptly,
because stale routing state leads to unnecessary chunk sends.

The timing, frequency, and trigger conditions for routing updates are not strictly defined.
The current implementation is overeager,
and we expect to refine the trigger conditions and frequency as the protocol matures.
When a poll produces both routing updates and chunks, the channel queues the updates first.
The control and data loops write independently,
so their arrival order on the wire is not guaranteed.

### 5.3. Completion signaling

A peer that has enough chunks to decode may still have in-flight chunk streams
that need to drain cleanly, but a departed peer can be removed immediately.
The signaling mechanism reflects that distinction.

When a node's strategy reports that it has enough chunks to decode,
it cancels its outbound `SESS` write side for that session with `Reconstructed`,
encoded as application error code `0x03`.
This happens before decoding completes and tells the remote peer
that further chunk sends are unnecessary.
The remote peer then cancels pending chunk sends to that peer.

If a peer disconnects without resetting,
it is treated as departed and removed from the session immediately.

## 6. Data protocol (CHUNK streams)

```protobuf
// Chunk is the protocol namespace for chunk streams.
message Chunk {
  // Header prefixes each chunk on a CHUNK stream. The receiver reads
  // data_length raw bytes from the stream immediately after the header.
  message Header {
    string channel = 1;
    string message_id = 2;
    bytes chunk_id = 3;
    uint32 data_length = 4;
  }
}
```

### 6.1. Chunk header

Each chunk is sent on its own unidirectional stream.
The sender opens it through `Peer.OpenUniStream` with selector `CHUNK`;
Stack writes the selector before the sender writes a `Chunk.Header` frame containing:

- `channel`, the channel identifier
- `message_id`, the message identifier
- `chunk_id`, the strategy-specific chunk identifier as opaque bytes
- `data_length`, the number of raw bytes that follow

The framework requires `data_length`
because it cannot parse strategy-specific chunk identifiers to determine where the payload begins.
The receiver routes the frame to the active local channel handler named by `channel`.
If no handler is attached for that channel, it cancels the stream.
The maximum raw chunk data length is also 1 MiB.

### 6.2. Chunk data

Immediately after the `Chunk.Header`,
the sender writes exactly `data_length` bytes of chunk data and closes the stream.
The receiver reads the declared number of bytes and does not validate trailing bytes.

The framework delivers the chunk identifier and chunk data to the session
for two-phase verification.
Some strategies can reject invalid chunks synchronously, such as Reed-Solomon using shard hashes.
Others require asynchronous cryptographic verification, such as KZG or BLS-based schemes.
To support both without blocking the event loop,
the strategy first performs synchronous verification through `VerifyChunk`.
If it returns `VerdictPending`,
the chunk has been submitted to an asynchronous verification pipeline
and the result will later be delivered on the `Verified` channel.
A chunk enters strategy state only after verification succeeds and `TakeChunk` accepts it.

When multiple peers send chunks in the same deduplication group,
such as duplicate Reed-Solomon shards or linearly dependent RLNC chunks for the same generation,
the framework ties their inbound reads to a shared cancellation context.
Once the strategy decides the group is satisfied, the framework cancels the remaining reads.
This immediately frees QUIC stream capacity and connection-level flow control budget.

If the session does not yet exist because the chunk arrived before `Sess.Open`,
the channel buffers up to 32 stream references for that message
and relies on transport-level backpressure while their payloads remain unread.
It cancels additional streams after that per-message buffer is full.

## 7. Session lifecycle

### 7.1. Session state machine

A session moves monotonically through four stages.
It never transitions backward.

```text
                         TakeChunk returns          Decode succeeds
  ┌────────────┐         complete=true         ┌──────────┐            ┌───────────────┐
  │ Consuming  │ ────────────────────────────→ │ Decoding │ ─────────→ │ Reconstructed │
  └────────────┘                               └──────────┘            └───────────────┘

  Origin sessions start here:
  ┌────────┐
  │ Origin │
  └────────┘
```

**Consuming.**
The session accepts verified chunks.
Transition to Decoding occurs when the strategy signals completeness.

**Decoding.**
A background decode task is running.
No additional chunks are accepted.
Failure is non-recoverable.

**Reconstructed.**
Decode succeeded and the message has been delivered.
The session may continue serving peers.

**Origin.**
The publisher already has the message and serves chunks from the strategy's production queue.
This stage does not transition.

### 7.2. Origin sessions

An origin session is created when the application publishes a message to a channel.
The strategy encodes the message into chunks and produces the preamble.
The framework registers the session
and opens `SESS` streams to all peers currently subscribed to the channel.

Origin sessions begin in the origin stage.
The framework polls the strategy for chunks and dispatches them to peers.
Peers that connect or subscribe after the session begins can still be attached
and start receiving chunks.
Origin sessions never enter consuming or decoding, because the publisher already has the message.

### 7.3. Relay sessions

A relay session is created when a peer receives `Sess.Open` from a remote peer.
The framework extracts the preamble, passes it to the strategy's relay factory,
and registers the session.

Relay sessions begin in the consuming stage.
As chunks arrive, the framework submits them to the strategy for verification and acceptance.
The strategy tracks progress toward reconstruction.

Relays do not wait for reconstruction before forwarding.
After each accepted chunk, the framework polls the strategy for outbound work through `PollChunks`.
The strategy uses its current state together with peers' advertised inventory from routing updates
to decide what to send next.
For RLNC, this may mean generating new random linear combinations from the current basis.
For Reed-Solomon, it may mean forwarding shards to peers that lack them.
In both cases, relays contribute new dissemination work rather than merely replaying the origin's
behavior.

### 7.4. Reconstruction and disposal

When the strategy reports completeness by returning `complete = true` from `TakeChunk`,
two things happen immediately.

First, the framework notifies all peers participating in the session
that no more chunks are needed by resetting `SESS` streams as described in Section 5.3.
Second, it starts a background decode task.
Notification happens before decode completes because decode failure is non-recoverable.
There is no case in which the node would resume accepting chunks
after completeness has been signaled.

If decoding succeeds, the framework delivers the decoded message to the application.
The session may still serve chunks to peers that have not reconstructed.
A session is disposed once all participating peers have either reconstructed or departed.

## 8. The Strategy interface

### 8.1. Ownership boundary

The framework owns the network-facing side of the protocol: peer connections, stream multiplexing,
session lifecycle, and the event-driven dispatch loop.
The strategy owns the coding-specific logic: encoding, verification, and dispatch policy.
The interface between them is a generic strategy interface parameterized by chunk identifier type
and routing state type.
The authoritative definition, supporting types,
and behavioral contracts are in `broadcast/types.go`.

Adding a new coding scheme requires only a new `Strategy` implementation.
It does not require changes to the wire protocol, session machinery, or stream multiplexing.

### 8.2. Strategy interface

```go
// Strategy is the per-session interface that unifies coding, routing,
// and dispatch. One instance per (channel, message, direction). Created
// by Scheme factory functions.
//
// # Threading model
//
// All methods are called from a single goroutine (the owning Channel's
// run loop), with two exceptions:
//
//   - Work() and Verified() return channels; the channels themselves
//     are read by the channel goroutine, but the strategy may write to
//     them from internal goroutines (e.g. an async encoder or
//     verification worker pool).
//   - VerifyChunk() reads only immutable state set at construction
//     (preamble hashes, commitment parameters). It may submit work to
//     an internal worker pool whose results arrive on Verified().
//   - Decode() is called from a background goroutine after TakeChunk
//     signals completeness. Safe because the strategy rejects further
//     chunks after completeness, freezing the state for concurrent reads.
//   - Close() is called after all in-flight reads have drained, so no
//     concurrent calls are in progress.
//
// Because of single-goroutine access, implementations do not need
// internal locking for session state. The exception is strategies that
// produce chunks asynchronously (e.g. RLNC origin encoding), which
// must synchronize the producer goroutine with PollChunks.
//
// # Lifecycle
//
// Factory (Scheme.NewOrigin / Scheme.NewRelay)
//
//	→ AttachPeer (one or more)
//	→ [VerifyChunk | TakeChunk | RoutingUpdate | PollChunks | PollRouting | ChunkSent]*
//	→ TakeChunk returns complete=true (relay only)
//	→ Decode (background goroutine)
//	→ DetachPeer (for each attached peer)
//	→ Close
//
// The session calls drainPolls (PollRouting then PollChunks) after
// every state-mutating event: AttachPeer, VerifyChunk, TakeChunk,
// RoutingUpdate, and ChunkSent. Strategies make new work available by returning it
// from the next PollChunks call rather than pushing it.
//
// # Invariants the strategy must maintain
//
//   - Per-peer tracking: the strategy must track which peers are
//     attached and must not return dispatches targeting detached peers.
//   - In-flight accounting: for each dispatch returned by PollChunks,
//     the session will call ChunkSent exactly once with the dispatch's
//     Handle. The strategy must track its own in-flight state keyed
//     by handle to correlate the callback.
//   - Failure handling: when ChunkSent reports ok=false (slot full,
//     peer gone, or cancelled), the strategy decides whether to retry
//     (re-enqueue for the next PollChunks) or drop the chunk for that
//     peer. The session does not retry automatically.
//   - Routing consistency: RoutingUpdate must return the handles of
//     in-flight sends that are no longer needed after the update. The
//     session cancels those outbound sends directly.
type Strategy[CI ChunkIdent, R Wire] interface {
	io.Closer

	// HaveChunk reports whether this chunk has already been received
	// and processed. Used as a fast-path gate before TakeChunk: if
	// true, the session skips the read entirely.
	//
	// False positives are forbidden (they would drop needed chunks).
	// False negatives are harmless (TakeChunk handles duplicates).
	HaveChunk(chunkID CI) bool

	// DedupKey returns a grouping key for concurrent inbound reads.
	// All in-flight reads sharing a non-nil key share a cancellation
	// context; the strategy cancels the group via TakeChunk's dedup
	// handle when it decides the group is satisfied. Returns nil to
	// opt out of dedup grouping for this chunk.
	DedupKey(chunkID CI) []byte

	// AttachPeer registers a peer in this session. Called before
	// PollChunks can target this peer. The strategy should initialize
	// per-peer state (send tracking, routing, etc.).
	AttachPeer(peer PeerID)

	// DetachPeer removes a peer. completed=true means the peer
	// signaled successful reconstruction (SESS value 1); false means
	// disconnection or unsubscribe. The
	// session cancels all in-flight sends for this peer before
	// calling DetachPeer, so any subsequent ChunkSent callbacks for
	// this peer will arrive with ok=false.
	DetachPeer(peer PeerID, completed bool)

	// VerifyChunk verifies an inbound chunk before acceptance. Returns
	// VerdictAccepted or VerdictInvalid for synchronous verification
	// (e.g. SHA-256). Returns VerdictPending when verification is
	// submitted to an async worker pool (e.g. BLS signatures, KZG
	// proofs); the result arrives on the Verified() channel.
	VerifyChunk(peer PeerID, chunkID CI, data []byte) Verdict

	// Verified returns a channel that delivers results of async
	// verifications submitted via VerifyChunk (VerdictPending).
	// Returns nil if the strategy never uses async verification.
	Verified() <-chan VerifyResult[CI]

	// TakeChunk delivers a pre-verified inbound chunk to the strategy.
	//
	// The session owns dedup group creation and lookup; it passes the
	// resulting cancel handle into TakeChunk via dedup. The strategy
	// decides when the group is satisfied and calls dedup.Cancel() at
	// that point. For RS, every accept satisfies the group (a shard
	// index is unique). For RLNC, the group is satisfied when the
	// generation reaches full rank.
	//
	// The bool return signals completeness: true means the strategy
	// has enough data to decode. The session calls Decode on a
	// background goroutine. After returning true, the strategy must
	// reject further chunks so the state remains frozen for the
	// concurrent Decode call.
	TakeChunk(peer PeerID, chunkID CI, data []byte, dedup *DedupCancel) (Verdict, bool, error)

	// RoutingUpdate delivers a peer's routing state (e.g. a bitmap
	// of which chunks they have). Returns handles of in-flight sends
	// to this peer that are now redundant.
	RoutingUpdate(peer PeerID, update R) (cancel []ChunkHandle, err error)

	// PollChunks returns pending chunk dispatches. Called after every
	// state-mutating event; returning nil signals no work available.
	// Must not return dispatches for detached or completed peers.
	PollChunks() []ChunkDispatch[CI]

	// PollRouting returns the current routing state for broadcast to
	// peers. force=false returns only if state changed since the last
	// poll; force=true returns unconditionally (used for the initial
	// routing update bundled with SessionOpen). Called before
	// PollChunks so peers see updated routing before chunks arrive.
	PollRouting(force bool) (R, bool)

	// ChunkSent reports the outcome of a chunk send. Called exactly
	// once per dispatch returned by PollChunks. err=nil means the
	// chunk was written to the wire; non-nil carries the failure
	// reason.
	ChunkSent(peer PeerID, handle ChunkHandle, err error)

	// Progress returns decode progress: chunks received and chunks needed.
	Progress() (have, need int)

	// Decode reconstructs the original message from the strategy's
	// complete state. Called from a background goroutine after
	// TakeChunk signals completeness. Decode failure is
	// non-recoverable and indicates a bug.
	Decode() ([]byte, error)

	// Work returns a channel signaled when new dispatches are
	// available from an async producer (e.g. background encoder).
	// Returns nil if the strategy never produces asynchronously.
	Work() <-chan struct{}
}

// Scheme is a per-channel factory for creating Strategy instances.
type Scheme[CI ChunkIdent, R Wire, P Wire] struct {
	Name      string
	NewOrigin func(MessageID, []byte) (Strategy[CI, R], P, error)
	NewRelay  func(MessageID, P) (Strategy[CI, R], error)
	// ...
}
```

### 8.3. Chunk verdicts

```go
// Verdict classifies the result of processing an inbound chunk.
type Verdict uint8

const (
	// VerdictAccepted indicates that the chunk was useful in advancing decoding.
	VerdictAccepted Verdict = iota
	// VerdictRedundant indicates that the chunk carries no new information:
	// the shard/generation is already satisfied, or the session has enough
	// data to decode.
	VerdictRedundant
	// VerdictDecoding indicates that the chunk arrived after completeness
	// was signaled to peers but before decode finished. These are in-flight
	// leftovers from the network.
	VerdictDecoding
	// VerdictSurplus indicates that the chunk arrived after the session
	// has fully reconstructed the message.
	VerdictSurplus
	// VerdictInvalid indicates that this chunk was malformed or failed
	// verification.
	VerdictInvalid
	// VerdictPending indicates that verification has been submitted to an
	// async worker pool. The result will arrive on the Verified() channel.
	VerdictPending
)
```

### 8.4. Chunk authentication

Chunk authentication is delegated to the strategy.
Different coding schemes require fundamentally different authentication mechanisms.

Reed-Solomon can authenticate individual shards using commitment hashes
or sender signatures included in the preamble.
RLNC cannot authenticate arbitrary coded chunks without homomorphic commitments,
because each chunk is a random linear combination whose contents are not known in advance.

## 9. Open questions

**Identity types.** `PeerID`, `ChannelID`,
and `MessageID` to be replaced with meaningful data in Ethereum.

**Channel lifecycle.**
The framework supports dynamic channel attachment and detachment,
but does not define channel discovery, metadata exchange, or access control at the protocol level.
It remains open whether channel membership should derive from validator duties
or from application configuration.

**Session disposal policy.**
The framework should support both application-driven disposal,
for example when a message becomes irrelevant after a new slot, and strategy-defined TTL policies.
The correct abstraction at the boundary between protocol and application time is still unclear.

**Transport abstraction.**
The framework is described in terms of QUIC streams,
but it should work over any multiplexed transport that provides ordered byte streams,
unidirectional stream creation, stream reset with application error codes,
and independent per-stream flow control.

**Framework-level authentication envelope.**
One possible extension is a framework-visible wrapper around strategy-specific chunk identifiers
that carries signatures or commitments.
That could let the framework reject some invalid chunks before invoking the strategy.

**Routing update validation.**
Routing updates are accepted without validation.
A malicious peer could advertise false inventory to suppress needed chunk sends
or to induce redundant transmission.
Mitigations are strategy-specific and may include consistency checks between advertised state
and observed traffic.

**Denial-of-service vectors.**
Stream exhaustion, chunk flooding for sessions the receiver never opened,
and routing poisoning still need analysis and mitigation design.
