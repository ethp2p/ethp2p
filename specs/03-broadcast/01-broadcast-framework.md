---
status: implemented
requires:
  - ../01-transport/02-streams.md
---

# Broadcast framework

Store-and-forward broadcast makes every node receive and validate a whole payload before forwarding it, so latency grows with object size and spare capacity goes unused.
Erasure-coded broadcast splits an object into chunks, adds redundancy and sends the chunks over parallel paths; a node reconstructs once it holds enough valid chunks and forwards useful chunks before it has reconstructed.
The framework is scheme-agnostic: coding, verification and chunk routing sit behind a strategy.
Broadcast attaches to the stack as protocol 1
([Streams](../01-transport/02-streams.md#protocols));
the stack owns connections, peer identity and stream headers, and broadcast owns everything above, including how it classifies its own streams.
This document defines the roles, the shared framing and outcome codes, the session lifecycle and the strategy contract.
The broadcast protocol and its three kinds are in [Broadcast protocol](02-broadcast-protocol.md); coding schemes are [Reed-Solomon](05-reed-solomon.md) and
[RLNC](06-rlnc.md).
The meaning of MUST and SHOULD is in [Conventions](../00-foundations/02-conventions.md); defined terms (channel, object, session, chunk, preamble, routing update, origin, relay, subscriber, binding, strategy, verdict) are in [Terminology](../00-foundations/03-terminology.md).

A node plays one of two roles per object, origin or relay; subscriber is a property of the channel.

```text
origin                         relay                        relay
  | -- session open + preamble -> |                            |
  | -- chunk, chunk, chunk ----> | -- session open ---------> |
  | <- routing update ---------- | -- chunk, chunk ---------> |
  |                              | <- "reconstructed" ------- |
```

Back-pressure is left to QUIC stream credit: a receiver that does not want or cannot process a chunk does not read it, and no overload message exists.
A relay session consumes chunks, decodes once it has enough, and then keeps serving peers; an origin session never consumes.

```text
 ┌───────────┐  TakeChunk: complete  ┌──────────┐  Decode ok  ┌───────────────┐
 │ Consuming │ ────────────────────→ │ Decoding │ ──────────→ │ Reconstructed │
 └───────────┘                       └──────────┘             └───────────────┘
 ┌────────┐
 │ Origin │  (no transitions)
 └────────┘
```

## Wire format

Broadcast is protocol 1.
Three unidirectional stream kinds carry all broadcast traffic, opened through the stack, so each stream begins with its stream header ([Streams](../01-transport/02-streams.md#stream-header)) and then a kind: one minimal uvarint, written by broadcast after the protocol header.
The same kind follows the protocol header on every broadcast datagram ([Datagrams](../01-transport/05-datagrams.md)).

| Kind | Number | Stream header | Frame types | Lifetime | Purpose |
| ---- | -----: | ------------- | ----------- | -------- | ------- |
| control | 1 | `01 01` | `HANDSHAKE` (0), `SUBSCRIBE` (1), `UNSUBSCRIBE` (2) | Per peer binding | Handshake, channel subscribe and unsubscribe. One stream per direction. |
| session | 2 | `01 02` | `OPEN` (0), `UPDATE` (1) | Per session and peer | Session open with preamble and routing, updates, completion. One stream per direction per session. |
| chunk | 3 | `01 03` | `HEADER` (0), `DATA` (1) | One chunk | Chunk header, then chunk data. |

Bidirectional streams MUST NOT be used for broadcast.

```text
stream = stream_header(1) || uvarint(kind) || *frame   # the frames are defined by the kind, in the broadcast protocol
```

Every frame on these kinds follows [Frames](../01-transport/02-streams.md#frames), with a payload limit of `MAX_BROADCAST_FRAME` (1 MiB), so a frame length takes 1 to 3 bytes.
Frame types are numbered per kind.
Each frame's payload is the message its type names, except `DATA`, whose payload is the chunk's bytes.
A receiver MUST cancel the read side of a stream that carries a malformed frame with `Unspecified`, and of one that carries a frame type its kind does not allow at that position, including an unknown type, with `Refused`.
A receiver MUST refuse a stream whose kind it does not define with `Refused`: unknown kinds are broadcast's to refuse, because the stack reads only the protocol header.

Broadcast adds two protocol-namespace codes to the shared stack codes ([Outcome codes](../00-foundations/06-outcome-codes.md)); a protocol code is `value<<1 | 1`.

| Value | Name | Wire | Sent on | Meaning |
| ----: | ---- | ---: | ------- | ------- |
| 1 | `Reconstructed` | `0x03` | session write side | The sender has enough chunks to reconstruct the object. |
| 2 | `Redundant` | `0x05` | chunk read side | The receiver no longer needs this chunk. |

It also uses the stack codes `Unspecified` (`0x00`), `Refused` (`0x02`) and `Timeout` (`0x04`).

| Constant | Value |
| -------- | ----: |
| `MAX_BROADCAST_FRAME`: payload limit of every frame on the control, session and chunk kinds | 1 MiB |
| Stack `MaxQueued`: broadcast, one bound shared across kinds (local, not sent) | 193 |

### Strategy interface

The strategy interface is in-process, not on the wire: a new scheme is a new strategy plus a factory and needs no change to any format.

| Factory | Purpose |
| ------- | ------- |
| `NewOrigin` | From the object ID and payload, returns a strategy and a preamble. |
| `NewRelay` | From the object ID and the received preamble, returns a strategy. |
| `NewCI`, `NewR`, `NewP` | Allocate empty chunk-identifier, routing and preamble values for unmarshalling. |

| Method | Called |
| ------ | ------ |
| `HaveChunk`, `DedupKey` | On chunk admission. |
| `AttachPeer`, `DetachPeer` | On peer membership changes. |
| `VerifyChunk`, `Verified` | On chunk verification. |
| `TakeChunk` | When a verified chunk is accepted. |
| `RoutingUpdate`, `PollRouting` | On routing state in and out. |
| `PollChunks`, `ChunkSent` | On chunk dispatch and its outcome. |
| `Progress`, `Decode`, `Close` | For the observer, reconstruction, and disposal. |

| Verdict | Meaning |
| ------- | ------- |
| `VerdictAccepted` | The chunk advanced decoding. |
| `VerdictRedundant` | Adds nothing: already held, its shard or generation is full, or the strategy can decode. |
| `VerdictDecoding` | Arrived after completeness was signalled and before decode finished. |
| `VerdictSurplus` | Arrived after the object was reconstructed. |
| `VerdictInvalid` | Malformed or failed verification. |
| `VerdictPending` | Verification went to a worker pool; the result arrives on `Verified`. |

Send outcomes reported to `ChunkSent`: `nil` (written to the wire), `ErrChunkMarshal`, `ErrChunkPeerGone`, `ErrChunkSlotFull`, `ErrChunkWriteFail`, `ErrChunkCancelled`.

## Behavior

### Framework

**Protocol attachment.**
Broadcast MUST be registered with the stack before it starts, and once per engine.
Broadcast sees a peer as peer-up, its streams as stream-in, its departure as peer-down.

- On peer-up broadcast binds the peer and starts its [handshake](02-broadcast-protocol.md#control-kind).
- On stream-in broadcast hands the stream to that peer's binding, or rejects it if there is none.
- On peer-down broadcast closes the binding and removes the peer from every channel.

A receiver MUST cancel a bidirectional stream of protocol 1 with `Refused`.
The stack delivers a peer's peer-down before the next peer-up for the same peer ID, so at most one binding per peer ID is live.
The application owns the connection; closing a binding cancels only streams broadcast owns.

**Subscriber delivery.**
A channel has at most one subscriber.
Delivery MUST NOT block the channel: if the subscriber cannot take the object it is dropped and logged.
The subscriber's feed ends when the channel stops.

**Channels.**
Attaching a channel with an existing ID fails and leaves the existing channel untouched.
Stopping a channel ends its actor, disposes all its sessions, and announces the removal to peers.

**Non-blocking channels.**
Strategy methods run in the channel's single-threaded context, so a strategy MUST NOT block.
A channel's processing MUST NOT wait for engine or peer progress, for stream credit, or for further channel events.
Network back-pressure is absorbed by QUIC stream credit and bounded queues at the edges, not inside the channel.

**Stream outcomes.**
A receiver MUST use `Refused` for a stream it will not process (unknown kind, unknown channel, a frame type not allowed at its position, second control stream, expired session) and `Timeout` when a deadline expires.
`Reconstructed` and `Redundant` are used as defined for the [session kind](02-broadcast-protocol.md#session-kind) and the [chunk kind](02-broadcast-protocol.md#chunk-kind).

**Stream bounds and progress.**
Broadcast shares one connection's stream credit across the session and chunk kinds, and a stream waiting on another stream can exhaust it, so every waiting stream is tied to something that can make progress:

- Handlers MUST consume incoming streams actively and cancel reads they no longer need.
- Concurrent chunk header reads are limited per peer; others wait unread, bounded by QUIC credit.
- A session reader keeps one of 64 per-peer slots for its lifetime; a session stream beyond that MUST
  be cancelled at once with `Unspecified` rather than waiting, because its completion depends on
  chunk progress.
- A chunk stream whose session does not exist is parked, bounded per object.
- A session outlives its session streams and is bounded only by its TTL and by disposal.

These rules bound what a peer can make the node hold; they reserve no QUIC capacity.

### Sessions

**Stages.**
A session is in exactly one stage, and stages only advance.

| Stage | Meaning |
| ----- | ------- |
| Consuming | Relay. Accepts verified chunks, forwards work to peers. |
| Decoding | Relay. The strategy signalled completeness; a background decode runs; chunks are refused. |
| Reconstructed | Relay. Decoding succeeded and the object was delivered to the subscriber (if any). The session keeps serving peers. |
| Origin | The node published the object. Holds the payload from the start and never enters the other stages. |

Chunk handling: in Consuming a chunk is checked with `HaveChunk`, read, verified, then given to the strategy.
In Decoding an inbound chunk stream MUST be cancelled with `Redundant` and counted `VerdictDecoding`.
In Reconstructed and Origin it MUST be cancelled with `Redundant` and counted `VerdictSurplus`.
A decode error is reported to the observer and the session stays in Decoding until disposed.

**Origin.**
Publishing builds the strategy and preamble, then in the channel: disposes any session with the same object ID (reason `replaced`); registers the origin session and attaches every current member; polls the strategy and dispatches its chunks.
Peers that subscribe later are attached and start receiving chunks.

**Relay.**
A relay session is created by a [session open](02-broadcast-protocol.md#session-kind).
After each accepted chunk the session polls the strategy for outbound work.
Relays do not wait for reconstruction to forward: the strategy combines its own state with peers' advertised inventory to decide what to send.

**Reconstruction.**
When `TakeChunk` first returns `complete = true` in Consuming, the session in this order:

1. moves to Decoding and signals completion to every peer;
2. starts the decode in the background;
3. on success, delivers the object to the subscriber without blocking, reports it to the observer,
   and moves to Reconstructed.

**Peer changes.**

- A peer that joins the channel is attached to all active sessions: the strategy learns of it
  (`AttachPeer`) and an `Open` is queued.
- A peer that leaves (disconnect, unsubscribe, channel stop) is removed from all sessions of the
  channel.
- A peer whose inbound session stream for one object ends by FIN, by a reset other than `Reconstructed`, or by a read error is removed from that session only.
  The event MUST be ignored if it comes from a binding since replaced by a newer one for the peer ID.
- Removal closes its outbound session stream, drops its queued chunk sends,
  cancels its parked and queued chunk streams with `Unspecified`,
  and tells the strategy it detached with `completed = false`.
- A peer removed for one session is not re-attached when it opens that object again: the `Open` only applies its `initial_update`.
  Only a new bind after an unbind (reconnect or resubscribe) re-attaches it.

**Disposal.**
A session is idle, and MUST be disposed in the same channel event, when:

- it is Origin or Reconstructed and every attached peer has completed or been removed; or
- it is Consuming or Decoding and every attached peer has been removed.
  Completed peers are not removed here: they may still send chunks on separate streams.

A session that never had a peer attached is never idle; it is left to the TTL.
Idleness is checked when a peer completes or is removed, including by the end of its session stream, and when decoding finishes.
Sessions also end by TTL (every 30 seconds the channel disposes sessions older than 5 minutes, reason `ttl_expired`), by replacement (`replaced`), and by channel stop or engine close.

Disposal, in order:

1. queues a session close to every attached peer, including completed peers: the outbound session
   stream is closed with FIN and the peer's chunk slot for the session dropped;
2. cancels the session context, which cancels inbound chunk reads and drops queued sends
   but does not abort a chunk write in progress (bounded by its own 5 s deadline);
3. cancels inbound chunk streams parked for the object and queued reads with `Redundant` if the
   session had reached Decoding or beyond, `Refused` if the TTL expired, `Unspecified` otherwise;
4. waits for the session's background reads and decode to end, then closes the strategy.

A later `Open` or chunk for a disposed object starts a new relay session or parks again.

### Strategy

The framework owns everything network-facing: peer bindings, streams, session lifecycle, deduplication groups, read concurrency and the event loop.
A strategy owns the coding-specific logic; one instance exists per session, for origin or relay.

**Threading.**
The framework MUST call every strategy method from the owning channel's single sequential context, so a strategy needs no locking for session state.
Exceptions: `Verified`'s channel is written by verification workers and read by a framework goroutine; `Decode` runs on a background goroutine after `TakeChunk` reports completeness; `Close` runs after every in-flight read and the decode finished, with no other call in progress.
Methods called from the channel context, including factories and `Close`, MUST NOT wait for engine or peer progress, stream credit or other channel events.
`VerifyChunk` runs in the channel context, so a slow verification stalls the channel: asynchronous verification MUST use `Verified` instead of blocking.

**Lifecycle and calls.**

```text
Factory (NewOrigin | NewRelay)
  -> AttachPeer (one or more)
  -> [ VerifyChunk | TakeChunk | RoutingUpdate | PollChunks | PollRouting | ChunkSent ]*
  -> TakeChunk reports complete   (relay only)
  -> Decode                       (background)
  -> DetachPeer                   (each attached peer)
  -> Close
```

After every event that can change strategy state (a peer attach, a verified chunk, an accepted chunk, a routing update, a send outcome) the session polls `PollRouting`, then `PollChunks`.
A strategy makes work available by returning it from the next poll; it does not push.

| Call | Contract |
| ---- | -------- |
| `HaveChunk` | MUST report whether the chunk was already received. False positives are forbidden (they drop needed chunks); false negatives are harmless. When true the session cancels the stream before reading data. |
| `DedupKey` | A group key for concurrent inbound reads, or nil for no grouping. |
| `AttachPeer` / `DetachPeer` | Attach is called before `PollChunks` may target the peer. Detach carries `completed = true` when the peer signalled `Reconstructed`, `false` on disconnect, unsubscribe or the end of the peer's session stream. Before `DetachPeer` the session cancels the peer's in-flight sends, so later `ChunkSent` calls carry errors. |
| `VerifyChunk` | See Authentication below. |
| `TakeChunk` | Receives an already verified chunk with the dedup handle (nil if none). Returns a verdict, whether the strategy can now decode, and an error. Never called on origin sessions. |
| `RoutingUpdate` | Applies a peer's routing state and returns the handles of in-flight sends to that peer that are now redundant; the session cancels them. |
| `PollChunks` | MUST NOT target detached or completed peers. Returns pending dispatches. |
| `PollRouting` | With `force = false`, state only if changed since the last poll. With `force = true`, unconditionally; the session uses this for the routing bundled into the session open. |
| `ChunkSent` | Called exactly once per dispatch, with its handle. `nil` means written; otherwise one of the errors above. The session never retries; the strategy re-enqueues or drops. |
| `Progress` | Chunks held and needed, for the observer. |
| `Decode` | Reconstructs the object. Failure is non-recoverable, since a strategy cannot evict an accepted chunk; failure after completeness indicates a defect. |

**Completeness.**
When `TakeChunk` returns complete, the strategy MUST reject every later chunk, so its state is frozen for the concurrent `Decode`.
The session also stops reading chunks past Consuming, but the strategy MUST NOT rely on that alone.

**Deduplication handle.**
The session owns dedup groups.
The strategy decides when a group is satisfied and cancels it through the handle given to `TakeChunk`, which cancels the group's remaining reads with `Redundant`.
For Reed-Solomon every accepted shard satisfies its group; for RLNC the group is satisfied when the generation reaches full rank.
Cancelling through a nil handle is a no-op.

**Verdicts.**
Every processed chunk yields a verdict, reported to the observer.
The session, not the strategy, assigns `VerdictDecoding` and `VerdictSurplus` for chunks it refuses by stage.
A verdict has no wire effect by itself; cancellation follows from admission and dedup.
Only `VerdictAccepted` from `VerifyChunk` or a completed async verification reaches `TakeChunk`.

**Authentication.**
The framework does not authenticate chunks or preambles; it guarantees only the ordering that lets a strategy do so:

- `VerifyChunk` runs on every inbound chunk before `TakeChunk`, and `TakeChunk` runs only after a
  `VerdictAccepted`: a chunk enters strategy state only through that path.
- A chunk is verified against a strategy built from the session's preamble.
  If the strategy cannot be built, no session exists and no chunk is accepted.
- An asynchronous verification (`VerdictPending`) delivers its result back to the channel; a result
  for a session disposed in the meantime is discarded.
- A chunk that fails verification is dropped and recorded.
  The framework applies no peer penalty and does not drop the peer.

What is authenticated depends on the scheme: Reed-Solomon can authenticate each shard against commitments in the preamble; RLNC cannot authenticate arbitrary coded chunks without homomorphic commitments, since each is a fresh linear combination.
The preamble's authenticity (for example an origin signature) is the strategy's responsibility.
Routing updates are not validated by the framework.

### Changes

The broadcast protocol has no extension points other than the opaque scheme fields: a peer reads `Handshake.version`, the kind numbers, the frame types and their message schemas, and nothing else.
An unknown frame type is refused, never ignored.
A change old implementations cannot process ships with a new application protocol name in place of `ethp2p_0`, or with a new protocol id: a peer that lists protocol 1 serves broadcast, all three of its kinds and their frame types as defined here, so they cannot change in place.
A kind is added with a new kind number, never by reusing or renumbering one.
`Handshake.version` is the one departure: a `version` above 1 is accepted and treated as 1, with no version stored per binding, so the field cannot signal an incompatible change today.
A new coding scheme needs no wire change: chunk identifiers, routing updates and preambles are opaque to the framework, and the scheme owns their encoding and versioning.
Each channel is bound to one scheme; peers that disagree on a channel's scheme do not interoperate on it.

## Failure modes

The streams are carried inside an authenticated QUIC connection, so a network attacker cannot modify, inject or forge a broadcast frame; the threats are those a peer can pose.

- **Resource exhaustion.**
  Every frame length and `data_length` is checked against `MAX_BROADCAST_FRAME` (1 MiB) before allocation.
  Early streams are bounded: `MaxQueued` 193 shared across kinds, 64 session slots per peer, 5 chunk header readers per peer with a 5 second header deadline, 32 parked streams per object, 64 payload readers per session.
  Queued and parked chunk streams are unread and bounded by the peer's own QUIC stream limit.
- **Payload buffers.**
  A buffer of `data_length` bytes is allocated at admission, so a peer that declares 1 MiB and sends nothing pins it until the 5 second read deadline: up to 64 MiB per session.
- **Session count.**
  Nothing bounds the relay sessions a peer can open on a channel it is subscribed to except the 5 minute TTL (swept every 30 seconds).
  The per-peer live-session cap was removed, and an admission check at session open that would validate the session against the scheme is not part of this document.
- **Misattribution.**
  A binding is keyed by the transport-authenticated identity and channel membership follows that peer's control stream only, so a peer cannot subscribe another peer, and a stale event from a replaced binding is ignored.
  A chunk is attributed to the peer whose stream carried it; whether it is genuine is the strategy's job.
- **False state.**
  A routing update is trusted as advertised: a peer can advertise inventory it does not hold or hide inventory.
  The cost is bandwidth, not correctness, because decoding accepts only verified chunks.
  A peer can signal `Reconstructed` falsely, which stops this node's sends to it but not its own chunk streams to this node.
- **Disclosure.**
  `Handshake` and `Subscribe` reveal attached channels; `Open` reveals the preamble to every
  subscribed peer, which is its purpose; `Redundant` reveals that a node holds a chunk, and no
  overload signal exists, so load is not disclosed.
- **Closure.**
  A violation on a control stream ends only that peer's binding; on one session or chunk stream it cancels that stream only.

## Rationale

**Scheme-agnostic framework.**
Keeping coding, verification and routing in the strategy means a new scheme needs no wire change.

**Back-pressure from QUIC credit.**
The `Overloaded` stack code and capacity advertisements were removed: `Overloaded` told a peer that a local queue was full and leaked the receiver's load; every overflow now resets or closes with `Unspecified`.
Per-peer chunk-stream caps and a registry of held chunks were removed for the same reason: QUIC stream limits already bound what a peer can hold, and peer close cancels parked and queued streams.
Per-protocol stream credit is not part of this document.

**Actors never wait.**
Actors never wait on one another or on stream credit, so channel-inbox back-pressure stays independent of network back-pressure.
Parked streams are bounded per object so they cannot retain all the credit their own session stream needs; the bound does not reserve QUIC capacity or guarantee progress by itself.

**Explicit disposal.**
Sessions are disposed explicitly so an object's traffic ends when its session ends.
Reconstruction is signalled before decoding finishes because decode failure is non-recoverable and no further chunks are accepted.
A session outlives its session streams and ends only when disposed or at its TTL.
Idle sessions are disposed directly in the event that made them idle, not by posting a dispose event to the session's own inbox, which could dispose a replacement session or be dropped.
The per-peer live-session lease and creator-departure disposal were removed, with the expectation that admission will be decided by validating a session at open time.

**Events keyed by object ID alone.**
Chunk-sent, read-done, decode and async-verification events find their session by object ID.
An object ID is a content address of the seed payload (for execution payloads, the beacon block), so an event for a replaced session is an event about the same object and applies to the replacement.

**Reconnect replacement removed.**
The stack queues a peer's peer-down before the next peer-up for the same ID, so the old binding is already unbound when the new one appears.
Checks that ignore stale events from a replaced binding remain, because an old binding's exit and queued subscriptions can still arrive afterwards.

**Routing ticker dropped.**
A 25 ms per-channel poll never found anything the event poll had not already sent, since routing changes come only from state-mutating events.

**Constants.**
The per-header-reader limit of 5 is a constant, not configuration, because nothing outside two tests varied it.

## Code

Package `broadcast`: `engine.go`, `channel.go`, `protocol.go`, `wire.go`, `errors.go`, `observer.go`, `types.go`, `peer.go`, `peer_in.go`, `peer_ctrl.go`, `queue.go`, `events.go`, `session.go`; schemas in `broadcast/pb/broadcast.proto`.
No second implementation exists, so interoperability is untested.
Tests: `wire_test.go`, `peer_test.go`, `lifecycle_test.go`, `session_test.go` (stages, disposal), `outcome_test.go`, `capacity_test.go` (flow control, bounds), `delivery_test.go`, `shutdown_test.go`, `engine_test.go`; end-to-end `e2e/publish_test.go`, `relay_test.go`, `relay_hold_test.go`, `subscription_test.go`, `subscription_hold_test.go`, `session_routing_test.go`, `churn_test.go`, `lifecycle_test.go`.
The harness and its devhooks are in `docs/design/e2e-testing.md`.

- Register once, before the stack starts, under the lock shutdown takes.
  On each wake drain the protocol queue until empty.
  Key bindings by the stack's stable per-peer handle, not peer ID, and make every cleanup event carry its binding and act only if it is still current.
  Closing a binding waits for its goroutines and runs on the engine actor, so every worker that posts to the engine also selects on its own peer context, or peer-down deadlocks.
- Require a buffered subscriber channel installed before peers connect; deliver with a non-blocking send; the channel closes the feed.
  Notify the observer of a decoded object whether or not the subscriber took it.
- Attachment is asynchronous; a duplicate ID cancels the new channel and reports to the observer, not the caller.
  On stop, wait for the channel actor to exit before asking the engine to drop it.
- Every send into the channel inbox (1024, finite) selects on the channel's lifetime.
  Anything slow runs off the actor and re-enters as an event: payload reads, decoding, async verification, publish encoding.
  After the channel's context ends the actor drains the inbox and cancels buffered chunk streams; a producer whose send lands after the final drain must notice and cancel its own stream.
- Map a cancel cause to a code in one place: deadline (`Timeout`), redundancy cause (`Redundant`), refusal cause (`Refused`), otherwise `Unspecified`.
  The first cancellation wins.
- Order the stages as integers (origin after the relay stages).
  Set Decoding before starting the decode goroutine, and consult `HaveChunk` only in Consuming.
  The decode goroutine returns its result as an event and abandons the post if the session ends; act on it only while the stage is before Reconstructed.
- Do publish encoding on the caller's goroutine, keeping only registration and attachment on the actor.
  Dispose a same-ID session before registering the origin session so its commands queue first.
- Attachment is idempotent per peer ID; the channel unbinds an ID before binding its next connection.
  The session-ended event carries the owning connection and is dropped on mismatch.
  In-flight chunk writes to a removed peer finish; strategy bookkeeping must tolerate that.
- Idleness is evaluated in the channel actor after each event that can change it.
  Disposal blocks the actor until background work ends; a running decode cannot be cancelled, so a long decode delays disposal.
  The TTL is measured from creation with a periodic sweep, so a session can live one sweep (30 s) beyond 5 minutes.
  Close the strategy last.
- Strategy: keep `Decode`'s inputs separate from everything else.
  Only chunk intake is frozen at completeness; routing, attach/detach, polls and send outcomes keep arriving while `Decode` runs, and `HaveChunk` must be read-only.
  The poll after an event is the only trigger for work; mark routing dirty only inside the calls the contract names.
  `PollRouting` with force is consumed once per attached peer, at attach.
  `ChunkSent` arrives on the channel context after `PollChunks` returned.
  Key in-flight bookkeeping on the dispatch handle and keep it unique among sends in flight to one peer.
  `NewOrigin` runs on the publisher's goroutine; `NewRelay` and the value constructors run on the channel context and must not block, and the value constructors return fresh values.
  A preamble is kept as the exact received bytes and re-sent unchanged to peers attached later.
  Use the dedup handle only during the `TakeChunk` call that received it.
  Make `VerifyChunk` a pure function of construction-time state; for async return `VerdictPending` and write to `Verified`, which a channel-owned watcher forwards to the inbox and `Close` closes.
  The chunk ID in a result must round-trip through marshal and unmarshal.

Pitfalls:

- Actors own their maps and communicate by messages; a direct read of another actor's map races.
  The exceptions: the per-peer channel table is a copy-on-write snapshot read atomically, and the protocol handle is atomic.
- Engine and channel actors must not wait on a peer writer, stream credit or each other.
- Reading past a frame corrupts the next one; trusting a length allocates attacker-chosen memory.
  Read each uvarint a byte at a time, reject a length above the limit before allocating, and write each frame in one call, except that `DATA`'s type and length may go with the `HEADER` before its payload.
- A binding that waits for its workers on close can miss one started concurrently; make worker start
  and the context check atomic with close, for example with a lock close takes and releases first.
- A peer that reconstructed is still a chunk source; marking it completed stops sends to it,
  removing it would cancel its chunk streams.
- A stream handed to a queue is owned by the queue until read or cancelled; peer unbind, session
  disposal, channel stop and shutdown drain must each cancel what they hold.
- The session refuses chunks by stage, but the strategy must enforce its own completeness.
- The session queues the slice `PollChunks` returned for writing after the call returns, so the
  strategy must not modify it.
- The per-peer data loop walks a map to pick the next session's slot, so session order is undefined.
