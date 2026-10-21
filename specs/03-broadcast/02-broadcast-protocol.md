---
status: implemented
requires:
  - 01-broadcast-framework.md
---

# Broadcast protocol

Broadcast is protocol 1, the only protocol the stack routes for broadcast traffic.
Its stream kinds are control (1), session (2) and chunk (3): a kind is one minimal uvarint after the protocol header on every broadcast stream, and after the protocol header on every broadcast datagram ([Datagrams](../01-transport/05-datagrams.md)).
Frame types are numbered per kind.
The stack routes and bounds broadcast as one protocol; broadcast classifies its own streams by kind, and a receiver MUST refuse a kind it does not define with `Refused`.
Unknown protocols are the stack's business, not broadcast's: the stack resets them with `UnsupportedProtocol` ([Streams](../01-transport/02-streams.md#routing-streams)).
Stream headers and frames are defined in [Streams](../01-transport/02-streams.md); the roles, the shared framing, the outcome codes and the session lifecycle are in [Broadcast framework](01-broadcast-framework.md).
The meaning of MUST and SHOULD is in [Conventions](../00-foundations/02-conventions.md); defined terms are in [Terminology](../00-foundations/03-terminology.md).

## Wire format

### Kinds

```text
broadcast_stream   = stream_header(1) || uvarint(kind) || *frame
broadcast_datagram = protocol_header(1) || uvarint(kind) || payload
```

| Kind | Number | Stream header | Frame types |
| ---- | -----: | ------------- | ----------- |
| control | 1 | `01 01` | `HANDSHAKE` (0), `SUBSCRIBE` (1), `UNSUBSCRIBE` (2) |
| session | 2 | `01 02` | `OPEN` (0), `UPDATE` (1) |
| chunk | 3 | `01 03` | `HEADER` (0), `DATA` (1) |

The headers are read as protocol 1, kind n: `01 01` is a control stream, `01 02` a session stream and `01 03` a chunk stream.

Broadcast registers one stack queue bound, `MaxQueued` (193), shared across all three kinds.
That sharing is a benefit, not just a saving: because the pool is shared and broadcast drains it, broadcast can order its own draining (control ahead of session ahead of chunk) with full knowledge of the protocol, instead of the stack imposing three FIFO queues whose relative priority it cannot know.

## Control kind

Before any broadcast traffic, each side announces the channels it has attached, and a node includes a peer in a channel's sessions exactly while the peer is subscribed.
The control kind, kind 1 of the broadcast protocol, carries that: a `HANDSHAKE` frame, then `SUBSCRIBE` and `UNSUBSCRIBE` frames.
Each side opens one unidirectional control stream toward the other; a side writes only on the stream it opened and reads only on the stream it accepted.
Two unidirectional streams mean simultaneous open is the normal case, so neither side is initiator or responder and no tie-break exists.
Stream headers and frames are defined in [Streams](../01-transport/02-streams.md); the payload limit and outcome codes are in [Broadcast framework](01-broadcast-framework.md).
The meaning of MUST and SHOULD is in [Conventions](../00-foundations/02-conventions.md); defined terms are in
[Terminology](../00-foundations/03-terminology.md).

### Wire format

#### Stream

```text
control_stream = stream_header(1) || uvarint(1) || HANDSHAKE || *(SUBSCRIBE | UNSUBSCRIBE)
```

| Type | Name | Payload | Allowed |
| ---: | ---- | ------- | ------- |
| 0 | `HANDSHAKE` | `Bcast.Handshake` | First frame, exactly once |
| 1 | `SUBSCRIBE` | `Bcast.Subscribe` | After `HANDSHAKE`, any number |
| 2 | `UNSUBSCRIBE` | `Bcast.Unsubscribe` | After `HANDSHAKE`, any number |

From `broadcast/pb/broadcast.proto`:

```protobuf
message Bcast {
  message Handshake {
    uint32 version = 1;
    repeated string channels = 2;
  }
  message Subscribe { string channel = 1; }
  message Unsubscribe { string channel = 1; }
}
```

#### `HANDSHAKE`

`HANDSHAKE` states the sender's version and the channels it has attached.

| Field | Type | Constraint |
| ----- | ---- | ---------- |
| `version` | `uint32` | MUST be at least 1; the sender MUST send 1. |
| `channels` | `repeated string` | Channel IDs the sender has attached; `MAX_BROADCAST_FRAME` (1 MiB) bounds the list. |

#### `SUBSCRIBE`

`SUBSCRIBE` asks the receiver to include the sender in a channel.

| Field | Type | Constraint |
| ----- | ---- | ---------- |
| `channel` | `string` | Channel the sender wants to be included in. |

#### `UNSUBSCRIBE`

`UNSUBSCRIBE` asks the receiver to stop including the sender in a channel.

| Field | Type | Constraint |
| ----- | ---- | ---------- |
| `channel` | `string` | Channel the sender no longer wants to be included in. |

#### States

```text
sender                                   receiver
  | -- HANDSHAKE(version=1, channels) --> |
  | -- SUBSCRIBE(channel) --------------> |    ; zero or more, any order
  | -- UNSUBSCRIBE(channel) ------------> |
```

Both sides run this concurrently on their own outbound stream.

| State | Allowed frames | Next state |
| ----- | -------------- | ---------- |
| Start | `HANDSHAKE` | Open |
| Open | `SUBSCRIBE`, `UNSUBSCRIBE` | Open |

Any other frame type is refused ([Behavior](#behavior)).

#### Constants

| Constant | Value |
| -------- | ----: |
| `Handshake.version` | 1 |
| Handshake deadline | 10 seconds |
| session handoff queue before the handshake result | 64 streams |

#### Examples

Channel `blocks` is `62 6c 6f 63 6b 73`.
A handshake and a later subscription:

```text
01 01                    stream header: protocol 1, kind 1 (broadcast control)
00                       type 0 (HANDSHAKE)
0a                       length: 10 bytes follow
08 01                    Handshake field 1 (version), value 1
12 06                    Handshake field 2 (channels), length 6
62 6c 6f 63 6b 73        "blocks"
01                       type 1 (SUBSCRIBE)
08                       length: 8 bytes follow
0a 06                    Subscribe field 1 (channel), length 6
62 6c 6f 63 6b 73        "blocks"
```

### Behavior

#### Handshake

When the stack reports the peer, each side MUST:

1. open its outbound control stream, exactly one per binding, and write one `HANDSHAKE` frame with
   version 1 and the channels it has attached at that moment;
2. read one `HANDSHAKE` frame from the peer's inbound control stream.

The write and the read MUST overlap: both frames may exceed the initial stream window (only `MAX_BROADCAST_FRAME` (1 MiB) bounds the channel list), and a write completes only as the peer reads, so write-then-read deadlocks when both frames are large.
The whole exchange MUST complete within 10 seconds.
A failure of either half cancels the other, and both streams are cancelled with `Timeout` for a deadline, `Refused` for a protocol violation, `Unspecified` otherwise.
A failed handshake leaves the application's connection untouched; the binding ends and the peer never joins any channel.

#### Validation

- A second inbound control stream MUST be cancelled with `Refused`.
- A first frame other than `HANDSHAKE`, a later frame other than `SUBSCRIBE` or `UNSUBSCRIBE`, or an unknown frame type is a protocol violation, and the stream MUST be cancelled with `Refused`.
- A `HANDSHAKE` with `version` below 1 fails and MUST be cancelled with `Refused`.
- A `version` above 1 MUST be accepted and treated as version 1.
- Duplicate channel IDs in `channels` MUST be collapsed to one.

#### Result

After a successful handshake a node records the peer's declared channels, binds the peer to each declared channel it has attached locally, and reports the peer to the observer.
A channel the peer declares but the node has not attached is remembered, and the peer is enrolled when the channel is attached later.
Only then does the node process the peer's session and chunk streams and subscription messages.
Streams that arrive earlier wait in handoff queues:

| Queue | Bound | Overflow |
| ----- | ----- | -------- |
| control | One stream. | Cancelled with `Refused`. |
| session | 64 streams. | Cancelled with `Unspecified`. |
| chunk | None locally: each queued stream is unread and holds QUIC credit, so the peer's stream limit bounds it. | n/a |

The stack applies `MaxQueued` before these queues.
When the binding closes, every queued stream MUST be cancelled with `Unspecified`.

#### Subscription

After the handshake a peer MAY send `SUBSCRIBE` or `UNSUBSCRIBE` at any time.

- On `SUBSCRIBE` a receiver SHOULD include the sender in the channel: attach it to every active session on that channel and open a session stream to it for each.
  Chunks already sent are not replayed; the new session carries the current routing state.
- On `UNSUBSCRIBE` a receiver SHOULD stop including the sender: close its session streams for that
  channel, drop it from the sessions as departed, and cancel its chunk streams still parked for that
  channel.
- A `SUBSCRIBE` for a channel already subscribed, or an `UNSUBSCRIBE` for one not, has no effect.
- A violation on the control stream MUST cancel the stream with `Refused` and end the binding.

A session or chunk stream naming a channel the node has not attached, or that the sender is not subscribed to, MUST be cancelled with `Refused`.

#### Announcements

When the application attaches a channel the node announces a `SUBSCRIBE` to every peer whose handshake completed; when it stops one it announces an `UNSUBSCRIBE`.
Announcements MUST keep their order relative to session opens and closes sent to the same peer.
The channel list in a node's `HANDSHAKE` is a snapshot taken when the binding was created.
Channels attached or stopped between that snapshot and handshake completion MUST be announced right after the handshake as the difference: a `SUBSCRIBE` for each added, an `UNSUBSCRIBE` for each removed.
A peer whose handshake finishes later thus converges on the same state as one already connected.

#### Queue bounds

Producing an announcement or session lifecycle command MUST NOT block on the peer's control writer.
Pending entries per peer are bounded by the attachments that are live or awaiting retirement, plus local publishes, plus at most two announcements per channel ID.
Two rules keep this:

- An `UNSUBSCRIBE` removes a still-queued `SUBSCRIBE` for the same channel; the remote never saw the subscribe.
  The reverse pair, `UNSUBSCRIBE` then `SUBSCRIBE`, is kept: it tells the remote to retire the local peer from the old channel instance and enrol it in the new one.
- A session close removes the session's still-queued open and the close-streams queued after it.

Routing updates use a separate, smaller queue; a full queue drops the update, since routing is advisory and the next update supersedes it.

### Failure modes

#### No handshake

A peer that never completes it is bounded by the 10 second deadline.

#### Violation

It ends only that peer's binding.

#### Channel disclosure

`HANDSHAKE` and `SUBSCRIBE` reveal the channels a node has attached.

#### Subscribing others

Membership follows the peer's own control stream, so a peer cannot subscribe another peer.

#### Early streams

Bounded by the handoff queues above and the stack's `MaxQueued`.

### Rationale

#### Overlapping write and read

A sequential write-then-read assumes the frame fits the initial stream window, but the transport leaves quic-go's 512 KiB window in place, frames may reach 1 MiB, and neither the channel count nor the ID length is bounded.
With large frames on both sides each write would wait for credit the other had not granted, and the 10 s timeout would kill broadcast on that connection.

#### Workers start after the handshake

The handoff queues already hold early streams without blocking the engine actor, so workers do not park until the handshake completes.
A failed handshake therefore cancels queued session and chunk streams with `Unspecified` instead of `Refused`; the failure only ends the binding, whose deferred stop disposes queued streams.
The negotiated version was always 1, so none is stored per binding.

### Code

Package `broadcast`: `peer.go`, `peer_in.go`, `peer_ctrl.go`, `queue.go`, `protocol.go`.
Tests: `peer_test.go`, `lifecycle_test.go`, `wire_test.go`; `e2e/subscription_test.go`, `subscription_hold_test.go`.
Tests that check the frame rules: `frames_test.go`.
The example was produced with `wire.AppendSelector`, `wire.AppendFrame` and the `broadcast/pb` encoder.

- Handshake: run the write on its own goroutine and the read on the caller, under one context with the 10 s deadline counted from the start, covering opening the stream (which can wait for credit).
  Whichever half fails cancels the shared context; join the writer on every exit path; plain reads and writes never cancel a stream, so each failing half cancels its own stream with the code from the error.
- Validation: admit the inbound control stream on the single actor that owns the binding's admission flag and hand it over through a one-slot queue, so the hand-over never blocks.
  Compare the version with "below 1", not "not equal to 1".
  Collapse duplicate channel IDs before enrolling.
- Result: do not start session, chunk and subscription workers until the engine has recorded the peer's channels and bound it, or early streams route to unbound channels and get refused.
  The sequence: handshake finishes, result posts to the engine, the engine records and binds, then signals the binding to start workers; the engine re-checks the binding is live, since peer-down can arrive in between.
  Enqueueing never blocks the actor that receives streams from the stack: a full session queue cancels the new stream at once.
  Closing a binding: stop workers first, then cancel everything left in the queues, on the enqueuing actor.
- Subscription: apply on the actor that owns subscriptions, ignore unless the binding is current for the peer ID, check the recorded set first.
  Binding a peer to a channel publishes it in a copy-on-write table that stream readers read without locks; that table is also what makes a stream for an unsubscribed channel fail lookup and be refused.
- Announcements: one ordered queue per peer read by one writer, or a close can overtake its open.
  Compute the post-handshake difference on the engine actor so no attach or stop is missed.
- Queue bounds: the lifecycle queue never blocks its producer and drops entries once the binding is stopped; apply the elision rules under the queue lock; a close-stream alone must keep the open, since the outbound chunk slot is still needed.
  The routing queue holds 16 events and uses a non-blocking send.

Pitfall: starting workers while closing; see [Broadcast framework](01-broadcast-framework.md#code).

## Session kind

A relay can build its coding state from the first frame of a session stream, before any chunk arrives.
Each peer opens one unidirectional session stream toward the other for a given session.
Two peers cooperating on an object hold two streams, one per direction; each carries only the opener's state, and only the opener writes.
The stream carries the session open with the preamble and initial routing, later routing updates, and the signal that the sender has reconstructed the object.
The session kind is kind 2 of the broadcast protocol.
Stream headers and frames are defined in [Streams](../01-transport/02-streams.md); the payload limit, outcome codes and the session stages are in
[Broadcast framework](01-broadcast-framework.md).
The meaning of MUST and SHOULD is in [Conventions](../00-foundations/02-conventions.md); defined terms are in
[Terminology](../00-foundations/03-terminology.md).

### Wire format

#### Stream

```text
session_stream = stream_header(1) || uvarint(2) || OPEN || *UPDATE   # then FIN or a reset of the write side
```

| Type | Name | Payload | Allowed |
| ---: | ---- | ------- | ------- |
| 0 | `OPEN` | `Sess.Open` | First frame, exactly once |
| 1 | `UPDATE` | `Sess.Update` | After `OPEN`, any number |

The stream ends with FIN, or with a reset of the write side: `Reconstructed` for completion, other codes for other causes.
From `broadcast/pb/broadcast.proto`:

```protobuf
message Sess {
  message Open {
    string channel = 1;
    string object_id = 2;
    bytes preamble = 3;
    bytes initial_update = 4;
  }
  message Update {
    bytes data = 1;
  }
}
```

#### `OPEN`

`OPEN` names the session and carries what a relay needs to build it.

| Field | Type | Constraint |
| ----- | ---- | ---------- |
| `channel` | `string` | Channel ID. |
| `object_id` | `string` | Object ID. |
| `preamble` | `bytes` | Scheme-specific; the relay builds its strategy from it. |
| `initial_update` | `bytes` | Optional; same format as `Update.data`; the opener's routing state as of attachment. |

#### `UPDATE`

`UPDATE` carries a change in the opener's routing state.

| Field | Type | Constraint |
| ----- | ---- | ---------- |
| `data` | `bytes` | Scheme-specific opaque routing state. |

#### States

| State | Allowed | Next state |
| ----- | ------- | ---------- |
| Start | `OPEN` | Streaming |
| Streaming | `UPDATE` | Streaming |
| Streaming | FIN or write reset | Ended |

Any other frame type is refused ([Behavior](#behavior-1)).

```text
opener                                       receiver
  | -- OPEN(channel, object_id, preamble, initial_update) -> |
  | -- UPDATE(data) ---------------------------------------> |   ; zero or more
  | -- FIN | reset(Reconstructed) | reset(other) ----------> |
```

| Ending | Meaning |
| ------ | ------- |
| reset `Reconstructed` (`0x03`) | The opener has enough chunks to decode. |
| FIN | The opener disposed its session. |

#### Examples

Object `m1` (`6d 31`) on channel `blocks`, preamble `aa bb`, initial routing `01`, then one update `05`:

```text
01 02                    stream header: protocol 1, kind 2 (broadcast session)
00                       type 0 (OPEN)
13                       length: 19 bytes follow
0a 06 62 6c 6f 63 6b 73  Open field 1 (channel), "blocks"
12 02 6d 31              Open field 2 (object_id), "m1"
1a 02 aa bb              Open field 3 (preamble)
22 01 01                 Open field 4 (initial_update)
01                       type 1 (UPDATE)
03                       length: 3 bytes follow
0a 01 05                 Update field 1 (data), routing state 05
```

### Behavior

#### Sending an open

A node opens a session stream to a peer when it attaches that peer to a session: when a session is created and the peer is already subscribed, or when a peer subscribes or its handshake completes while the session is active.
Attachment is idempotent per peer.
The `OPEN` MUST carry the channel, object ID and the session's preamble.
It also carries `initial_update`, the strategy's routing state as of attachment, so the peer has the sender's inventory before any chunk arrives and no round trip is spent on redundant sends.
The stream stays open for the session's lifetime.

#### Receiving an open

- A first frame other than `OPEN`, a later frame other than `UPDATE`, or an unknown frame type MUST cancel the stream with `Refused`.
- If the named channel is not attached locally, or the sender is not enrolled in it,
  the stream MUST be cancelled with `Refused`.
- If a session for `(channel, object_id)` already exists, no second session is created, but the `initial_update` MUST still be applied as a routing update from the opener (it may have connected after receiving shards); later `UPDATE` frames are processed as usual.
  The opener is not attached by this: attachment follows channel membership.
- If no session exists, the receiver creates a relay session: unmarshals the preamble, builds the strategy, attaches the channel's current members, applies `initial_update` from the opening peer, and only then processes chunk streams parked for this session.
  A preamble that does not unmarshal is rejected at session creation.
  The receiver MUST apply the `initial_update` of every `OPEN`, new session or not, before any chunk data it carries, so the strategy knows the opener's inventory before deciding what to send.
- If the preamble or strategy fails to build, no session is created and an error goes to the observer.
  Parked chunks for that object stay parked until the cleanup interval ([chunk kind](#chunk-kind)).

#### Routing updates

After `OPEN` the sender MAY write `UPDATE` frames at any time.
The framework MUST pass `data` to the strategy uninterpreted.
Malformed data is reported to the observer and skipped; the stream stays open.
Routing updates are not validated: a peer can advertise false inventory.
The session sends routing to every attached peer that has not completed, after each state change the strategy reports.
It MUST queue routing before dispatching the chunks that the same poll produced, so peers usually learn the sender's state before its chunks arrive; wire ordering is not guaranteed, because control and chunk traffic are written independently.
An update to a peer whose control queue is full is dropped; the next update supersedes it.
A routing update from a peer lets the strategy name in-flight sends to that peer that are now redundant; the session cancels those.

#### Completion

When a session's strategy reports it can decode, the node MUST cancel the write side of its outbound session stream to each peer with `Reconstructed` and stop sending routing to them.
This happens before decoding finishes, because decode failure is non-recoverable and the node accepts no further chunks.
A clean end (FIN) means the sender disposed its session.
A node that reads a session stream reset with `Reconstructed` marks that peer completed in the session: it cancels in-flight chunk sends to the peer and tells the strategy the peer detached with `completed = true`.
The peer is not removed; it stays a source of chunks on other streams.
A peer leaves a session when its inbound session stream ends by FIN, by a reset with any other code, or by a read error, and when it disconnects or the channel goes away.
If the connection is going away, the unbind covers the removal and no separate event is sent.

### Failure modes

#### Slot exhaustion

A session reader holds one of 64 per-peer slots; a stream beyond that is cancelled with `Unspecified`.

#### Unknown channel

Cancelled with `Refused`.

#### Bad preamble

No session is created; the opener is not penalised.

#### Session flood

Nothing but the 5 minute TTL bounds the relay sessions a peer can open on a channel it is subscribed to ([Broadcast framework](01-broadcast-framework.md#failure-modes)).

#### False routing and false completion

A peer can advertise false inventory or signal `Reconstructed` falsely; the cost is bandwidth.

#### Disclosure

`OPEN` reveals the preamble to every subscribed peer, which is its purpose.

### Rationale

#### Initial update in `OPEN`

The peer has the sender's inventory before any chunk, so no round trip is spent on redundant sends.
Every opener's `initial_update` is applied even when the session exists, because the opener may have connected after receiving shards.

#### Completion by write reset

`Reconstructed` is signalled before decoding finishes, and by a reset code rather than a frame, so it cannot queue behind routing updates.
`Reconstructed` marks the peer completed instead of removing it, since a completed peer may still send chunks.

### Code

Package `broadcast`: `peer_ctrl.go` (outbound writer), `peer_in.go` (readers), `session.go`, `channel.go`.
Tests: `session_test.go`, `peer_test.go`, `outcome_test.go`; `e2e/session_routing_test.go`, `relay_test.go`.

- Sender: take the initial routing snapshot at attachment, before queueing the open, and carry it in the queued open so snapshot and attachment are one step.
  The outbound writer opens the stream and writes `OPEN` when it dequeues that command, and only then hands the session's chunk slot to the data writer: that order guarantees `OPEN` precedes every chunk and routing frame.
  A failure to open or write `OPEN` ends the whole binding.
- Receiver: read `OPEN` and check the channel in the peer's lock-free table before touching the channel actor; deliver with a select on both the channel's and the binding's lifetime.
  Create a relay session in this order: build the strategy (fail without registering), register, attach current members, apply `initial_update`, flush parked chunk streams.
  Apply `initial_update` even when the session exists, and never let it attach the opener.
- Routing: run the poll-for-routing step before the poll-for-chunks step after every state change.
  Queue routing with a non-blocking send into the peer's small routing queue; never wait for space, since a session blocking on a peer writer whose work depends on that session is a circular wait.
  Cancel in-flight sends keyed on (peer, strategy handle).
  A write failure on a routing stream cancels it and removes it from the writer's table, so later updates are skipped silently.
- Completion: order the signal through the same per-peer ordered queue as opens and closes, as a close-stream command that cancels the write side with `Reconstructed` while leaving the chunk slot, so chunks keep flowing; final disposal removes the slot.
  On the reader, a reset with `Reconstructed` raises a completion event (not a removal); FIN, any other reset and any read error raise a peer-session-ended event, skipped when the binding's context is done.

Pitfall: an `OPEN` from a peer that already left the session must only apply its routing; attachment follows channel membership.

## Chunk kind

Each chunk travels on its own short-lived unidirectional chunk stream, and only the sender writes.
Header and data are separate so a receiver can cancel after the header, before any data is transferred.
Back-pressure is QUIC stream credit: a receiver that does not want or cannot process a chunk does not read it, and a stream is cancelled, not delayed, only when the receiver knows it does not want the data.
The chunk kind, kind 3 of the broadcast protocol, is how a receiver admits, reads and verifies chunks, how a sender dispatches them, and the bounds on every place a chunk stream can wait.
Stream headers and frames are defined in [Streams](../01-transport/02-streams.md); the payload limit, outcome codes, session stages and the strategy contract are in
[Broadcast framework](01-broadcast-framework.md).
The meaning of MUST and SHOULD is in [Conventions](../00-foundations/02-conventions.md); defined terms are in
[Terminology](../00-foundations/03-terminology.md).

### Wire format

#### Stream

```text
chunk_stream = stream_header(1) || uvarint(3) || HEADER || DATA   # then FIN
```

| Type | Name | Payload | Allowed |
| ---: | ---- | ------- | ------- |
| 0 | `HEADER` | `Chunk.Header` | First frame, exactly once |
| 1 | `DATA` | The chunk's bytes, not a message | After `HEADER`, exactly once, last frame |

From `broadcast/pb/broadcast.proto`:

```protobuf
message Chunk {
  // Header is the payload of the HEADER frame. data_length announces the
  // payload length of the DATA frame that follows.
  message Header {
    string channel = 1;
    string object_id = 2;
    bytes chunk_id = 3;
    uint32 data_length = 4;
  }
}
```

#### `HEADER`

`HEADER` names the chunk and announces its length, so a receiver can decide whether it wants the data before reading it.
Its payload limit is `MAX_BROADCAST_FRAME` (1 MiB) ([Broadcast framework](01-broadcast-framework.md)).

| Field | Type | Constraint |
| ----- | ---- | ---------- |
| `channel` | `string` | Channel of the chunk. |
| `object_id` | `string` | Object of the chunk. |
| `chunk_id` | `bytes` | Strategy-specific chunk identifier, opaque to the framework. |
| `data_length` | `uint32` | At most 1 MiB. The payload length of the `DATA` frame that follows. |

#### `DATA`

`DATA` carries the chunk's bytes.
Its payload length MUST equal `Header.data_length`, and FIN MUST follow it, so the receiver can allocate exactly `data_length` bytes before it reads them.

#### States

| State | Allowed | Next state |
| ----- | ------- | ---------- |
| Start | `HEADER` | Data |
| Data | `DATA` of exactly `data_length` bytes | Done |
| Done | FIN | n/a |

Any other frame type, a `DATA` of another length, FIN before `DATA`, or a frame after `DATA` is refused ([Behavior](#behavior-2)).

```text
sender                               receiver
  | -- HEADER ---------------------> |
  | -- DATA (data_length bytes) ---> |   ; receiver MAY cancel its read side at any point
  | -- FIN ------------------------> |
```

#### Codes

| Code | Wire | Sent by | Meaning on a chunk stream |
| ---- | ---: | ------- | --------------------------- |
| `Redundant` | `0x05` | Receiver, read side | The chunk is no longer needed. |
| `Refused` | `0x02` | Receiver, read side | The receiver will not process the stream. |
| `Timeout` | `0x04` | Either side | A deadline expired. |
| `Unspecified` | `0x00` | Either side | No specific reason. |

#### Constants

| Constant | Value |
| -------- | ----: |
| Header read deadline | 5 seconds |
| Payload read deadline | 5 seconds |
| Send deadline (open and write, one budget) | 5 seconds |
| Header readers per peer | 5 |
| Parked streams per object per channel | 32 |
| Payload reads per session | 64 |
| Outbound slot per peer and session | 16 entries |
| Parked-stream sweep | 30 seconds |

#### Examples

A 4-byte chunk for `m1` on channel `blocks` with the illustrative chunk identifier `00 03`:

```text
01 03                    stream header: protocol 1, kind 3 (broadcast chunk)
00                       type 0 (HEADER)
12                       length: 18 bytes follow
0a 06 62 6c 6f 63 6b 73  Header field 1 (channel), "blocks"
12 02 6d 31              Header field 2 (object_id), "m1"
1a 02 00 03              Header field 3 (chunk_id)
20 04                    Header field 4 (data_length), value 4
01                       type 1 (DATA)
04                       length: 4 bytes follow, equal to data_length
de ad be ef              chunk data
                         FIN
```

A receiver that already holds the chunk cancels its read side with `Redundant` (`0x05`) after `HEADER` and before `DATA`.

### Behavior

#### Receiving a header

A receiver MAY cancel a chunk stream at any time, with `Redundant`, `Refused`, `Timeout` or `Unspecified`; if it cancels before the sender finishes writing, the sender sees a write failure.
A receiver accepts chunk streams in arrival order, reads the `HEADER` frame with a 5-second deadline, limits header reads in progress per peer, and routes the stream to the channel named by `channel`.

- A first frame other than `HEADER`, including an unknown type, MUST cancel the stream with `Refused`.
- A `data_length` above 1 MiB MUST cancel the stream with `Unspecified`.
- If the channel is not attached or the peer is not enrolled in it,
  the stream MUST be cancelled with `Refused`.
- If the channel has ended, the stream MUST be cancelled with `Unspecified`.
- A read error MUST cancel the stream with the code the error implies: `Timeout` for an expired
  deadline, `Unspecified` otherwise.

#### Parking

The channel handles the routed stream without reading its data.
If the session exists, the chunk is admitted (see Admission below).
If it does not (the chunk beat its session open), the stream is parked, still unread, until the session appears.
At most 32 streams are parked per object per channel; a stream beyond that MUST be cancelled with `Unspecified`.
There is no per-peer bound: a peer's QUIC stream limit bounds what it can park.
When the session appears, parked streams are handled in arrival order after the opener's initial routing is applied.
Parked streams from a peer MUST be cancelled with `Unspecified` when the peer leaves the channel.
Every 30 seconds the channel cancels every parked stream with `Refused`; a session disposal cancels its object's parked streams too.

#### Admission

For a chunk on an existing session the session decides, without reading data:

1. The chunk ID fails to unmarshal: cancel with `Unspecified`, report to the observer.
2. The session is past Consuming, or `HaveChunk` reports the chunk already held: cancel with
   `Redundant` and record `VerdictRedundant`, `VerdictDecoding` or `VerdictSurplus` per the session
   stage.
3. Otherwise the chunk is read.
   At most 64 payload reads run concurrently per session; further chunks wait unread in a session queue, still holding QUIC credit, and start as reads finish.

The receiver reads one `DATA` frame and then the stream's FIN.
A payload read has a 5-second deadline that starts when the read starts.
The receiver MUST cancel the stream with `Refused` if FIN arrives where `DATA` belongs, if the frame is not `DATA`, if its length differs from `data_length`, or if any byte follows it, so a sender that disagrees with its own header is never half-read.
A read that fails otherwise is cancelled with the code its error implies; a `DATA` frame cut short by FIN is malformed and cancelled with `Unspecified`.

#### Deduplication

The strategy may group concurrent reads with a dedup key, for example all in-flight copies of one Reed-Solomon shard.
Reads in a group share a cancellation context.
When the strategy decides the group is satisfied, the remaining reads are cancelled with `Redundant`, freeing stream credit and connection flow-control budget at once.

#### Verification

A fully read chunk returns to the channel and:

1. `HaveChunk` is checked again; a chunk held meanwhile is counted redundant and dropped.
2. `VerifyChunk` returns `VerdictAccepted` (go to step 3), `VerdictPending`
   (the result arrives later and is processed the same way),
   or another verdict (the chunk is dropped and the verdict recorded).
3. `TakeChunk` gives the chunk to the strategy, which returns a verdict,
   an error and whether it can now decode; errors go to the observer.

A chunk enters strategy state only through `TakeChunk` after `VerdictAccepted`.
The framework applies no penalty to a peer that sends an invalid chunk.

#### Sending

The strategy returns dispatches, each naming a peer, a chunk ID and the data.
The session pushes each into a per-peer, per-session slot of 16 entries.
If the slot is full, or the peer is no longer attached, the strategy is told the send failed (`ErrChunkSlotFull`, `ErrChunkPeerGone`); the session never retries.
A node sends one chunk at a time to a given peer, taking the next from a different session's slot each time, so one object cannot starve another.
A send:

1. is skipped with `ErrChunkCancelled` if the strategy or a completing peer already cancelled it;
2. opens a chunk stream, waiting for QUIC stream credit;
3. writes `HEADER`, then `DATA`, and closes the stream.

Steps 2 and 3 share one 5-second deadline.
On failure the stream is cancelled (`Timeout` for a deadline, otherwise `Unspecified`) and the strategy hears `ErrChunkWriteFail`; on success it hears `ChunkSent` with `nil`.
Each dispatch MUST produce exactly one `ChunkSent`.

#### Flow control

Broadcast MUST NOT reset a stream merely because the receiver is busy.
A slow receiver does not read, which withholds credit from the sender.
Waiting happens only at these points, each leaving the stream unread and holding credit:

| Point | Bound |
| ----- | ----- |
| Stack queue for broadcast | `MaxQueued` 193 per peer, shared across kinds, applied by the stack. |
| Binding FIFO before a header reader | The peer's QUIC stream limit. |
| Header readers | 5 per peer. |
| Channel inbox | 1024 events per channel; a header reader waits to post. |
| Parked streams | 32 per object. |
| Session read queue | Unbounded locally; each entry holds a stream, so the QUIC stream limit bounds it. |
| Payload readers | 64 per session. |

A stream is cancelled, not delayed, only when the receiver knows it does not want the data: redundant, unknown channel or session state, over-limit session streams, parked overflow, or shutdown.
Cancelling costs the sender the bytes already written, not a retry.
The channel never waits on these limits: it queues work and is woken by a reader completion; a header reader waiting to post to a busy inbox is the intended pressure on that peer.
A sender feels backpressure as delay in opening streams and writing bytes, bounded by the 5-second deadline; a chunk that misses it is reported as a failed send and the strategy decides whether to retry.
A stalled peer delays the chunks queued behind it on that peer only.
No wait may depend on a resource that only a later stream can release: session streams are refused, not queued, past their slot limit; parked chunk streams are bounded per object so they cannot retain all the credit their own session stream needs; reads never wait on a channel that waits on them.
These bounds limit what a peer can make the node hold; they reserve no QUIC capacity.

### Failure modes

#### Oversized length

`data_length` above 1 MiB is cancelled before any allocation.

#### Declared but not sent

The payload buffer is allocated at admission, so a peer that declares 1 MiB and sends nothing pins it until the 5-second deadline: up to 64 MiB per session.

#### Park flood

Parked streams are 32 per object, and unread streams are bounded by the peer's QUIC stream limit.

#### Invalid chunks

Dropped and recorded; no peer penalty.

#### Disclosure

`Redundant` reveals that a node already holds a chunk; no overload signal exists, so load is not disclosed.

### Rationale

#### Header and data are separate frames

A receiver can cancel after `HEADER`, before any data moves.

#### `DATA` keeps the frame layout

`data_length` already says how long the data is, so a raw tail after `HEADER` would save the two bytes of `DATA`'s type and length.
Framing it keeps every byte after the stream header in a frame, so one reader serves every protocol, and the length check turns a sender whose header and data disagree into a refused stream instead of a misread one.

#### No chunk-stream reset for load

A reset for "busy" would leak load and hand the peer credit to open more streams; unread streams make the peer's own stream limit the bound.

#### Parked bound per object

It keeps parked chunks from retaining all the credit their own session stream needs; the bound reserves no capacity and does not by itself guarantee progress.

### Code

Package `broadcast`: `peer_in.go` (header readers, parking), `channel.go` (admission, parked lists), `session.go` (reads, dedup, sending), `peer_ctrl.go` (data loop), `types.go`, `observer.go`.
Tests: `frames_test.go` (frame order and `DATA` rules), `capacity_test.go` (flow control and bounds), `session_test.go`, `outcome_test.go`, `delivery_test.go`; `e2e/publish_test.go`, `relay_test.go`, `relay_hold_test.go`, `churn_test.go`.
Hooks compiled into test and debug builds can stall chunk reads, chunk writes and other points, which is how tests fill these queues deterministically without sleeping.

- Header: take a reader slot before starting the reader and wait when none is free; the deadline starts when a slot is taken, not at arrival, and is cleared once the header is read (a leftover deadline would cut the payload read short).
  Re-check the binding's context after the header read.
  Write `HEADER` with `DATA`'s type and length in one call, then the payload, so the payload is not copied.
  Hand the stream to the channel with a send that also selects on the channel's and the binding's lifetime; if the send succeeds but the channel's context has ended, cancel the stream yourself.
  The Go code checks `data_length` when the session admits the chunk, so a parked stream is checked when flushed.
- Parking: per-object list owned by the channel actor, flushed after the session exists and the opener's routing is applied.
  The 30 s sweep cancels every parked stream regardless of age (there is no creation timestamp), so a stream waits zero to one sweep.
- Admission: decide on the channel actor without I/O.
  Try a non-blocking payload-slot acquire; when none is free queue the stream unread, never wait on the actor.
  Each finished read releases its slot then posts a wake-up; the actor drains the queue on that event and on each new chunk stream, re-running full admission.
  Allocate after admission; start the deadline at the read.
  Pick the cancel code from the read error and the context cause.
- Dedup: create the group lazily at the first keyed read, derived from the session context.
  The session drops its map entry on cancel; a group the strategy never cancels lives until disposal.
- Verification: re-run `HaveChunk` on the actor when the read finishes.
  For `VerdictPending` forward results from `Verified` into the inbox from a channel-owned watcher.
  A pending result takes the same accept path.
- Sending: reserve the slot entry before registering the send's cancel function, and tell the strategy the outcome of any dispatch not queued, in the same call.
  Register the cancel function so a routing update or completion can cancel a queued send by (peer, handle); a send cancelled while queued reports `ErrChunkCancelled`.
  One data loop per peer sends one chunk at a time and re-reads slot registrations between chunks.
  Compute the 5 s deadline once before opening and apply it to the open, the write deadline and the cancel watcher.

Pitfall: the payload read runs under a watcher that cancels the stream when the read context ends.
If the reader also cancels on a read error while the watcher is live, the first cancellation wins and the stream may end with the wrong code.
Once the context has ended the watcher owns the cancel: stop and join it before the reader cancels, and before the slot is released.
