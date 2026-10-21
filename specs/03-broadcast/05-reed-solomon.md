---
status: implemented
requires:
  - 01-broadcast-framework.md
---

# Reed-Solomon strategy

The origin splits an object into k data shards, computes n-k parity shards, and sends the n shards as chunks; any k of them reconstruct the object.
An RS relay cannot recode: it forwards the exact shards it received.
Whether the next shard to arrive is new therefore depends on the origin producing enough distinct shards and on the topology spreading them, and as a node accumulates shards the chance that the next arrival is new drops (the coupon collector effect), so the last few shards dominate completion time.
The strategy counters this with three mechanisms: bitmap routing (peers advertise which shards they hold, so senders skip shards a peer has), least-allocated-first dispatch (shards offered to fewer peers go first), and a per-relay random tie-break (neighboring relays prefer different shards among equals).
Each shard is authenticated by a SHA-256 hash the origin puts in the preamble, so a relay checks every shard before forwarding it.
This document defines the preamble, chunk identifier, chunk payload and routing update, and how an origin and a relay encode, verify, dispatch and decode.
Verdicts and the strategy contract are in [Broadcast framework](01-broadcast-framework.md).
The meaning of MUST and SHOULD is in [Conventions](../00-foundations/02-conventions.md); defined terms (shard, bitmap, allocation count, forward count, forward multiplier) are in [Terminology](../00-foundations/03-terminology.md).

```text
   Origin                       Relay                       Peer
     |-- session open(preamble) --->|                          |
     |-- routing update(bitmap) --->|                          |
     |-- chunk(index, shard) ------>|                          |
     |                              |-- session open --------->|
     |                              |-- routing update ------->|
     |                              |-- chunk(index, shard) -->|
     |<-- routing update(bitmap) ---|<-- routing update -------|
```

Shards 0 to k-1 are the object bytes in order (the code is systematic); any k distinct valid shards reconstruct the object; a relay never creates a shard, it only forwards verified ones; a shard that fails verification never enters strategy state.

## Wire format

```abnf
preamble       = Preamble       ; protobuf, in Open.preamble
chunk-ident    = ChunkIdent     ; protobuf, in the chunk header
chunk-payload  = shard          ; shard-length bytes, the same for every shard of a session
routing-update = bitmap         ; raw bytes, in Update.data
bitmap         = *OCTET         ; bit i is bit (i mod 8) of byte (i div 8)
```

Shard index i is in 0 to n-1, with n = `num_data` + `num_parity`.
Schema `broadcast/rs/pb/rs.proto`:

```protobuf
syntax = "proto3";
package ethp2p.broadcast.rs;

message Preamble {
  // Number of data shards (k). Indices 0 through k-1.
  int32 num_data = 1;
  // Number of parity shards (n-k). Indices k through n-1.
  int32 num_parity = 2;
  // Original unpadded message size in bytes. Used to trim decoded output.
  int32 length = 3;
  // SHA-256 hash of each shard, indexed by shard index. Used for per-chunk
  // verification. Provisional; will be replaced by Merkle commitment or
  // builder signature.
  repeated bytes hashes = 4;
  // SHA-256 hash of the original message. Verified after decode.
  bytes hash = 5;
}

message ChunkIdent {
  int32 index = 1;
}
```

| Field | Type | Constraint |
| ----- | ---- | ---------- |
| `Preamble.num_data` | int32 | MUST be greater than 0. |
| `Preamble.num_parity` | int32 | MUST be at least 0. |
| `Preamble.length` | int32 | No schema constraint; checked at decode. |
| `Preamble.hashes` | repeated bytes | MUST have `num_data` + `num_parity` entries. |
| `Preamble.hash` | bytes | No schema constraint. |
| `ChunkIdent.index` | int32 | No schema constraint; checked on receipt. |

The preamble grows linearly with n, and no other limit applies to its size.
The routing update is the bitmap, a raw byte string with no envelope; bits beyond its length are unset.

| Default | Value |
| ------- | ----: |
| Bitmap threshold (percent of shards held) | 50 |
| Forward multiplier | 4 |

Examples (bytes in hexadecimal).
A preamble for a 5-byte object with k = 2, n-k = 1 and all-zero hashes begins:

```text
08 02                    num_data (field 1) = 2
10 01                    num_parity (field 2) = 1
18 05                    length (field 3) = 5
22 20 00 ...             hashes (field 4), first entry: 32 bytes
```

The complete preamble is 142 bytes: those 6, three hash entries of 34 bytes, and the object hash of 34 bytes.
The chunk identifier of shard 3 is `08 03`.
A bitmap of 10 shards with shards 0, 3 and 9 held is two bytes, `09 02`: bits 0 and 3 of byte 0, and bit 1 of byte 1.

## Behavior

**Encoding.**
The origin MUST encode the whole object when it publishes.
Shards 0 to k-1 MUST be the object bytes in order, the last data shard zero-padded to the shard length, and shards k to n-1 the parity shards.
It derives (k, n-k, shard length) from its configuration and the object length:

- With a fixed shard count (chunk length 0),
  k and n-k come from the configuration and the shard length is the object length divided by k,
  rounded up.
- With a fixed shard length (chunk length greater than 0), the shard length is the chunk length,
  k is the object length divided by it rounded up, and n-k equals k (a 2x code).
- When n is 256 or more the code library switches to the leopard algorithm, which works on 64-byte blocks, so the shard length MUST be a multiple of 64.
  The fixed-shard-count path rounds up to satisfy this; the fixed-shard-length path does not.

The origin MUST put the SHA-256 hash of every shard, and of the original object, in the preamble.
After encoding it holds every shard and its bitmap is all ones.
The publish fails if the encoder rejects the parameters.

**Verifying a preamble.**
A relay MUST validate the preamble when it creates the session, before allocating any per-shard state.
It MUST reject a preamble in which `num_data` is not greater than 0, `num_parity` is negative, or the number of `hashes` differs from `num_data` + `num_parity`.
A rejected preamble creates no session.

**Verifying a chunk.**
All shards of a session have the same length, and verification is synchronous.
A receiver MUST accept a chunk only if its index is in 0 to n-1 and the SHA-256 hash of its data equals the preamble hash for that index.
A chunk that fails is invalid and MUST NOT enter strategy state.

**Receiving a chunk.**

| Condition | Result |
| --------- | ------ |
| Index outside 0 to n-1 | Invalid. |
| Session already complete | Redundant. |
| Shard already held | Redundant. |
| Otherwise | Accepted. |

On acceptance the node MUST: store the shard and set its bit; mark routing dirty; make the shard available for dispatch with allocation count 0; cancel the shard's dedup group, which ends concurrent reads of the same shard from other peers; and record the sending peer as holding it.
A node MUST NOT request an out-of-range index.
The session is complete once the node holds k shards, reported in the same call that accepts the k-th.
After completion the node MUST NOT store another shard: every in-range chunk is redundant, even for an index never held.
Progress is (shards held, k).

**Decoding.**
On completion the node reconstructs the missing data shards, concatenates shards 0 to k-1, and trims to `length`.
It then hashes the output and compares it with `hash`.
A mismatch, or a `length` outside 1 to k times the shard length, fails the decode.
Decode failure is not recoverable for the session.

**Tracking peers.**
For each attached peer the node tracks a bitmap of the shards the peer holds and, per shard, the number of sends to that peer in flight.
A peer that detaches as completed keeps its state but MUST be excluded from dispatch; any other detach deletes it.

**Dispatching.**
Dispatch offers a shard to a peer one at a time.
A node MUST serve the shard with the lowest allocation count across all peers first, and breaks ties by priority:

- A relay's priority for shard i is `(seed ^ uint64(i) * 0x9E3779B97F4A7C15) >> 32`
  (Fibonacci hashing, unsigned 64-bit multiplication),
  with `seed` a random 64-bit value chosen once per session,
  so neighboring relays prefer different shards among equals without coordination.
- The origin has no seed and every priority is 0; the order among equally allocated shards is not
  defined, and an origin MUST NOT be assumed to offer shards in index order.

A shard is eligible for a peer when the peer's bitmap does not have it, no send of it to that peer is in flight, and, for a relay, its forward count is below the forward multiplier.
The origin skips the forward-count condition: it sends each shard to every peer that lacks it.
A relay MUST treat a forward multiplier of 0 or less as 4.
A peer receives at most one shard per poll.
Allocating a shard raises its allocation count and the peer's in-flight count for it.
When a send ends:

- On success the node MUST set the peer's bit,
  and for a relay MUST add one to the shard's forward count.
- On failure the node MUST clear the peer's bit, MUST NOT charge the forward count, and MUST undo the allocation count increase, so the shard regains its priority and can be offered to that peer again.
  A failed send is invisible to both counters, on origin and relay alike.
- A remote `Redundant` reset after a clean close is not a failure:
  the send completed and counts as sent.

A send fails when the chunk cannot be marshaled, the peer is not attached, the peer's outbound slot is full, the send is cancelled, opening the chunk stream fails or exceeds its deadline, or writing or closing the header or payload fails.

**Sending a bitmap.**
A node emits its bitmap when routing is dirty or the framework forces an update, and: with the bitmap disabled, never; with a threshold of 0 or less, always; otherwise once the percentage of the n shards held is at least the threshold.
Above the threshold every accepted shard makes routing dirty, so each leads to an update.
The origin starts dirty and holds every shard, so it emits its bitmap at once unless disabled.

**Receiving a bitmap.**
A receiver MUST treat bits beyond the bitmap's length as unset.
It MUST merge a bitmap from an attached peer into that peer's bitmap with a bitwise OR over the shorter length, and MUST cancel in-flight sends of shards now present in the peer's bitmap.
It MUST ignore an update from a peer that is not attached.

**Changes.**
The preamble, chunk identifier and bitmap have no extension points; a change an old implementation cannot process ships with an application protocol name change, as for the rest of the stack.
The per-chunk hashes are provisional: the schema comment names a Merkle commitment or a builder signature as replacements.
That alters the preamble and the verification rule and is made with a new strategy version, not as an extension of this one ([Execution payloads](../06-adoption/01-execution-payloads.md) discusses it).

## Failure modes

- **Forged shards.**
  A chunk is accepted only if its SHA-256 hash equals the preamble's hash for its index.
- **Lying origin.**
  A valid set of shards can still decode to the wrong object when the origin lied in the preamble, so the end-to-end hash is checked after decode.
- **Unauthenticated preamble.**
  The hashes authenticate chunks against the preamble only.
  Nothing binds the preamble to a publisher, so a peer that opens a session with its own preamble can broadcast any object under any object identifier the framework accepts.
- **Preamble allocation.**
  Requiring one hash per shard bounds every per-shard allocation by the bytes the peer sent.
- **Odd hash sizes.**
  A hash that is not 32 bytes, or a `hash` of any length, is not checked on receipt: a short hash never matches a shard, so the shard is invalid, and an unmatched object hash fails the decode.
- **Long bitmap.**
  A bitmap of any length is merged over the shorter length, so it does not grow node state.
  Chunk size, read deadlines and read concurrency are bounded by the framework.
- **Disclosure.**
  A bitmap tells its receiver which shards the sender holds; a first bitmap of all ones tells it the sender is the origin of the session.

## Rationale

**Undoing the allocation count.**
The count orders dispatch: a shard whose send failed would otherwise look offered, rank behind untouched shards, and starve while the peer still lacks it.
The forward count is not refunded because it is charged only on success.

**Bitmap threshold.**
It avoids sending updates while the node has little to report and the coupon collector effect is not yet visible.
The default is 50% of the shards.

**Mechanisms reduce, not eliminate, the tail.** `specs/003-ec-broadcast-rs.md` reports that in simulations they bring RS within about 10% of the p90 latency of RLNC across the benchmarked object sizes ([RLNC](06-rlnc.md) is the comparison strategy).

**Late results.**
A late send or read result is keyed by object identifier and can reach a replacement session under the same identifier.
That is by design, since an object identifier is a content address; the RS strategy ignores a result for a peer or shard it does not track.

**Open.**
The best configuration values as a function of object size and network conditions have not been characterized.
Nothing authenticates the preamble yet.

## Code

Package `broadcast/rs`: `strategy.go` (encoding, verification, dispatch, decode), `emit.go` (the dispatch heap), `types.go` (preamble, chunk identifier, configuration, scheme), `bitmap.go`, `pb/rs.proto`; library `klauspost/reedsolomon`.
Tests: `strategy_test.go` (relay decoding, duplicate and bad chunks, malformed preambles, routing cancellation, origin dispatch), `emit_test.go`; `e2e/relay_test.go`, `relay_hold_test.go`, `session_routing_test.go`.
No test covers the undo of a failed send at the strategy level.
No second implementation exists, so interoperability is untested.

- Encoding: compute the shard length before allocating and size one buffer for all n shards; copy the object in, leave the tail zero, let the encoder fill parity.
  A configuration of 256 or more shards needs the 64-byte rounding, and the fixed-shard-length path does not round, so keep the chunk length a multiple of 64 when n can reach 256.
  Hash every shard and the object after encoding.
- Preamble: validate where the relay strategy is built;
  the framework builds it through the scheme's relay factory,
  and a factory error rejects the session.
- Receiving: store the shard, set the bit, mark routing dirty, insert into the dispatch heap and cancel the dedup group in one step.
  The dedup key of a shard is its index as four big-endian bytes.
  After the k-th shard stop writing the shard array: the framework decodes on a background goroutine that reads it.
- Decode on a copy of the shard slice
  (reconstruction fills missing entries in place) and reconstruct only data shards.
- Tracking: allocate per-peer bitmap and in-flight counters at attach, sized to n.
  Keep a completed peer but skip it in dispatch, so sends already in flight still resolve.
- Dispatch: one heap entry per held shard, ordered by allocation count then priority.
  For each peer pop entries until one is eligible, then reinsert the skipped ones.
  Raise the allocation count at allocation, so the next peer in the same poll tends to get a different shard.
  Undo exactly what allocation did on failure: in-flight count, the peer's bit, the allocation count.
  Draw the relay seed once per session.
- Bitmap: clear the dirty flag only when a bitmap is returned, so the first poll after the threshold is crossed emits; emit a copy.
  Merging is an OR, so a stale or shorter update never clears a bit; the strategy returns the handles of sends now redundant and the framework cancels them.

Pitfalls:

- With a chunk length above 0 the shard count depends on the payload size,
  so a test that counts sends must fix shard counts (the e2e `fixedRS` helper does).
- Any new path that writes a shard after completion races with the decode goroutine;
  route it through the `Redundant` return.
- A remote `Redundant` reset after a clean close completed the send;
  counting it as a failure resends the shard the peer just said it has.
- The 5 second chunk-write deadline and the 16-entry outbound slot live in `broadcast`,
  so a timeout or a full slot reaches the strategy only as a failed send.
- Origin ties have no defined order: an origin with one peer and a 4+4 code offered shards in the order 0 1 3 7 4 2 5 6.
  A test must not expect index order.
- `PollChunks` iterates the peer map,
  so the order in which peers receive the shards of one poll is not defined.
- The framework cancels a peer's in-flight sends before `DetachPeer`,
  but a cancelled send still reports afterwards; ignore results for a peer no longer tracked.
- The framework calls `VerifyChunk` on the session's actor,
  so the SHA-256 of every chunk runs there and a slow verification stalls the channel.
