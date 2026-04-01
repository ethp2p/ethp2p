# Reed-Solomon broadcast strategy

This document specifies the Reed-Solomon (RS) erasure coding strategy for the
ethp2p broadcast framework defined in the
[framework spec](002-ec-broadcast.md).

We assume the reader is familiar with systematic Reed-Solomon codes: k data
shards, n-k parity shards, any k of n suffice to reconstruct. What follows
focuses on the network-level machinery built around RS: how we verify, route,
dispatch, and deduplicate shards across a mesh of peers.

RS is a good fit when chunks are independently verifiable and the topology is
well-connected enough for shard-level dedup to be effective. Its main limitation
(cf. RLNC) is that relays cannot recode: they forward the exact shards they
received, so data novelty depends on the origin producing enough distinct shards
and on the topology spreading them. This limitation creates a coupon collector
dynamic: as a node accumulates shards, the probability that the next arriving
shard is novel decreases. Without countermeasures, the last few shards dominate
completion time.

We mitigate this through bitmap-based routing (peers advertise what they have,
so senders skip redundant shards), the emit planner's least-allocated-first
ordering (rare shards get priority), and per-relay Fibonacci hashing
(neighboring relays spread different shards). These mechanisms reduce but do not
eliminate the coupon collector tail; further topological optimizations are under
investigation.

In simulations, these mechanisms bring RS within ~10% of RLNC's p90 latency
across all benchmarked message sizes, despite the no-recode constraint. RS is
already deployed in Ethereum for PeerDAS blob erasure coding, so the
network-level optimizations described here achieve close-to-optimal performance
without adding any novelty to the protocol.

## 1. Terminology

Terms defined in the framework spec (Channel, Session, Chunk, Preamble, Routing
Update, Strategy) apply here. Additional terms:

**Shard.** An RS chunk. Each shard carries a unique, independently verifiable
piece of the erasure code. RS chunks are addressed by shard index (0 to n-1),
where indices 0 through k-1 are data shards and k through n-1 are parity shards.

**Bitmap.** The routing state type for RS. A bitset where bit i indicates
whether the node holds shard i. Peers exchange bitmaps to advertise inventory.

## 2. Encoding

When the application publishes a message, the origin strategy encodes it. The
encoding is systematic: data shards `0` through `k-1` contain the original
message bytes (plus padding on the last shard), and parity shards `k` through
`n-1` contain redundancy. The origin can set these parameters dynamically based
on message size, and it announces the values via the Session `Preamble` for
receivers to configure their strategy for the session.

```go
// 1. Compute chunk structure from config.
//    ChunkLen=0 (default): split into DataShards equal pieces, derive chunk length.
//    ChunkLen>0: derive DataShards from message length and fixed chunk size.
preamble, chunkLen := initPreamble(config, len(payload))

// 2. Create RS encoder with (k, n-k) parameters.
enc, _ := reedsolomon.New(preamble.DataChunks, preamble.ParityChunks)

// 3. Pad message to fill k*chunkLen bytes, allocate space for parity shards.
totalLen := chunkLen * preamble.TotalChunks()
msg := slices.Clone(payload)
msg = slices.Grow(msg, totalLen-len(payload))
msg = msg[:totalLen]

// 4. Split into shards and encode (fills parity shards in place).
shards := slices.Collect(slices.Chunk(msg, chunkLen))
enc.Encode(shards)

// 5. Hash each shard (for per-chunk verification) and the original message
//    (for end-to-end integrity after decode). Both go into the preamble.
// TODO: will be replaced with Merkle commitment or builder signatures.
for i, data := range shards {
    h := sha256.Sum256(data)
    preamble.ChunkHashes[i] = h[:]
}
preamble.MessageHash = sha256.Sum256(payload)

// 6. Insert all shards into the emit planner. Origin is ready to serve.
for i := range shards {
    planner.Insert(emitEntry{Idx: i})
}
```

## 3. Preamble

The RS preamble carries everything a relay needs to initialize its strategy
instance and verify incoming chunks. It is transmitted in the `Sess.Open` frame
as opaque bytes (see framework spec, Section 5.1).

```protobuf
// Preamble is the session preamble for Reed-Solomon coded messages.
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
  // SHA-256 hash of the original message. Verified after decode for
  // end-to-end integrity.
  bytes hash = 5;
}
```

## 4. Chunk verification

> The current hash-based verification is provisional, but we proceed to describe
> it here and then introduce future replacements.

RS uses synchronous per-chunk verification. When a chunk arrives, the strategy
computes its SHA-256 hash and compares it against the corresponding entry in
`ChunkHashes`. If the hashes match, the chunk is accepted; if not, it is
rejected. No asynchronous verification pipeline is used.

This hash-based scheme authenticates individual chunks independently, so a relay
can verify and begin forwarding each shard the moment it arrives. **A single
invalid chunk cannot corrupt the decode; it is caught and rejected before
entering the strategy's state.**

After reconstruction, the strategy computes the SHA-256 hash of the decoded
message and verifies it against `MessageHash`. This provides end-to-end
integrity: even if Reed-Solomon reconstruction produces output (because enough
shards passed individual verification), the message hash catches any
inconsistency.

In the future, two authentication modes are planned, and the choice may vary per
channel:

1. **Builder signature.** In contexts like ePBS where the builder's identity is
   known, the builder authenticates every chunk via a signature. Sending invalid
   chunks is irrational for a builder, so this alone should afford the required
   security. This method is compatible with streaming chunks out.

2. **Merkle commitment.** A Merkle tree over all chunk hashes, where each chunk
   carries its inclusion proof. This does not require a known signer but
   requires the full chunk set to construct the commitment, therefore
   incompatible with streaming. Suitable for contexts where the publisher is
   anonymous, or there are multiple concurrent publishers.

## 5. Routing

The RS routing update type is a bitmap acting as a shard havelist.

Routing update emission is gated by a threshold: the bitmap is not emitted until
the node has received at least `BitmapThreshold` percent of the total shards
(default 50%). This avoids flooding the network with routing updates when the
node has very little to report, and where the coupon collector problem is not
yet visible.

Once the threshold is crossed, every accepted shard triggers a routing update.
The threshold can be set to zero (always emit) or routing can be disabled
entirely via `DisableBitmap`.

During development, we also send routing updates every 25ms, but this behavior
will be replaced by more adaptive constructions.

When a routing update arrives from a peer, the strategy OR-merges the received
bitmap into its previous view of that peer's inventory, and cancels in-flight
sends that are now redundant.

## 6. Dispatch

Dispatch is driven by the **emit planner**, a min-heap that orders shards by
allocation count (how many times a shard has been dispatched across all peers).
Ties are broken by a per-relay random priority derived from Fibonacci hashing:
`(seed ^ uint64(idx) * 0x9E3779B97F4A7C15) >> 32`, where the seed is random per
relay. This means neighboring relays naturally prioritize different shards,
reducing duplicate transmissions without explicit coordination.

For each peer that has not yet reconstructed, the planner pops the
least-allocated shard and checks:

1. the peer does not already have it (bitmap),
2. it is not already in-flight to this peer,
3. the shard has not exceeded the per-shard redundancy budget.

If any check fails, the next entry is tried. On allocation, both the peer's
in-flight count and the planner's allocation count are incremented, pushing the
shard down the heap for future rounds.

Origins are unconstrained: they send each shard as many times as there are peers
that need it. Relays are budget-constrained by `ForwardMultiplier` to limit
per-relay bandwidth amplification. Failed sends do not consume budget; the shard
remains available for future allocation.

On successful send, the strategy optimistically marks the shard as present in
the peer's inventory and increments the shard sent count.

## 7. Decoding

Decoding triggers when the strategy has received at least k shards (any k of n).
The framework calls `Decode` on a background goroutine.

```go
// 1. Clone shards (RS reconstruction mutates the array).
shards := cloneShards(s.chunks, s.preamble.TotalChunks())

// 2. Reconstruct missing data shards from available parity.
enc, _ := reedsolomon.New(s.preamble.DataChunks, s.preamble.ParityChunks)
enc.ReconstructData(shards)

// 3. Concatenate data shards, truncate padding.
data := concat(shards[:s.preamble.DataChunks])
data = data[:s.preamble.MessageLength]

// 4. Verify end-to-end integrity.
hash := sha256.Sum256(data)
if !bytes.Equal(hash[:], s.preamble.MessageHash) {
    return nil, fmt.Errorf("message hash mismatch")
}
return data, nil
```

Decode failure is non-recoverable (see framework spec, Section 7.4).

## 8. Configuration

| Parameter           | Default | Effect                                                                                                                                                                                        |
| ------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DataShards`        | 16      | Number of data shards (k). Message is split into k equal pieces.                                                                                                                              |
| `ParityShards`      | 16      | Number of parity shards (n-k). Any k of n total shards suffice to decode.                                                                                                                     |
| `ChunkLen`          | 0       | Fixed chunk size in bytes. If zero, computed from message length and DataShards. If nonzero, DataShards is derived from message length and ChunkLen; ParityShards is set equal to DataShards. |
| `BitmapThreshold`   | 50      | Percentage of shards received before routing bitmap is emitted (0 = always emit).                                                                                                             |
| `ForwardMultiplier` | 4       | Maximum successful sends per shard for relays. Origins are unlimited.                                                                                                                         |
| `DisableBitmap`     | false   | If true, routing bitmaps are never emitted.                                                                                                                                                   |

These parameters interact with message size in ways that are not yet fully
characterized. The optimal configuration as a function of message size and
network conditions is an open empirical question.
