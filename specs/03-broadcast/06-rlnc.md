---
status: proposed
requires:
  - 01-broadcast-framework.md
  - 05-reed-solomon.md
---

# RLNC strategy

Nothing in this document is implemented.
`broadcast/rlnc/` holds only a README on the intellectual-property stance.
The messages, rules and configuration below are a proposal, and field numbers are illustrative.
The layouts of the chunk and the have-list are not specified beyond the contents listed.

This document proposes a Random Linear Network Coding strategy for comparative benchmarking against [Reed-Solomon](05-reed-solomon.md); RLNC is patent-encumbered and is not a candidate for deployment (`specs/004-ec-broadcast-rlnc.md`).
The origin partitions an object into generations and sends random linear combinations of each.
A relay can recode: instead of forwarding the chunks it received, it emits fresh random combinations of what it holds, so every outbound chunk is innovative with high probability regardless of what the peer already has, which removes the coupon collector effect RS works around.
The cost is twofold: each chunk carries a coefficient vector, so per-chunk overhead is higher, and a coded chunk cannot be verified against a precomputable hash, so one malicious chunk with a valid structure corrupts a generation's basis and is detected only at decode.
Verdicts and the strategy contract are in [Broadcast framework](01-broadcast-framework.md).
The meaning of MUST and SHOULD is in [Conventions](../00-foundations/02-conventions.md); defined terms (generation, basis, coefficient vector, rank, recoding, have-list) are in [Terminology](../00-foundations/03-terminology.md).
Where the text says a rule is proposed, no code follows it.

```text
   Origin                           Relay                          Peer
     |-- session open(preamble) ------>|                              |
     |-- chunk(generation, vector|data)->|                            |
     |                                  |-- chunk (recoded) ---------->|
     |<-- have-list(generation, rank) --|<-- have-list ----------------|
```

A relay holds a basis per generation, recodes on demand, and tells its peers its ranks in a have-list so they stop sending a generation once it is at full rank.
A relay amplifies at most by its forward budget, and chunks are not authenticated.

## Wire format

```abnf
preamble       = Preamble       ; protobuf, in Open.preamble
chunk-ident    = ChunkIdent     ; protobuf, names the generation
chunk-payload  = coefficients data   ; generation_size scalars, then the coded data
routing-update = HaveList       ; protobuf, one entry per generation with progress
```

Arithmetic is over Ristretto255 scalars: 253-bit field elements of 32 canonical bytes.
Payload bytes are packed in 31-byte groups so every scalar is canonical on decode, which makes chunk data about 3% larger than the payload.

```protobuf
// PROPOSED. Field numbers are illustrative.
message Preamble {
  int32 num_generations = 1;  // number of generations
  int32 generation_size = 2;  // full-rank dimension per generation
  int32 length = 3;           // original unpadded object length in bytes
}

message ChunkIdent {
  int32 generation = 1;       // index of the generation the chunk belongs to
}

message HaveList {
  message Entry {
    int32 generation = 1;
    int32 rank = 2;           // size of the sender's basis for the generation
  }
  repeated Entry entries = 1; // one per generation where the sender has progressed
}
```

| Field | Type | Constraint |
| ----- | ---- | ---------- |
| `Preamble.num_generations`, `generation_size`, `length` | int32 | None specified. |
| `ChunkIdent.generation` | int32 | MUST be in range. |
| Chunk coefficient vector | `generation_size` scalars | MUST have exactly `generation_size` scalars. |
| `HaveList.Entry.generation` | int32 | MUST be in range. |
| `HaveList.Entry.rank` | int32 | None specified. |

The preamble carries no authentication material.
Proposed configuration:

| Field | Proposed default | Effect |
| ----- | ---------------- | ------ |
| `NumChunks` | 8 | Basis chunk count when `TargetChunkSize` is 0 (one generation). |
| `NumChunksPerGeneration` | 8 | Generation size. Larger values lengthen coefficient vectors. |
| `TargetChunkSize` | 16384 | Target bytes per chunk; when nonzero, generations are derived from the object length. |
| `OriginRedundancy` | 16 | Multiplier for background production at the origin. |
| `ForwardMultiplier` | 4 | Forward budget per accepted chunk. |

## Behavior

These rules are proposed.

**Encoding.**
The origin derives the generation count, generation size and chunk size from the configuration and the object length, and zero-pads the object to fill all generations evenly.
It produces one basis chunk per position in each generation, with a standard-basis coefficient vector, and produces in the background `OriginRedundancy` times the basis chunk count of extra random combinations, spread across generations.
Basis chunks are served first, then the pre-produced redundancy, then on-demand recoding.

**Validating a chunk.**
Validation is structural only.
A chunk with valid structure and garbage data is accepted into the basis and corrupts the generation, detectable only at decode.
A chunk whose vector is linearly dependent on the basis does not raise the rank.

**Have-lists.**
A node marks routing dirty whenever an accepted chunk raises a rank, and the framework then sends the have-list to session peers.
The list reveals rank only, not which combinations the peer holds; that suffices because a fresh combination is innovative with high probability while the peer is below full rank.
A receiver records the peer's rank per generation and cancels in-flight sends for any generation where the peer reached full rank.

**Dispatching.**
For each peer that has not completed, the strategy picks the generation with the lowest dispatch count for that peer, then a chunk for it.
The origin prefers pending chunks (basis, then redundancy) for the generation with the lowest sent plus in-flight count, and falls back to recoding; relays always recode.
A generation is eligible for a peer when the peer's rank is below full, the node holds at least one chunk of the generation, the generation's forward budget is positive (or the node has decoded everything), and the interaction bound is not reached.

**Forward budget.**
Each accepted inbound chunk adds `ForwardMultiplier` units to that generation's budget, and each relay dispatch spends one, which caps relay amplification.
A failed send restores its unit.
A node that has decoded the whole object ignores the budget, so it can serve any generation when neighbors have exhausted theirs.

**Interaction bound.**
For each (peer, generation) the strategy counts chunks sent, in flight, and received from that peer.
When the count reaches the node's current rank for the generation, it stops sending that generation to that peer.

**Decoding.**
Decoding starts when every generation reaches full rank.
Generations are concatenated and truncated to `length`.
Decode failure is not recoverable for the session.

**Changes.**
The proposal has no extension points.
Adopting it, or changing it after adoption, ships with an application protocol name change.

## Failure modes

This reasons about the proposal; no code exists to verify it.

- **Corrupt chunk.**
  A chunk with a valid vector and garbage data enters the basis and corrupts the generation, and the node learns of it only at decode, after receiving the whole object.
  A deployment would need homomorphic commitments (KZG or Pedersen) in the preamble, checked against each chunk's coefficient vector.
- **Unauthenticated preamble.**
  Nothing binds a session to a publisher ([Execution payloads](../06-adoption/01-execution-payloads.md)).
- **Unbounded preamble.**
  The preamble constraints are unspecified; an implementation would have to bound `num_generations` and `generation_size` by the bytes the peer sent, as [Reed-Solomon](05-reed-solomon.md) does for the hash count.
- **Disclosure.**
  A have-list reveals ranks, so how much of the object a node holds; a full-rank have-list from the first update would identify the origin.

## Rationale

**Comparison point only.**
RLNC exists in this design to compare against RS; the patent encumbrance rules it out for deployment.

**Homomorphic commitments.**
Without them coded chunks cannot be verified before acceptance.

**Interaction bound.**
Chunks received from a peer already lie in that peer's coding subspace, so combinations of them add nothing for it; the bound makes outbound traffic add information instead of recycling what the peer contributed.

**31-byte packing.**
It keeps every scalar canonical on decode, at about 3% size overhead.

## Code

Nothing is implemented.
A strategy and a `broadcast.Scheme` would live in `broadcast/rlnc/`, as `broadcast/rs/` does for RS.
Design point: per generation, maintain the echelon form's transformation matrix incrementally as chunks arrive, so decode back-substitutes without reprocessing the basis and applies the inverse to the data vectors.
The framework calls `Decode` on a background goroutine.
