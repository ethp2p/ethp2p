# Random Linear Network Coding broadcast strategy

> RLNC is specified and implemented here for comparative benchmarking only. It is patent-encumbered and not a candidate for deployment (see [README](../README.md#stance-on-intellectual-property)).

This document specifies the Random Linear Network Coding (RLNC) strategy for the ethp2p broadcast framework defined in the [framework spec](broadcast-spec-v3.md).

We assume the reader is familiar with RLNC: messages are split into generations, each chunk is a random linear combination of the generation's basis vectors over a finite field, and any k linearly independent chunks suffice to reconstruct a generation of size k. What follows focuses on how we adapt RLNC for network broadcast: generation structure, relay recoding, rank-based routing, forward budgets, and the interaction count bound.

RLNC's advantage over RS is that relays can recode: instead of forwarding the exact chunks they received, they produce fresh random linear combinations from their accumulated basis. Every outbound chunk is innovative with high probability, regardless of what the peer already holds. This eliminates the coupon collector problem that RS must work around.

The cost is twofold. Per-chunk overhead is higher (each chunk carries a coefficient vector of Ristretto255 scalars). More importantly, individual coded chunks cannot be verified before acceptance: each chunk is a unique random combination with no precomputable hash. A single malicious chunk with valid structure but garbage data corrupts the generation's basis, detectable only at decode time. For production deployment, homomorphic commitment schemes (KZG or Pedersen) are required to close this gap.

## 1. Terminology

Terms defined in the framework spec (Channel, Session, Chunk, Preamble, Routing Update, Strategy) apply here. Additional terms:

**Generation.** A partition of the message. Large messages are split into multiple generations, each coded independently. A generation has `NumChunksPerGeneration` basis dimensions; full rank means the generation has accumulated that many linearly independent chunks and can be decoded.

**Basis.** The set of linearly independent chunks accumulated for a generation. Maintained as an incremental echelon form: each arriving chunk undergoes Gaussian elimination against the existing basis. If it reduces to zero, it is linearly dependent and rejected. If independent, it is inserted and the generation's rank increases by one.

**Coefficient vector.** Each RLNC chunk carries a vector of Ristretto255 scalars recording the linear combination used to produce it. Length is `NumChunksPerGeneration`. For basis chunks (origin's initial encoding), the vector is a standard basis vector (e_i). For recoded chunks, the coefficients are random.

**Rank.** The number of linearly independent chunks a node has for a generation. Progresses from 0 to `NumChunksPerGeneration`; at full rank, the generation can be decoded.

**Recoding.** Generating a new random linear combination from a generation's accumulated basis. The result is a fresh chunk whose coefficient vector and data are random combinations of the basis entries. With high probability over Ristretto255, a recoded chunk is linearly independent of any specific set of chunks the recipient holds.

## 2. Encoding

When the application publishes a message, the origin determines the generation structure from the configuration, zero-pads the message to fill all generations evenly, and produces basis chunks. All arithmetic is over Ristretto255 scalars (253-bit field elements, serialized as 32 canonical bytes). Payload bytes are packed in 31-byte groups to ensure every scalar is canonical on decode; this makes chunk data ~3% larger than the raw payload.

```go
// Determine generation structure from config and zero-pad message.
numGenerations, numChunksPerGen, chunkSize := initGenerations(config, messageLen)

// For each generation, produce basis chunks with standard basis coefficients (e_i).
for g := range numGenerations {
    for i := range numChunksPerGen {
        coefficients := standardBasis(i, numChunksPerGen)
        data := bytesToRistrettoVector(genData[i*chunkSize : (i+1)*chunkSize])
        chunks = append(chunks, Chunk{Generation: g, Coefficients: coefficients, Data: data})
    }
}

// Spawn async producer: OriginRedundancy * totalBasisChunks extra random
// linear combinations, distributed across generations.
go s.produceAsync(len(chunks))
```

The origin has two sources of outbound chunks: the initial basis (deterministic, produced synchronously) and the async redundancy (random, produced in the background). The pending queue is consumed by `PollChunks`; once exhausted, the strategy falls back to on-demand recoding.

## 3. Preamble

The RLNC preamble carries the generation structure. It is transmitted in the `Sess.Open` frame as opaque bytes (see framework spec, Section 5.1).

```protobuf
// Preamble is the session preamble for RLNC-coded messages.
message Preamble {
  // Number of generations the message is split into.
  int32 num_generations = 1;
  // Basis size (full rank dimension) per generation.
  int32 generation_size = 2;
  // Original unpadded message size in bytes. Used to trim decoded output.
  int32 length = 3;
}
```

The preamble does not currently carry authentication commitments. Without homomorphic commitments, individual coded chunks cannot be verified before acceptance (see Section 4).

## 4. Chunk authentication

Given the inability to deploy RLNC in Ethereum (see IP stance), we have not implemented per-chunk authentication. The current strategy performs only structural validation: it checks that the generation index is in range and that the coefficient vector has the correct length. A malicious chunk with valid structure but garbage data will be accepted into the basis, corrupting the generation. The corruption is detectable only at decode time.

If RLNC were to be deployed, homomorphic commitment schemes (KZG or Pedersen) would be required. These can authenticate coded chunks without knowing their content in advance: the commitment is placed in the preamble, and each chunk's validity is checked against the commitment using the chunk's coefficient vector.

## 5. Routing

The RLNC routing type is a `HaveList`: a list of (generation, rank) pairs, one per generation where the node has made progress. When a node accepts a new chunk (increasing a generation's rank), it flags its routing state as dirty. On the next routing poll, the framework broadcasts the updated `HaveList` to all session peers.

Unlike RS (where the bitmap reveals exactly which shards a peer has), the `HaveList` reveals only rank. A fresh random combination is innovative with high probability as long as the peer has not reached full rank.

When a routing update arrives from a peer, the strategy updates its view of the peer's per-generation rank. If the peer has reached full rank for a generation, all in-flight sends to that peer for that generation are canceled.

## 6. Dispatch

Dispatch is peer-driven: `PollChunks` iterates peers, and for each non-completed peer selects a chunk to send. The origin and relay paths diverge at this point.

The origin scans its pending queue (basis chunks plus async redundancy) for the best match: the chunk whose generation has the lowest (sent + in-flight) count for this peer, skipping generations where the peer has reached full rank. If nothing suitable is pending, it falls back to on-demand recoding from the generation basis.

Relays always recode. For each peer, the strategy selects an eligible generation and produces a fresh random linear combination from that generation's basis. Generation selection picks the generation with the lowest dispatch count for the target peer.

A generation is eligible for recoding if the peer's rank is below full, the node has at least one chunk in the basis, the generation's forward budget is positive (or the node has fully decoded), and the interaction count bound is not exceeded.

### 6.1. Forward budget

Each accepted inbound chunk grants `ForwardMultiplier` (default 4) units of forward budget for that chunk's generation. Each relay dispatch consumes one unit. This limits per-relay bandwidth amplification: a relay that receives one chunk can forward at most `ForwardMultiplier` coded chunks for that generation. Failed sends restore the budget.

Decoded relays bypass the budget entirely. Once a node has reconstructed the full message, it can serve any generation without budget constraints, enabling pull recovery when surrounding peers have exhausted their budgets.

### 6.2. Interaction count bound

For each (peer, generation) pair, the strategy tracks interactions: chunks sent, in-flight, and received from that peer. If this count reaches the generation's current basis rank, the strategy stops sending to that peer for that generation.

The rationale: chunks received from a peer already live in that peer's coding subspace. Sending combinations of those chunks back is mathematically redundant. The interaction bound ensures outbound traffic adds information rather than recycling what the peer already contributed.

## 7. Decoding

Decoding triggers when all generations have reached full rank. The framework calls `Decode` on a background goroutine.

For each generation, the strategy computes the inverse of the coefficient matrix via back-substitution on the echelon form's transformation matrix, applies the inverse to the data vectors, and concatenates the reconstructed chunks. The transformation matrix is maintained incrementally as chunks arrive, so no re-processing of the basis is needed at decode time.

After all generations are decoded, the per-generation outputs are concatenated and truncated to `MessageLength` to remove zero-padding.

Decode failure is non-recoverable (see framework spec, Section 7.4).

## 8. Configuration

| Parameter                | Default | Effect                                                                                                                               |
| ------------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `NumChunks`              | 8       | Total basis chunks when `TargetChunkSize=0` (single generation).                                                                     |
| `NumChunksPerGeneration` | 8       | Basis size per generation. Larger values increase per-chunk overhead (longer coefficient vectors) but improve coding efficiency.     |
| `TargetChunkSize`        | 16384   | Target bytes per chunk. If zero, single generation with `NumChunks` chunks. If nonzero, generations are derived from message length. |
| `OriginRedundancy`       | 16      | Multiplier for async origin production. Higher values accelerate initial dissemination at the cost of origin bandwidth.              |
| `ForwardMultiplier`      | 4       | Forward budget per accepted chunk on relays.                                                                                         |

These parameters interact with message size in ways that are not yet fully characterized. The optimal configuration as a function of message size and network conditions is an open empirical question.
