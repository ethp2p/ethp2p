---
status: proposed
requires:
  - ../03-broadcast/01-broadcast-framework.md
  - ../03-broadcast/05-reed-solomon.md
---

# Execution payloads

Nothing in this document is implemented, and no part of it is specified in detail.
The RS preamble carries per-chunk hashes, not a builder bid, and no chunk is signed.
All rules below are proposed.

Execution payloads travel over gossipsub, a store-and-forward protocol: a peer must hold the whole payload before it can act on or forward it, so each hop adds the full transfer time.
A peer four hops from the sender waits for three intermediate nodes to receive, validate and forward the message before its first byte arrives, and by then it has usually received more copies than it needs.
Gossipsub reduces duplicates with control messages (IHAVE, IDONTWANT), but they help only if they beat the peer's queued push, and once a write has started there is no cancellation.
The pull path costs 1.5 round trips, so Ethereum treats it as fault recovery.
Payloads grow with the gas limit, which makes these costs rise.
Erasure-coded broadcast moves the unit of propagation from the payload to the coded chunk: peers make progress from the first chunk, chunks are small and travel faster, fanout can be wider, and redundancy comes from the code instead of full-payload replication.
This document proposes using the [broadcast framework](../03-broadcast/01-broadcast-framework.md) to propagate execution payloads: a payload is one session on a channel, announced by a small preamble that carries a signed builder bid, and each chunk is authenticated by the builder's signature.
The meaning of MUST and SHOULD is in [Conventions](../00-foundations/02-conventions.md); defined terms are in
[Terminology](../00-foundations/03-terminology.md).

```text
   Builder                      Relay                       Proposer
     |-- session open(preamble: bid, coding params) -->|          |
     |-- chunk(index, data, signature) -------------->|           |
     |                         |-- chunk, unchanged ------------->|
     |<------- routing update (above a threshold) ---|<- routing --|
```

## Wire format

```abnf
payload-session = session-open(preamble) *chunk      ; one session on a channel per payload
preamble        = bid-or-reference auth-material coding-params
chunk           = chunk-ident chunk-payload builder-signature
```

| Field | Type | Constraint |
| ----- | ---- | ---------- |
| Preamble bid | signed builder bid, or a reference | Open question 1. |
| Preamble auth material | not specified | What a relay needs to authenticate incoming chunks. Open questions 2 and 4. |
| Preamble size | n/a | SHOULD fit in one MTU, so the preamble propagates quickly. |
| Chunk builder signature | not specified | Present on every chunk; verified against the builder's public key from the bid. |

## Behavior

**Authenticating a session.**
Authentication is two-layered:

1. The bid, from the preamble, authenticates the session and binds it to the builder's identity.
   The signed bid is what legitimizes a builder to propagate a block.
2. Relays MUST verify each chunk's builder signature against the builder's public key from the bid
   before accepting the chunk.

Because RS relays forward chunks unchanged ([Reed-Solomon](../03-broadcast/05-reed-solomon.md)), each chunk keeps its original signature through every hop.
A recoding strategy would need homomorphic authentication ([RLNC](../03-broadcast/06-rlnc.md)).

**Routing updates.**
Once a node holds a threshold of unique chunks it begins sending routing updates, so senders skip chunks it has and prefer ones it lacks.
For RS the threshold is 50% of the shards and the update is a bitmap, which addresses the coupon collector effect.

**Session lifetime.**
Slot timing bounds the lifetime of a session.
What happens to a session whose payload misses the attestation deadline, and when it is torn down, is undefined (open question 6).

**Changes.**
Adopting this changes the preamble and the chunk verification of the strategy, so it ships as a new strategy with an application protocol name change.

## Failure modes

This reasons about the proposal; no code exists to verify it.

- **Unbound preamble.**
  Today nothing binds an RS preamble to a publisher.
  The proposal binds it with the signed bid and a signature on each chunk.
- **Header-only bid.**
  A bid that signs only an execution header, not the payload body, does not make chunk authentication sound (open question 2).
- **Chunks racing the preamble.**
  They need a buffer whose bound and lifetime are undefined (open question 3); an unbounded buffer would let a peer exhaust memory.
- **Dead sessions.**
  A session whose payload misses the attestation deadline has no defined teardown (open question 6), so a peer could keep dead sessions alive.

## Rationale

The questions below are open.

1. **Bid in the preamble, or a reference.**
   Embedding the signed bid makes relays self-sufficient and avoids a fetch, but the preamble must then fit the bid and the coding parameters within one MTU.
   A reference keeps the preamble small but relays must obtain the bid before authenticating any chunk.
2. **Bid-to-body commitment.**
   Today's builder bid signs an execution header, not the payload body.
   For chunk authentication to be sound, the signed material must commit to the body, for example through the transactions hash or a chunk Merkle root, which may require a bid format change.
3. **Chunks racing the preamble.**
   Whether relays buffer chunks that arrive before the preamble, for how long, and at what budget.
4. **Preamble size.**
   The RS preamble carries one hash per chunk, so it grows with n and exceeds one MTU at large k.
   The one-MTU goal holds only with a Merkle commitment or a builder signature in place of per-chunk hashes.
5. **Coexistence with gossipsub.**
   Same-topic replacement, a parallel topic during transition, or fallback, and the behavior of mixed fleets.
6. **Session lifetime against the attester deadline.**
   What happens to a session whose payload misses the attestation deadline, and when it is torn down.
7. **Publishers and relays.**
   Builder to relay to proposer is the natural flow.
   Whether relays and proposers forward chunks, and under what budgets.

## Code

Nothing in this topic is implemented.
