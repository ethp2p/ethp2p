# Integration: erasure-coded broadcast for execution payloads

Status: draft.

This document motivates and specifies the integration of the
[erasure-coded broadcast framework](002-ec-broadcast.md)
into Ethereum's execution payload propagation path.
Sections 1 to 3 make the case for erasure-coded broadcast.
Sections 4 and 5 specify the integration: what the Session preamble carries,
how chunks are authenticated, and how Session lifetime is bounded by slot timing.
Strategy-level machinery is out of scope.
The [RS strategy spec](003-ec-broadcast-rs.md) defines Reed-Solomon encoding, routing, and dispatch.
The [RLNC strategy spec](004-ec-broadcast-rlnc.md) is the comparison point.

## 1. Gossipsub: store-and-forward propagation

Execution payloads are propagated via gossipsub.
Gossipsub is a store-and-forward protocol.
A peer must receive the full payload before it can act on the message or make any progress.
As the gas limit rises, payloads grow, and so does the cost of moving each one whole.

Say the payload is 200 KiB and you sit 4 hops away from the original sender.
You wait for 3 intermediate nodes to fully receive, validate,
and forward the message before your first byte arrives.
When the message arrives, you have probably received more copies than you need.

Gossipsub is also a push-pull protocol.
Messages can be delivered proactively (push) or demanded (pull).
Ethereum's mesh degree configuration favors push.
Ethereum time is segmented into subslots, and each message class has strict deadlines for emission,
distribution, forwarding, delivery, processing, and reaction.
The pull mechanism costs 1.5 RTT,
so Ethereum treats it as a fault-recovery path rather than the primary delivery route.

## 2. Best-effort delivery through redundancy

The Ethereum network is trustless, byzantine, and dynamic.
Deterministic, infallible message routing is impossible.
Nodes interface with the mesh through adjacent neighbours and rely on redundancy
and peer scoring to sustain reliability and delivery quality.
Gossipsub makes no delivery guarantees.

A node sends D replicas to its peers.
Control messages such as IHAVE and IDONTWANT curtail duplicates.
These mechanisms only help when the control message reaches a peer before
that peer's queued push fires.
Whether that happens is implementation dependent.
Once a network write has started, gossipsub today has no cancellation semantics.
Adding them would be a breaking change.

**Point: the information dispersal efficiency and throughput of gossipsub are low.**

## 3. Erasure-coded broadcast

Erasure-coded broadcast divides the original payload into N chunks
and applies some form of erasure coding.
Redundancy comes from information theory instead of full-payload replication.
The unit of propagation is the coded chunk rather than the payload.
With k=128, a 256 KiB payload travels as 2 KiB chunks.

This changes propagation in three ways:

- Peers make progress from the first chunk.
- Chunks are smaller, so they propagate faster.
- Fanouts can be wider.

The network spreads the same information with less duplication.
That lowers propagation latency and raises protocol throughput and capacity.

Coding also removes transport machinery.
Redundancy via routing needs duplicate tracking, control messages, and cancellation semantics.
Redundancy via coding needs none of them.

## 4. ethp2p erasure-coded broadcast

We propose ethp2p EC broadcast, a Reed-Solomon based protocol with RLNC-like throughput.
Simulations put it within ~10% of RLNC's p90 latency across all benchmarked message sizes.
The protocol introduces no fundamental algorithmic novelty,
and it stays byzantine-fault tolerant in a p2p network like Ethereum's.
Several mechanisms address the coupon collector problem.
Those internal to the strategy are specified in the RS spec.
The one that shapes the integration is described below.

**Session.** ethp2p broadcast defines a broadcast Session.
Chunks that belong to a message are nested under that message's Session.
Slot timing bounds the lifetime of a Session.

**Preamble.**
The publisher emits a small message announcing the Session.
It is under 1 MTU, it propagates quickly, and it precedes the data.
It sets the authentication and coding parameters for the incoming chunks.
Chunks can still race it through the network.

For execution payloads, the preamble can embed the signed bid or contain a reference to it.
It is the signed bid that legitimizes a builder to propagate a block.
It also carries the cryptographic material needed to authenticate incoming chunks.

**Routing updates.**
In RLNC, intermediate nodes recombine chunks before forwarding.
Recombination raises the chance that a recipient receives innovative data.
In Reed-Solomon, only the origin can produce new valid chunks.
Every other node forwards the chunks it holds as-is,
so chunks travel with their original authentication attached.
The no-recode constraint is both an advantage and a disadvantage.
Authenticating a recombined chunk in RLNC requires homomorphic cryptography to prove
that the chunk remains a valid linear combination of original chunks.
RS avoids that entirely.
But the inability to recode creates the coupon collector problem.
As a node accumulates chunks, the chance that the next arriving chunk is novel drops.

To counter the coupon collector problem,
we introduce a convergence phase in the network once a node holds a threshold of unique chunks.
We set the threshold at L=0.5,
the point where simulation shows the coupon collector problem starting to dominate.
When a peer reaches the threshold, it starts sending bitmaps, the RS strategy's Routing Update.
Senders read the bitmaps to skip chunks a peer already has and to prioritize the chunks it lacks.

**Chunk authentication.**
Bid at preamble, then builder signature at chunk.
The bid authenticates the Session and binds it to the builder's identity.
Each chunk carries a builder signature,
and relays verify that signature against the builder's public key from the bid.

## 5. Open questions

1. **Preamble carries the bid or references it.**
   Embedding the signed bid makes relays self-sufficient and avoids a fetch round trip.
   The preamble must then fit the bid and the coding parameters under the 1 MTU budget.
   A reference keeps the preamble small but forces relays to obtain the bid
   before authenticating any chunk.
2. **Bid-to-body commitment.**
   Today's builder bid signs an execution header, not the full payload body.
   For chunk authentication to be sound, the signed material must commit to the body,
   for example via the transactions hash or a chunk Merkle root.
   Would adding that commitment require a change to the bid format?
3. **Chunks racing the preamble.**
   What does a relay do with chunks that arrive before the preamble: buffer them, for how long,
   at what budget?
4. **Provisional preamble does not fit.**
   The RS spec's provisional preamble carries one hash per chunk.
   Its size grows with n and exceeds 1 MTU at large k.
   The 1 MTU claim holds only under the planned modes: Merkle commitment or builder signature.
5. **Coexistence with gossipsub.**
   Same-topic replacement, parallel topic during transition, or fallback: what is the rollout plan,
   and what do mixed fleets do?
6. **Session lifetime vs the attester deadline.**
   A Session is bounded by slot timing.
   What happens to a Session whose payload misses the attestation deadline,
   and when is it torn down?
7. **Who publishes and who relays.**
   Builder → relay → proposer is the natural flow.
   Do relays and proposers forward chunks, and under what budgets?
