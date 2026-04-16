# ethp2p

ethp2p is a next-generation p2p networking stack purpose-built for Ethereum.
Its design principles are vertical integration, mechanical sympathy, zero idle resources,
privacy by default, and the conviction that in a time-critical protocol,
consistent performance _is_ an essential part of correctness.

ethp2p replaces generic sequential store-and-forward propagation with object-specific broadcast.
For large objects like execution payloads, we adopt erasure-coded broadcast; for smaller objects
(e.g. attestations),
we design specialized broadcast strategies that leverage the inherent properties of the data.
We replace reactive peering with duty-aware proactive connections, full network awareness,
and rapid session resumption.
We leverage the full potential of QUIC standards to saturate network links
and maximize Ethereum's goodput.
We thread the fine needle between latency and anonymity to boost network privacy.
We grease the critical path per-slot by prioritizing network flows, shaping traffic,
and coordinating bandwidth between the consensus and execution layers.

ethp2p is constrained by
[EIP-7870 (Hardware and Bandwidth Recommendations)](https://eips.ethereum.org/EIPS/eip-7870).
While requirements may adjust in lockstep with the evolution of bandwidth distribution around the
world, we are committed to keeping Ethereum universally accessible everywhere.
Frugality and efficiency are our best bet at ensuring that everyone can run an Ethereum node
while upholding censorship resistance, openness, privacy, and security.
That is the job of the networking layer: making Ethereum a World Computer, and not just a Computer.

## Layers

Layering is the bread and butter of networking stacks,
and ethp2p couldn't break that tradition. ethp2p comprises five layers:

| Layer     | Notions                                                                                 | Status         |
| --------- | --------------------------------------------------------------------------------------- | -------------- |
| Transport | QUIC-native streams, TLS 1.3 + ECH, BBRv3 congestion control, TCP/QMux fallback         | 🌗 Designing   |
| Peering   | Duty-aware peer selection, composite scoring, supernode emergence, connection pooling   | 🌗 Designing   |
| Broadcast | Erasure-coded parallel dissemination, pluggable coding strategies, per-object protocols | 🌕 Implemented |
| Privacy   | 2-hop mixnet, RLN spam resistance, proof of validator, decoy traffic                    | 🌑 Research    |
| Control   | Slot-phase traffic shaping, duty scheduling, EL coordination, circuit breakers          | 🌗 Designing   |

Each layer has a single responsibility, clear interfaces, and the ability to degrade independently.
The layers share context (slot timing, duties, peer quality) to enable cross-layer optimization;
isolation is for failure containment, not information hiding.
There is no plug'n'play modularity:
each layer's design deliberately compounds with the next for maximum mechanical sympathy.

```text
┌───────────────────────────────────────────────────────────────────────┐
│                           CONSENSUS CLIENT                            │
│                (beacon chain, fork choice, validator)                 │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│                             CONTROL PLANE                             │
│  Duty Scheduler · Traffic Shaper · EL/CL Conductor · Observability    │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│                            BROADCAST LAYER                            │
│  Block · Attestation · Aggregate · Blob/DAS · PTC/IL · BAL            │
│  Erasure coding engine (Reed-Solomon, rateless codes)                 │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│                             PRIVACY LAYER                             │
│  2-Hop Mixnet · Rate Limiting Nullifiers · OHTTP · Privacy Pass       │
│  ZK Proofs · Decoy Generator                                          │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│                             PEERING LAYER                             │
│  Network View · Peer Quality · Strategic Peering · Supernodes         │
│  Connection Pool · QUIC Session Resumption · Discovery V5             │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│                            TRANSPORT LAYER                            │
│  Stream Manager · QUIC (primary) · TCP/QMux (fallback)                │
│  TLS 1.3 + ECH · BBRv3/Prague CC · Multipath · Flow Control           │
└───────────────────────────────────────────────────────────────────────┘
```

## Transport

🌗 Designing.

QUIC (RFC 9000) is the primary transport: native stream multiplexing without head-of-line blocking,
0-RTT resumption, and datagram support (RFC 9221) for fire-and-forget messages like attestations.
TLS 1.3 with Encrypted Client Hello (ECH) hides both content and connection metadata.
Congestion control defaults to BBRv3, with Prague CC / L4S available for ECN-capable paths.
We leverage stream prioritization to favor critical-path traffic during congestion.
We factor in pacing and flow control signals to load balance traffic across peers more
intelligently.
Multipath QUIC (RFC 9443) aggregates bandwidth across interfaces when available.
For environments where QUIC is blocked,
TCP with QMux (draft-opik-quic-qmux) provides stream multiplexing as a degraded fallback.

QUIC has a censorship resistance dividend. ethp2p traffic over QUIC with TLS 1.3
and ECH is indistinguishable from ordinary HTTPS: same ports, same handshake, same packet structure.
The more our traffic looks like normal internet traffic, the harder it is to censor.
This is not just a privacy feature;
it is a precondition for operating Ethereum nodes in adversarial network environments.

Several QUIC capabilities are load-bearing for the design.
Stream priorities let us favor block propagation over background sync during congestion.
Exposed pacing and flow control signals enable the control plane to load balance across peers
when a write would block for lack of flow control credit.
Pluggable congestion control (BBRv3, Prague CC for L4S) matches the algorithm to path
characteristics.
Multipath QUIC aggregates bandwidth across interfaces, particularly useful between supernodes.
MASQUE provides a QUIC-native proxy protocol that doubles as a privacy building block
and potential relay transport.
On the horizon: datagram optimizations, TLS handshake improvements
(including better 0-RTT resumption), and GRO for reduced per-packet overhead.

<!-- MISSING from design doc:
- Stream model: varint protocol ID per stream, no multistream-select, protocol ID registry (blocks=0x01, attestations=0x02...), stream manager with priority scheduling and stale preemption
- Departure from libp2p protocol negotiation: one round trip per stream eliminated, compounds at thousands of streams per slot
- TCP fallback details: Noise XX handshakes, fallback detection, periodic QUIC retry
- Vertical integration argument: "we do not abstract over transports in the libp2p sense... the transport layer knows about deadlines, priorities, and slot phases because the transport layer is QUIC"
- Security layer: ECH hides SNI, not just content but metadata of connections
- Connection migration for mobile/multi-homed nodes
-->

## Peering

🌗 Designing.

Peers keep an ample view of the entire network via discv5, identifying peers by capability,
latency/RTT, geoip, and ASN.
Storing ENR records of 10k peers only costs 7-9MiB, so a full network view is viable.
During slot downtime, they schedule tasks to warm up connections to peers and suspend them,
so they can be resumed later via 0-RTT if needed.
Peer tables are created intentionally, and are structured in selfish and altruistic segments.
Selfish segments map to the node's current and upcoming protocol duties,
with slots for attestation subnet subscription, committee membership, DAS column propagation,
aggregate participation, and more.
Conversely, altruistic segments reserve slots to help other peers sync the chain,
bootstrap to the network, and to let light clients observe consensus.

On top of this structure, it is possible to build more sophisticated routing,
such as latency-sensitive or expander graph-like topologies,
as well as privacy-oriented structures like relay networks and small mixnets.

Supernodes.
The Ethereum network comprises nodes with varying resources; this happens naturally.
Ethereum should keep operating even if all nodes satisfy the bare minimum EIP-7870 requirements.
But it should be able to leverage excess resources opportunistically,
without top-down orchestration.
Nodes continuously test peer quality of service,
locally deciding to promote high-performing peers or demote them when they regress.
If enough nodes do this across the network,
a "highway" emerges organically without deliberate configuration.
Supernodes help performance, but their absence does not break the network.
They are an optimization layer; the base layer is self-sufficient.

RTT awareness runs through peer scoring and selection.
Each peer's round-trip latency is a first-class signal alongside reliability, bandwidth,
and behavioral history; scores decay over time and update on events, not heartbeats.
When the duty scheduler selects peers for a given role, it factors in latency:
a proposer needs low-RTT peers with geographic diversity to maximize first-hop reach;
an attester needs subnet peers and nearby aggregators;
a sync committee member needs persistent connections with stable RTT.
Diversity enforcement across regions, autonomous systems,
and client implementations prevents latency optimization from collapsing the topology into a single
cluster.

<!-- MISSING from design doc:
- Discovery details: full DHT crawl, ENR extensions (custody columns, validator proof commitment, protocol versions, coarse geographic hint)
- Composite network view structure: peers map, topology graph with quality annotations, subnets map, columns map, Bloom filter for validator presence
- Peer scoring model: RTT, reliability, bandwidth, behavioral signals; decay over time; event-triggered updates (no heartbeat)
- Strategic peering dispatch by duty type: proposer (high fanout, geographic diversity), attester (subnet peers, aggregators), aggregator (subnet + global), sync committee (persistent connections), PTC (current committee), column custody (specific columns)
- Diversity enforcement across geographic regions, ASes, client implementations
- Supernode emergence: top ~10% by composite score, meritocratic, ephemeral; absence doesn't break network (cf. Flashbots relay)
- Connection pool tiers: tier 1 (validator-proven), tier 2 (high-quality full nodes), tier 3 (light/transient); warmth states (hot, warm, cold)
- Session resumption mechanics: prepare_for_slot() lookahead 1-2 slots, activate_duty_peers() at slot boundary, 0-RTT tokens cached
- "The validator schedule is deterministic and known epochs in advance. This is what mechanical sympathy means in practice."
-->

## Broadcast

🌕 Implemented.
Code: [`broadcast/`](broadcast/).

Erasure-coded broadcast breaks a large message into smaller chunks, adds redundancy through coding,
and spreads them across the network in parallel.
Any node that collects enough valid chunks reconstructs the original.
A relay with partial data is already contributing.
The framework is coding-scheme agnostic:
adding a new scheme requires implementing the Strategy interface,
with no changes to the wire protocol, session machinery, or stream multiplexing.

For smaller objects like attestations, aggregates, block-level access lists, and beacon blocks,
we specialize the propagation technique.
Gossipsub, by contrast, treats every message identically: content-addressed dedup,
IHAVE/IWANT chatter, shared streams across topics.
This is a reasonable default for unknown workloads.
But Ethereum objects tend to be bursty, time-sensitive, within specific size ranges,
and often predictable (e.g. nodes know which attestations to expect in each committee).

Reconciliation primitives are very useful here.
Instead of content-addressing messages and gossiping message IDs,
we can model committees and gossip havelists as mere bitmaps,
where each index corresponds to the expected validator.
This is an example of the principle of vertical integration.
We can batch attestations to amortize MTU.
Instead of identifying subnets by long topic names
(which represent 27% of the actual attestation data), we can identify them by ordinal.
Attesters could fan out more aggressively on the first hop (but this degrades privacy further).
We could allow batching attestations at source,
where beacon nodes managing multiple validators assigned to the same committee can send a single
batched attestation instead of many individual ones.
If we consolidate attestation volume, we can reduce the number of subnets.

Reed-Solomon is the primary candidate for production deployment.
It offers per-chunk authentication via Merkle proofs
(each chunk can be verified independently before contributing to decoding)
and sits comfortably in the public domain.
The current implementation covers the full strategy lifecycle: emit planning, bitmap-based routing,
consistent-hash relay dedup, and forward multiplier budgeting.

Rateless codes (e.g. R10) are a natural complement.
Where RS fixes the coding rate at the origin, a rateless encoder produces chunks indefinitely,
letting the network decide when "enough" have arrived.
The feedback signal is attestation rate: if attestations arrive quickly after a block,
propagation succeeded and the encoder can stop; if attestations lag, the encoder increases output.
This turns attestations into an implicit ACK channel for broadcast quality.

Broadcast is also latency-sensitive.
When a node has chunks to forward, it classifies peers into latency tiers: inner
(0-60ms RTT), mid (60-120ms), and outer (120ms+).
Inner-tier peers receive chunks first,
creating a propagation wavefront that expands outward through the fastest paths.
Combined with erasure coding (which reduces per-peer transfer size),
tier-aware forwarding exploits both topology and coding gain.

| Spec                                             | Description                                                              |
| ------------------------------------------------ | ------------------------------------------------------------------------ |
| [Broadcast framework](002-ec-broadcast.md)       | Wire protocol, session lifecycle, stream types, Strategy interface       |
| [Reed-Solomon strategy](003-ec-broadcast-rs.md)  | Per-chunk hash verification, bitmap routing, consistent-hash relay dedup |
| [RLNC strategy](004-ec-broadcast-rlnc.md)        | Ristretto255 arithmetic, rank-based routing, subspace fingerprinting     |

<!-- MISSING from design doc:
- Erasure coding framing: "transforms broadcast from a bandwidth-concentration problem into a bandwidth-distribution problem"
- Framework design: three stream types (BCAST, SESS, CHUNK) with per-stream flow control
- Pull-based strategy dispatch: framework polls after every state-mutating event, synchronous on event loop, no locks on hot path; Work channel exception for async producers
- RS details: emit planner (min-heap by allocation count), Fibonacci hashing for per-relay priority, forward multiplier budget
- RLNC details: generation/basis structure, relay recoding, rank-based routing (subspace fingerprinting with pivot columns)
- RS vs RLNC tradeoff: "RS has per-chunk authentication; RLNC has relay diversity"
- Per-object broadcast protocols:
  - Blocks: EC push with rateless extension, sqrt(n) first-hop fanout, attestation-rate feedback loop, deadline-aware
  - Attestations: MTU-aware batching, QUIC datagrams, mixnet routing, 300ms target
  - Blobs (PeerDAS): column-based distribution, cell-level deltas, 10x bandwidth reduction target
  - Aggregates: direct push to ~20 high-quality global peers
  - PTC votes: committee-scoped push
  - Inclusion lists (FOCIL): two-phase (IL committee, then global after quorum)
- RTT-sensitive broadcast: latency tiers (inner 0-60ms, mid 60-120ms, outer 120ms+), tier-aware forwarding
-->

## Privacy

🌑 Research.

Attestations are the strongest deanonymization vector in Ethereum: recurring, deterministic,
correlatable.
A passive observer monitoring outbound attestation timing over a few epochs can link IP addresses to
validator identities.

We are working to bake privacy-preserving primitives into ethp2p.
The difficulty is striking the right tradeoff with latency.
Some impact is inevitable, but at least its ceiling should be strictly bounded.
We have considered OHTTP-style 2-hop mixnet built on MASQUE
(decoupling content from origin via QUIC-native relay proxying),
potentially routing attestations through VRF-selected relays that rotate each epoch.
With traffic now being obfuscated, we have to worry about spam.
Rate Limiting Nullifiers (RLN) can prevent spam without revealing sender identity,
but they come at a complexity cost.
Privacy Pass offers a lighter-weight alternative:
anonymous tokens that prove rate compliance without linking requests to identity.
Proof of Validator (PoV) could have enabled prioritized meshes via ZK proofs of validator set
membership, but the same credential can be used to irrevocably prove that a given endpoint runs an
Ethereum validator, so not apt unless the endpoint can be obfuscated too.
Decoy traffic could drown out behavioral traces
that would otherwise reveal identity to sophisticated attackers,
but reserving bandwidth for this reduces goodput.

The transport layer contributes a baseline:
running over QUIC with TLS 1.3 and ECH makes ethp2p traffic indistinguishable from ordinary HTTPS.
The more our traffic looks like normal internet traffic, the harder it is to censor.

How should the network behave if the privacy layer itself becomes degraded?
For example, if mixnet congestion increases jitter and latency guarantees are broken,
should we revert to more transparent communication
(e.g. normal gossip or direct messaging),
or should the backpressure degrade the chain too as long as liveness is not threatened?

<!-- MISSING from design doc:
- Deanonymization elaboration: "Once linked, validators become targets for censorship, DoS, and coercion"
- 2-hop mixnet flow: validator encrypts two layers → relay 1 strips outer → relay 2 strips inner → injects into subnet. Injection point unlinkable to origin.
- Relay selection: VRF-based randomness from beacon chain, rotating set per epoch, relays must be validator-proven (Sybil resistance)
- Latency budget: <100ms total, ~50ms per hop; timing jitter 10-50ms per hop
- RLN mechanics: membership derived from deposit, ZK proof of validator set membership + rate compliance, nullifier per epoch, duplicate nullifiers → slashing
- RLN verification at relay entry points
- PoV mechanics: ZK proof of private key knowledge for a public key in active validator set without revealing which; nullifier from secret key + epoch
- PoV purpose vs RLN: RLN gates mixnet, PoV gates prioritized mesh (tier 1 connections)
- Decoy specifics: default 10% of real traffic, indistinguishable in format/size/timing
- Graceful degradation: "Privacy yields to liveness, but the default is maximum privacy, and degradation requires genuine failure, not convenience"
-->

## Control

🌗 Designing.

The EL and CL share the bandwidth envelope defined in EIP-7870, yet today they do not coordinate.
They compete for the same pipe:
the EL pushes transactions and state sync while the CL propagates blocks, attestations, and blobs.
Neither knows what the other is doing, and neither yields.
The control plane acts as a top-down conductor,
allocating priority to different flows depending on where the node is in the slot
and what remains on the critical path: has the block arrived,
has the execution payload been validated, are we attesting, is aggregation complete.

Ethereum's slot structure makes this possible.
The control plane divides each slot into four phases
(block arrival, attestation, aggregation, idle/prep) and allocates bandwidth accordingly.
During block propagation, the CL receives maximum bandwidth and the EL is throttled.
During idle, bandwidth is released to the EL for mempool sync and state serving.

The duty scheduler precomputes peer sets an epoch ahead and warms connections before duties arrive.
Circuit breakers isolate misbehaving peers with suspension and cooldown.

<!-- MISSING from design doc:
- Duty scheduling mechanics: assignments from consensus client, sorted map keyed by slot, epoch-start prefetch, slot-start activation
- Slot phase table with timing: 12s slots (0-2s block, 2-4s attestation, 4-8s aggregation, 8-12s idle/prep), 6s slots compress proportionally
- Phase-priority-bandwidth table (Critical/High/Medium/Low mapping)
- EL coordination interface: signal_throttle() / signal_release(), requires EL-side support, not mandatory for basic operation
- Circuit breaker details: misbehavior trigger → suspension → cooldown → restore; per-peer rate limiting as softer protection
- Health monitoring: periodic checks against thresholds, per-subsystem degradation modes to prevent cascading failures
-->
