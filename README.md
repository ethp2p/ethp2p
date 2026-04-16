# ethp2p

ethp2p is a next-generation p2p networking stack purpose-built for Ethereum. Its design principles are vertical integration, mechanical sympathy, zero idle resources, privacy by default, and the conviction that in a time-critical protocol, consistent performance _is_ an essential part of correctness.

ethp2p replaces generic sequential store-and-forward propagation with object-specific broadcast. For large objects like execution payloads, we adopt erasure-coded broadcast; for smaller objects (e.g. attestations), we design specialized broadcast strategies that leverage the inherent properties of the data. We replace reactive peering with duty-aware proactive connections, full network awareness, and rapid session resumption. We leverage the full potential of QUIC standards to saturate network links and maximize Ethereum's goodput. We thread the fine needle between latency and anonymity to boost network privacy. We grease the critical path per-slot by prioritizing network flows, shaping traffic, and coordinating bandwidth between the consensus and execution layers.

ethp2p is constrained by [EIP-7870 (Hardware and Bandwidth Recommendations)](https://eips.ethereum.org/EIPS/eip-7870). While requirements may adjust in lockstep with the evolution of bandwidth distribution around the world, we are committed to keeping Ethereum universally accessible everywhere. Frugality and efficiency are our best bet at ensuring that everyone can run an Ethereum node while upholding censorship resistance, openness, privacy, and security. That is the job of the networking layer: making Ethereum a World Computer, and not just a Computer.

<img width="1024" height="792" alt="image" src="https://github.com/user-attachments/assets/ee3f0878-123b-410b-9caf-6b15b2b10cc0" />

## Layers

Layering is the bread and butter of networking stacks, and ethp2p couldn't break that tradition. ethp2p comprises five layers:

| Layer     | Notions                                                                                 | Status         |
| --------- | --------------------------------------------------------------------------------------- | -------------- |
| Transport | QUIC-native streams, TLS 1.3 + ECH, BBRv3 congestion control, TCP/QMux fallback         | 🌗 Designing   |
| Peering   | Duty-aware peer selection, composite scoring, supernode emergence, connection pooling   | 🌗 Designing   |
| Broadcast | Erasure-coded parallel dissemination, pluggable coding strategies, per-object protocols | 🌕 Implemented |
| Privacy   | 2-hop mixnet, RLN spam resistance, proof of validator, decoy traffic                    | 🌑 Research    |
| Control   | Slot-phase traffic shaping, duty scheduling, EL coordination, circuit breakers          | 🌗 Designing   |

The first priority is the broadcast layer: an erasure-coded broadcast engine with Ethereum-optimized schemes, a minimal QUIC-based transport, a simulation framework, and a high-performance graphical visualizer.

The transport, peering, and control layers follow. In parallel, we are researching privacy and scrutinizing design decisions to ensure we only advance. See [001-ethp2p](specs/001-ethp2p.md) for the full architecture and design rationale.

## Specs

| Spec                                                     | Description                                                              |
| -------------------------------------------------------- | ------------------------------------------------------------------------ |
| [ethp2p](specs/001-ethp2p.md)                            | Architecture, design rationale, layer responsibilities                   |
| [Broadcast framework](specs/002-ec-broadcast.md)         | Wire protocol, session lifecycle, stream types, Strategy interface       |
| [Reed-Solomon strategy](specs/003-ec-broadcast-rs.md)    | Per-chunk hash verification, bitmap routing, consistent-hash relay dedup |
| [RLNC strategy](specs/004-ec-broadcast-rlnc.md)          | Ristretto255 arithmetic, rank-based routing, subspace fingerprinting     |

When working on specs, run:

```bash
just specs
```

This installs `rumdl` if needed and formats files under `specs/`.

## Transport

🌗 Designing.

- QUIC (RFC 9000) primary: native stream multiplexing, 0-RTT resumption, datagram support (RFC 9221)
- TLS 1.3 with Encrypted Client Hello (ECH) for content and metadata privacy
- BBRv3 congestion control; Prague CC / L4S for ECN-capable paths
- Stream prioritization to favor critical-path traffic during congestion
- Pacing and flow control signals for intelligent load balancing across peers
- Multipath QUIC (RFC 9443) for bandwidth aggregation across interfaces
- MASQUE: QUIC-native proxy protocol, privacy building block and potential relay transport
- TCP/QMux (draft-opik-quic-qmux) fallback for environments where QUIC is blocked
- Censorship resistance: QUIC + TLS 1.3 + ECH traffic indistinguishable from ordinary HTTPS

## Peering

🌗 Designing.

- Full network view via discv5: capability, latency/RTT, geoip, ASN (10k ENRs ~ 7-9 MiB)
- Selfish peer table segments mapped to current and upcoming protocol duties
- Altruistic segments: chain sync, bootstrapping, light client support
- Proactive connection warming during slot downtime; 0-RTT session resumption
- RTT-aware scoring: latency as first-class signal; duty-specific selection (proposers need low-RTT diversity, attesters need subnet peers)
- Diversity enforcement across geographic regions, autonomous systems, client implementations
- Supernode emergence: high-quality peers promoted organically, no top-down orchestration
- Foundation for advanced routing: expander graphs, relay networks, mixnets

## Broadcast

🌕 Implemented. Code: [`broadcast/`](broadcast/).

- Erasure-coded broadcast: chunks + redundancy spread in parallel; partial relaying
- Coding-scheme agnostic: Strategy interface, no changes to wire protocol or session machinery
- Per-object protocols for attestations, aggregates, BALs, beacon blocks
- Optimized Reed-Solomon as primary candidate: per-chunk Merkle authentication, public domain
- Rateless codes (e.g. R10) with attestation-rate feedback loop: encoder adjusts output based on attestation arrival
- Bitmap reconciliation: committees and havelists as bitmaps instead of content-addressed IDs
- MTU-aware batching, ordinal subnet IDs, source-level attestation consolidation
- Latency-tier forwarding: inner (0-60ms), mid (60-120ms), outer (120ms+); inner-tier peers receive chunks first

## Privacy

🌑 Research.

Potential directions:

- Attestations: strongest deanonymization vector (recurring, deterministic, correlatable)
- OHTTP-style 2-hop mixnet built on MASQUE: decouple content from origin via QUIC-native relay proxying
- Rate Limiting Nullifiers (RLN): spam prevention without revealing sender identity
- Privacy Pass: lightweight anonymous tokens for rate compliance without linking requests to identity
- Proof of Validator (PoV): ZK proof of validator set membership (requires endpoint obfuscation)
- Decoy traffic to drown behavioral traces (trades goodput for anonymity)
- Censorship resistance baseline: QUIC + TLS 1.3 + ECH makes traffic indistinguishable from HTTPS
- Graceful degradation: privacy yields to liveness, but default is maximum privacy

## Control

🌗 Designing.

- EL/CL bandwidth coordination: shared pipe, slot-phase-aware priority allocation
- Four slot phases: block arrival, attestation, aggregation, idle/prep
- CL gets maximum bandwidth during block propagation; EL released during idle
- Duty scheduler: precomputes peer sets an epoch ahead, warms connections before duties arrive
- Circuit breakers: misbehaving peer isolation with suspension and cooldown

## Simulation

```bash
# install the CLI
cd sim/cli && uv sync

# run a single simulation (see sim/experiments/)
simctl run simulation.yaml

# compare strategies side by side
simctl experiment run experiment.yaml
```

Two simulation drivers:

- **Shadow**: discrete-event network simulator. Each node is a separate process with simulated network I/O. Scales to 1000+ nodes. Requires [Shadow](https://shadow.github.io/) installed.
- **Simnet**: in-process using Go's testing/synctest. Deterministic, fast iteration, no external deps. Reliable up to ~16 nodes.

Topologies are generated from real data: node placement follows Ethereum's geographic distribution, edge latencies from a country-to-country RTT matrix, bandwidth defaults to 50/50 Mbps (regular) and 50/100 Mbps (builder).

## Client adoption

ethp2p is, in principle, designed for the Consensus Layer, but several components can be adopted by Execution Layer clients, e.g. the QUIC transport, TLS 1.3 handshake, peering techniques, traffic shaping. For unified clients (CL+EL in a single process), we would recommend ethp2p to be the networking stack of choice. We could port the devp2p protocols as-is to run on ethp2p's modern transport, or we could take the opportunity to re-design them from scratch.

## Stance on intellectual property

ethp2p is developed in alignment with CROPS principles: censorship and capture resistance, open source, privacy, security. Open source is non-negotiable.

We explored other coding schemes during development but they did not appear to meet our requirements for unencumbered use. Implementations were created purely for internal experimentation and benchmarking purposes, and only their specs are included.

## License

GNU Lesser General Public License v3.0 (LGPLv3).
