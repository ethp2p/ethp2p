# ethp2p specifications

ethp2p is a p2p networking stack purpose-built for Ethereum.
These documents describe how it works today, one topic per file.
Folders are numbered in stack order, from foundations up, and files within a folder in the order one would read them.
Each spec's `requires` front matter lists the specs it builds on.
Start with [Overview](00-foundations/01-overview.md) and [Conventions](00-foundations/02-conventions.md), and look terms up in [Terminology](00-foundations/03-terminology.md).
The reference implementation's own behavior (its API, queues and shutdown order) is documented in its Go packages, not here.
Rules for writing and editing specs are in [AGENTS.md](AGENTS.md).

## Specs by area

### Foundations

| Spec | Covers |
| ---- | ------ |
| [Overview](00-foundations/01-overview.md) | The layers and the principles the specs follow |
| [Conventions](00-foundations/02-conventions.md) | Requirements language and byte notation |
| [Terminology](00-foundations/03-terminology.md) | Every defined term |
| [Registries](00-foundations/04-registries.md) | Assigned names and numbers: application protocols, protocols, kinds, frame types |
| [Identity](00-foundations/05-identity.md) | Node keys, peer IDs, signature schemes |
| [Outcome codes](00-foundations/06-outcome-codes.md) | The code space for resets, closes and `GOAWAY` |
| [ENR](00-foundations/07-enr.md) | Node records (not yet specified) |

### Transport

One QUIC connection per peer, shared with libp2p, and everything that happens on it before a protocol sees a stream.

| Spec | Covers |
| ---- | ------ |
| [QUIC and TLS](01-transport/01-quic-and-tls.md) | QUIC and TLS parameters, the certificate, stream credit, how a connection ends |
| [Streams](01-transport/02-streams.md) | Stream headers, frames, protocols, read bounds, how streams end |
| [Connection lifecycle](01-transport/03-connection-lifecycle.md) | The phases of a view, duplicates, closing |
| [libp2p compatibility](01-transport/04-libp2p-compat.md) | Connection forms, telling streams apart, closing a shared connection |
| [Datagrams](01-transport/05-datagrams.md) | Datagram routing (proposed) |

### Peering

| Spec | Covers |
| ---- | ------ |
| [Peering](02-peering/01-peering.md) | Peer selection and scoring (not yet specified) |

### Broadcast

Erasure-coded broadcast, protocol 1 on the stack.

| Spec | Covers |
| ---- | ------ |
| [Broadcast framework](03-broadcast/01-broadcast-framework.md) | Channels, sessions, the strategy contract |
| [Broadcast protocol](03-broadcast/02-broadcast-protocol.md) | One protocol, three kinds: handshake and subscriptions, session open and routing, chunk framing and admission |
| [Reed-Solomon](03-broadcast/05-reed-solomon.md) | The Reed-Solomon strategy |
| [RLNC](03-broadcast/06-rlnc.md) | Random linear network coding strategy (proposed) |

### Privacy

| Spec | Covers |
| ---- | ------ |
| [Privacy](04-privacy/01-privacy.md) | Mixnet, spam resistance, decoy traffic (not yet specified) |

### Control

Protocol 0, which every view serves.

| Spec | Covers |
| ---- | ------ |
| [Control protocol](05-control/01-control.md) | `HELLO`, node records, `GOAWAY`, violations |

### Adoption

| Spec | Covers |
| ---- | ------ |
| [Execution payloads](06-adoption/01-execution-payloads.md) | Broadcast for execution payloads (proposed) |

### Roadmap

Ideas that are not built, each with the problem and why it waits.

| Spec | Covers |
| ---- | ------ |
| [Legacy peers](09-roadmap/01-legacy-peers.md) | Caching that a peer speaks only libp2p |

## Status

Each spec's front matter has a `status`: `implemented`, `proposed` (specified, not built) or `idea` (not yet specified, or not built and waiting).
Specs describe implemented behavior unless their status says otherwise.
