---
status: idea
requires:
  - ../01-transport/04-libp2p-compat.md
---

# Remembering legacy peers

A dial to a peer that speaks only libp2p fails with `transport.ErrDialLegacyPeer`.
The stack keeps nothing about that result, so every later `Connect` to the same peer repeats the dial and pays for the same failed attempt.
A caller that retries on a timer, or a discovery loop that keeps returning the same record, wastes dials on peers that will never speak ethp2p.

## Idea

Cache, per peer ID, that a dial found a non-ethp2p peer.
`Connect` consults the cache and returns `ErrDialLegacyPeer` at once while the entry is fresh.
An entry expires after a lifetime, and a changed node record (higher sequence number) or an inbound ethp2p view from the peer clears it.

The cache would record only the negative result of the ALPN dial.
It would not stop a peer that dials us, and it would not affect peers that have an active view.
Entries are advisory: a miss only costs one more dial.

A refinement is to cache per endpoint instead of per peer ID, since one peer can publish several endpoints and only some may be shared with libp2p.
That multiplies the entries and the policy questions below.

## Why not now

- It is new stack state with a lifetime policy: how long an entry lives, how many entries are kept, and what clears it when the peer upgrades.
  A wrong policy hides a peer that has become reachable.
- Today `ErrDialLegacyPeer` stops only the remaining endpoints of one `Connect` call, and callers own retry policy ([libp2p compatibility](../01-transport/04-libp2p-compat.md)).
  Callers know their own discovery cadence better than the stack does.
- Build it when measured wasted dials justify the state,
  and decide ownership then: the stack, or a helper callers opt into.

## Related

- [libp2p compatibility](../01-transport/04-libp2p-compat.md)
- [QUIC and TLS](../01-transport/01-quic-and-tls.md)
