# Erasure coded broadcast for Execution Payloads

Status: superseded by [005-integration-v2](005-integration-v2.md).

## Gossipsub: store-and-forward propagation

Execution payloads are propagated via gossipsub.
Gossipsub is a store-and-forward protocol,
meaning that every peer has to receive the _full_ payload
before it can act on it / make any kind of progress.
As we push the gas limit higher,
Average sizes are slated to increase as a result of Average payload size ...

Serialized network use, say you have a 200KiB payload,
and you are sitting 4 hops away from the original sender.
You need to wait for 3 intermediate nodes to have fully received, validated,
and forwarded the full messages before you even receive the first byte.
When you do, you will probably receive more copies than you need.

Gossipsub is also a push-pull protocol.
Messages can be delivered proactively (push), or demanded (pull).
Ethereum's mesh degree configuration favours push,
the rationale being that Ethereum time is segmented in subslots with strict deadlines
for the delivery of the corresponding messages
(execution payloads, attestations, aggregates),
which includes emission, distribution, forwarding, delivery, processing, and reaction time.
The pull mechanism is viewed as a fault recovery path, as it requires 1.5 RTT .

## Best-effort delivery through redundancy

Given the trustless, byzantine, dynamic nature of the Ethereum network,
it is impossible to route messages through deterministic, infallible paths.
Therefore, nodes interface with the mesh/graph via adjacent neighbours,
relying on redundancy and peer scoring to sustain the required reliability and delivery quality.
Gossipsub makes no guarantees of

Redundancy is achieved by sending D replicas to peers,
with mechanisms in place to curtail duplicates, e.g. IHAVE and IDONTWANT.
However, these are dependent on the peer receiving .
Oftentimes, after a message has been queued for push, it's hard to cancel it,
although this is implementation dependent.
And once the network write starts, gossipsub today has no cancellation semantics to interrupt it
(and they'd be hard to implement without a breaking change).

Point: The information dispersal efficiency and throughput of gossipsub are low.

## Erasure-coded broadcast

This family of broadcast protocols divides the original payload into N chunks,
and apply some form of erasure coding.
The redundancy is thus achieved through information theory instead of full-payload replication.
The unit of propagation is no longer the full payload, but rather the coded chunks.
So instead of handling a 256KiB blob as single unit, one can handle 2KiB chunks (e.g. if k=128).
This streamlines the propagation effort on multiple fronts:
peers now begin making progress at the first chunk,
chunks are smaller and therefore propagate faster, fanouts than be wider, etc.
Ultimately the network can parallelize and spread the same information more efficiently,
reducing duplicates, resulting in lower propagation latencies and higher protocol throughputs
and capacities.

Reduce transport complexity.
Redundancy via routing implies tracking duplicates, control messages, etc.

## ethp2p erasure-coded broadcast

We propose ethp2p EC broadcast: a Reed-Solomon based protocol that achieves RLNC-like throughput,
without introducing fundamental algorithmic novelty,
and while staying secure and byzantine-fault tolerant in a p2p network like Ethereum's.
The coupon collector problem has been addressed by several mechanisms we proceed to describe.

**Session.** ethp2p broadcast formalizes the notion of a broadcast Session.
Chunks pertaining to a message are nested under the broadcast Session for that message.
Slot timing demarcates the lifetime of a Session.

**Preamble.**
The publisher emits a small message announcing the creation of the Session.
This message is under 1 MTU and is propagated really quickly, precedes any data (\*),
and sets the authentication and coding parameters
for the incoming chunks. (\*) These can race in the network.

In the case of execution payloads, the Session preamble can embed the signed bid,
or contain a reference to it.
Since it is the signed bid that legitimatizes a builder to propagate a block
and contains the cryptographic material necessary to authenticate incoming chunks.

**Routing updates**.
In RLNC, intermediate nodes can recombine chunks before forwarding to increase the chance
that recipients will receive innovative data.
However, in Reed Solomon, only the origin
(and nodes holding the uncoded data) can produce valid chunks.
This is both an advantage and a disadvantage.
Acceptable chunk authentication in RLNC requires homomorphic cryptography schemes to prove
that the recombined chunk remains a valid linear combination of original chunks.
In RS, chunks propagate as-is and therefore carry over their original signature.
However, the inability to recombine makes RS suffer from the coupon collector problem.
To alleviate this, we introduce a convergence phase in the network
after a certain threshold of unique chunks are received.
Currently this is L=0.5, empirically observed,
statistically around the threshold at which the coupon collector problem begins manifesting.
When a peer reaches this threshold, it begins dispatching bitmaps.

**Chunk authentication.**
Bid at preamble, then builder signature at chunk.
