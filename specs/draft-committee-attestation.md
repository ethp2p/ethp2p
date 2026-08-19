# Committee Attestation Broadcast

On the beacon chain, every epoch,
a validator gets to vote once for what it thinks is the tip of the chain
and the justified checkpoint.
Currently, the validators vote in a single slot every epoch on one of the attestation committees.
These votes are disseminated to other nodes in the mesh using gossipsub.
The gossipsub mechanism is inefficient for a couple of reasons.

1. There's no batching of multiple attestations.
   We currently duplicate `AttestationData` for all the attestations.
   Batching attestations allows us to deduplicate and send `AttestationData`
   once in a batch with a list of validators and their indices.

2. There's too much `IHAVE` and `IWANT` overhead when using the hash-based messageID function.
   `IHAVE` and `IWANT` are shared with 25% of our connected peers.
   At 20 bytes per message, this is a significant overhead.
   In some measurements the receive bandwidth for `IHAVE` and `IWANT` is almost as much
   as the bandwidth used to transmit the actual messages themselves.

This spec addresses both of these issues using gossipsub partial message extension.

We don't aggregate signatures when forwarding within the subnet.
It's difficult to aggregate signatures without complicating the protocol
as you cannot aggregate two signatures whose validator sets overlap.
So you can only aggregate signatures whose validator sets are completely disjoint.

## Attestation broadcast using Partial Messages

A node participating in the subnet batches attestations before forwarding them to its mesh peers.
Compared to gossipsub, attestations are batched efficiently by deduplicating attestation data.
Note that this keeps the signatures separate and no aggregations are involved.

For gossiping the state of available attestations,
nodes send the attestation data along with the bitlist of all attesting validators.

## Data types exchanged for Partial Messages

```python
class AttestationData(Container):
    slot: Slot
    index: CommitteeIndex
    beacon_block_root: Root
    source: Checkpoint
    target: Checkpoint

class BatchedAttestation(Container):
    attestation_data: AttestationData
    attestor_indices: Bitlist[MAX_VALIDATORS_PER_COMMITTEE]
    signatures: List[BLSSignature, MAX_VALIDATORS_PER_COMMITTEE]

class CommitteeAttestationPartsMetadata(Container):
    slot: Slot
    attestation_data: AttestationData
    available: Bitlist[MAX_VALIDATORS_PER_COMMITTEE]
    requests: Bitlist[MAX_VALIDATORS_PER_COMMITTEE]
```

A `BatchedAttestation` object contains attestations from validators attesting to the same
`attestation_data`.
The attesting validators' indices in the committee are set to 1 in the `attestor_indices` bitlist.

A `CommitteeAttestationPartsMetadata`'s `available` field is a bitlist indicating the validator
indices in the committee whose attestations we have and the `requests` bitlist indicates the
validator indices whose attestations we want from the peer.

## Protocol

Nodes send messages to peers every `TICK_DURATION`.
Recommended value for `TICK_DURATION` is 20ms.

As attestations themselves are small,
the node always eagerly pushes all the attestations it has to its mesh members.

1. Every `TICK_DURATION`, the node selects the set of attestations it will send to its peers.
   After selecting all the attestations,
   the node SHOULD batch all the attestations with the same `AttestationData` in a single
   `BatchedAttestation` object.
   The node SHOULD NOT send the same attestation twice to its peers.
   As we eagerly forward all attestations in the mesh,
   the node SHOULD NOT send `CommitteeAttestationPartsMetadata` to its mesh peers.

2. The node sends `CommitteeAttestationPartsMetadata`
   when gossiping its set of available attestations.
   The node will send one `CommitteeAttestationPartsMetadata` object
   for every unique `AttestationData` it has.

3. On receiving `CommitteeAttestationPartsMetadata`,
   the node requests attestations it wants from the peer using the `requests` field.
   An empty `requests` bitlist on the `CommitteeAttestationPartsMetadata` indicates no attestations
   are requested.
   The node MAY send its `available` bitlist in the same message.

Nodes MUST NOT deduplicate by (slot, committee position).
In the event of a fork, the committee is a function of the `attestation_data`.
So the same (slot, committee position) may represent different validators across forks.

Requests received on `CommitteeAttestationPartsMetadata` are not persistent.
If a node cannot satisfy the request on reception,
it SHOULD discard it rather than queuing it for later.

Peer scoring stays the same as current gossipsub dissemination.
