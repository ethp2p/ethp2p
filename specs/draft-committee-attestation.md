# Committee Attestation Broadcast

On the beacon chain, every epoch,
a validator gets to vote once for what it thinks is the tip of the chain and the finalized block.
Currently, the validators vote in a single slot every epoch on one of the attestation committees.
These votes are disseminated to other nodes in the mesh using gossipsub.
The gossipsub mechanism is inefficient for a few reasons.

1. There's no batching of multiple attestations.
   We currently duplicate `AttestationData` for all the attestations.
   Batching attestations allows us to deduplicate and send `AttestationData`
   once in a batch with a list of validators and their indices.

2. There's too much `IHAVE` and `IWANT` overhead when using the hash based messageID function.
   `IHAVE` and `IWANT` are shared with 25% of our connected peers.
   At 20 bytes per message, this is a significant overhead.
   In some measurements the receive bandwidth for `IHAVE` and `IWANT` is almost as much
   as the bandwidth used to transmit the actual messages themselves.

This spec addresses both these reasons using gossipsub partial message extension.


## Attestation broadcast using Partial Messages

A node participating in the subnet batches attestations for 20ms
before forwarding them to its mesh peers.
This forwarding system is the same as gossipsub but we batch the attestations more efficiently,
ensuring that we send the attestation data once for a single batch.

For gossiping with peers the node sends the list of validators whose attestations it has
in the current slot to our gossip peers.
When receiving `IHAVE` gossip over partial messages,
the node waits for 20ms before sending our request to the peer for the attestations.
When the node receives a request for a specific attestation in the form of an `IWANT`
it forwards the requested attestations to the peer after 20ms.


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
    attestor_indices: List[uint32, MAX_VALIDATORS_PER_COMMITTEE]
    signatures: List[BLSSignature, MAX_VALIDATORS_PER_COMMITTEE]

class CommitteeAttestationPartsMetadata(Container):
    slot: Slot
    available: List[uint32, MAX_VALIDATORS_PER_COMMITTEE]
    requests: List[uint32, MAX_VALIDATORS_PER_COMMITTEE]

class BatchedAttestationList(Container):
    attestation_batches: List[BatchedAttestation, MAX_ATTESTATION_BATCHES]
```

A `BatchedAttestation` object contains attestations from validators
attesting to the same `attestation_data`.
The `attestor_indices` are validator indices for the attesting validators.
`signature[i]` is the attestation by the validator `attestor_indices[i]`.

A `CommitteeAttestationPartsMetadata`'s `available` field contains the list of validators
whose attestations we have and the `requests` field contains the list of validators
whose attestations we want from the peer.

## Protocol
The general protocol is straightforward.

1. Every `X`ms the node should forward attestations it has to its peers.
   The node SHOULD NOT send the same attestation twice to its peers.
   After selecting the list of all the attestations that the node intends to send to the peer,
   the node SHOULD batch all the attestations with the same `AttestationData` in a single
   `BatchedAttestation` object.
   Then it should send all the `BatchedAttestation` objects in a single `BatchedAttestationList`.
   Sending them as a single object helps dedup the topic_id that is repeated in gossipsub messages.

2. `CommitteeAttestationPartsMetadata` is used to gossip which attestations you have
   and which you want from the peer.
   An empty `requests` field on the `CommitteeAttestationPartsMetadata` objects MUST be
   treated as a request for all attestations.

<!-- run shadow simulations for this -->
As attestations themselves are so small, we always eagerly push to all our mesh members.

TODO: <Other details of the protocol>

## Arguments against using Partial Messages
Using gossipsub we get limited control over the protocol.
Gossipsub and/or its implementations have some limitations like D value is shared by all topics,
the mesh handling isn't exposed to the app, no per topic streams.
An argument can be made that it's easier to make something custom
for attestation broadcast than to address all of these problems within gossipsub.
