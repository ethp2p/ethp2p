# Committee Attestation Broadcast

On the beacon chain, every epoch,
a validator gets to vote once for what it thinks is the tip of the chain and the finalized block.
Currently, the validators vote in a single slot every epoch on one of the attestation committees.
These votes are disseminated to other nodes in the mesh using gossipsub.
The gossipsub mechanism is inefficient for a few reasons.

1. There's too much `IHAVE` and `IWANT` overhead when using the hash based messageID function.
   `IHAVE` and `IWANT` are shared with 25% of our connected peers.
   At 20 bytes per message, this is a significant overhead.
   In some measurements the receive bandwidth for IHave and IWant is almost as much
   as the bandwidth used to transmit the actual messages themselves.
2. There's no batching of multiple attestations.
   We currently duplicate `AttestationData` for all the attestations.
   Batching attestations allows us to deduplicate and send `AttestationData`
   once in a batch with a list of validators and their indices.

This spec addresses both these reasons using gossipsub partial message extension.


## Attestation broadcast using Partial Messages

To transmit attestations using partial messages,
the beacon chain node makes a list of all validators in the committee.
This is straightforward.
The current validator to committee assignment function takes all validators in that epoch
and makes a deterministic pseudorandom shuffle of the list of validators,
and then divides them into equal sized committees.
So at the start of every epoch a validator knows the assignment of validators to the committees
and there's also a deterministic order for the validators in the committee.

```
[... 127889 | 34886, 45, 29182, ... 192839, 12211 | ]
```

The `|` demarcates an individual committee while the numbers there are the validator indices part of
that committee.
This list is easily represented
as a bitlist to indicate presence of attestation by those validators.
In our example `34886` is the leftmost bit, `45` one bit to the right of the leftmost bit and so on.

Stated more tersely we can get the whole list of validators as

```python
validators = compute_committee(indices, seed, index, count)
```
Now validator[i] is represented in our bitlist as the `ith` bit and in attestor_indices list
(defined below) as `i`.

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
    attestor_indices: List[uint16]
    signatures: List[BLSSignature]

class CommitteeAttestationPartsMetadata(Container):
    available: Bitlist[MAX_VALIDATORS_PER_COMMITTEE]
    requests: Bitlist[MAX_VALIDATORS_PER_COMMITTEE]

class BatchedAttestationList(Container):
    attestation_batches: List[BatchedAttestation]
```

A `BatchedAttestation` object contains attestations for the same `attestation_data`.
The attestor indices are indices in the validators list obtained by
`validators = compute_committee(indices, seed, index, count)`.
So an attestation by validator `validators[i]` is represented in `attestor_indices` as `i`.
`batched_attestation.signatures[i]` is the signature of the validator
`validators[batched_attestation.attestor_indices[i]]`.

A `CommitteeAttestationPartsMetadata` object contains the bitlist representation of the validators
whose attestations we have or want.
`available[i] == 1` if the node has received attestation from validator `validators[i]`
where `validators = compute_committee(indices, seed, index, count)`.


## Protocol
The general protocol is straightforward.

1. Every `X`ms the node should forward attestations it has to its peers.
   The node SHOULD NOT send the same attestation twice to its peers.
   After selecting the list of all the attestations that the node intends to send to the peer,
   the node SHOULD batch all the attestations with the same `AttestationData` in a single
   `BatchedAttestation` object.
   Then it should send all the `BatchedAttestation` objects in a single `BatchedAttestationList`.
   Sending them as a single object helps dedup the topic_id that is repeated in gossipsub messages.

2. PartsMetadata is used to gossip which attestations you have and which you want from the peer.
   An empty `requests` field on the partsmetadata objects MUST be treated as a request
   for all attestations.

<!-- run shadow simulations for this -->
As attestations themselves are so small, we always eagerly push to all our mesh members.

TODO: <Other details of the protocol>

## Arguments against using Partial Messages
Using gossipsub we get limited control over the protocol.
Gossipsub and/or its implementations have some limitations like D value is shared by all topics,
the mesh handling isn't exposed to the app, no per topic streams.
An argument can be made that it's easier to make something custom
for attestation broadcast than to address all of these problems within gossipsub.
