---
status: implemented
requires: []
---

# Identity

A node has one long-lived secp256k1 identity key.
Other nodes know it by its peer ID, a compact identifier derived from the public key.
The same key proves who a connection is with ([QUIC and TLS](../01-transport/01-quic-and-tls.md)), signs the node's record ([Control protocol](../05-control/01-control.md)) and seeds the QUIC stateless reset key.
This document defines the key, its encodings, the peer ID and the signature schemes.
Notation and the meaning of MUST and SHOULD are in [Conventions](02-conventions.md); defined terms are in [Terminology](03-terminology.md).

## Goals

- One key names a node to both ethp2p and libp2p.
- Each key has exactly one peer ID, and each signed message exactly one valid signature.

## Wire format

### Keys

| Value | Encoding | Constraint |
| ----- | -------- | ---------- |
| Private key | 32-byte big-endian scalar | In 1 to n-1 (n = group order); zero, out-of-range and wrong-length values are invalid. |
| Public key, compressed | 33-byte SEC1 | A valid curve point. |
| Public key, wire | libp2p `crypto.proto` `PublicKey` message | Restricted as below. |

```protobuf
message PublicKey {
  KeyType Type = 1;   // MUST be 2 (secp256k1)
  bytes   Data = 2;   // the 33-byte compressed key
}
```

| Field | Type | Constraint |
| ----- | ---- | ---------- |
| `Type` (1) | varint | MUST equal 2. |
| `Data` (2) | bytes | MUST be the 33-byte compressed form of a valid curve point. |

A sender MUST encode the message canonically: `Type` before `Data`, no other field, `Data` compressed.
A receiver MUST reject a `Type` other than 2, a missing `Data`, and `Data` that is not the 33-byte compressed form of a valid point, including the 65-byte uncompressed form.
It skips unknown fields.

### Peer ID

A peer ID is the binary form of a libp2p `peer.ID`: the identity multihash of the wire public key.

```text
peer_id = 0x00 || uvarint(len(public_key)) || public_key   # public_key: the marshalled PublicKey message
```

| Field | Width | Value |
| ----- | ----- | ----- |
| Multihash code | 1 byte | `0x00` (identity) |
| Length | 1 byte | 37 |
| Digest | 37 bytes | The marshalled `PublicKey` message |

A secp256k1 peer ID is 39 bytes.
The peer ID and the key determine each other.

Example: private scalar 1 (`00..01`, 32 bytes) has the compressed public key `0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798`.
Its marshalled `PublicKey` is 37 bytes, and its peer ID is `00 25` followed by them:

```text
08 02                    field 1 (Type), varint 2: secp256k1
12 21                    field 2 (Data), length 33
02 79 be 66 7e f9 dc bb  compressed public key,
ac 55 a0 62 95 ce 87 0b  33 bytes
07 02 9b fc db 2d ce 28
d9 59 f2 81 5b 16 f8 17
98
```

### Signature schemes

Both use the identity key.

| Scheme | Input | Signature | Used by |
| ------ | ----- | --------- | ------- |
| libp2p TLS | message, hashed with SHA-256 | DER-encoded ECDSA | certificate binding |
| ENR v4 | caller-computed 32-byte hash | 64-byte `r\|\|s`; S MUST be low | node records |

A verifier of an ENR v4 signature MUST reject a high-S signature, so each message has exactly one valid signature.
A signature of one scheme does not verify under the other.

### Certificate binding

The identity key signs the certificate key once; the signature travels in the certificate ([QUIC and TLS](../01-transport/01-quic-and-tls.md)):

```text
identity key  --signs-->  "libp2p-tls-handshake:" || DER public key of the certificate key
```

```text
SignedKey ::= SEQUENCE {
    publicKey  OCTET STRING,   -- marshalled identity PublicKey message
    signature  OCTET STRING    -- identity key's DER signature
}
```

`publicKey` is a secp256k1 wire public key.
`signature` is DER ECDSA over the SHA-256 of the signed input.

### Stateless reset key

The QUIC stateless reset key is HKDF-SHA256 over the 32 private key bytes, no salt, info string `ethp2p stateless reset`, 32 bytes of output.
For the scalar-1 key above it is `2228a71fd55ce039ce3e06adfa2802e8e880486b0d1d6f9d624b309fb6c54f89`.

## Behavior

**Parsing.**
A private key parser MUST reject zero, out-of-range and wrong-length scalars instead of reducing them.
A public key parser MUST reject malformed messages, other key types and invalid points.

**Peer ID.**
An endpoint MUST derive a peer ID from the decoded key, re-marshalled canonically, and never from received bytes or from a value the peer sent.

**Identity of a connection.**
A connection's authenticated remote identity is fixed when the connection is created and does not change for its life.
For an `ethp2p_0` connection it is derived from the key the handshake authenticated; the same holds for dialed and accepted connections.
A legacy `libp2p` connection has no ethp2p view; libp2p obtains the identity through its own upgrader.
The local identity is derived from the endpoint's private key.
Authentication is not authorization: a node decides on its own which authenticated peers it keeps.

**Stateless reset key.**
An endpoint SHOULD derive its stateless reset key as above, so the key is stable across restarts with the same identity.
A restarted endpoint then answers packets on its previous connections with stateless resets, and its peers close those connections within one round trip.

**Key types.**
An endpoint MUST accept secp256k1 keys only.
A new key type needs a new `Type` value, a new peer ID length and a decision about the signature scheme, and ships with a new application protocol name ([QUIC and TLS](../01-transport/01-quic-and-tls.md)).

## Failure modes

- **One key, many encodings.**
  The `PublicKey` parser skips unknown fields, so more than one byte string decodes to one key.
  Using received bytes as the identity would let one key appear as many peer IDs; deriving the peer ID from the decoded key prevents it.
  ENR v4 signatures must be low-S for the same reason ([EIP-778]).
- **Key compromise.**
  There is no revocation; a stolen identity key is a stolen identity.
- **Reset key disclosure.**
  The reset key is an HKDF output; the identity key cannot be recovered from it.
- **Disclosure of the peer ID.**
  The peer ID is public by design.

## Rationale

**Two signature schemes on one key.**
ENR v4 requires a 64-byte low-S signature over a caller-computed hash, while the libp2p TLS binding uses a DER signature over SHA-256.
Rejecting high-S gives each ENR message exactly one valid signature.

**secp256k1 only.** libp2p supports several key types. ethp2p accepts one, because ENR v4 uses the same key.

**Stable reset key.**
A stale connection to a restarted endpoint would otherwise linger until it times out and could win the duplicate rule ([Connection lifecycle](../01-transport/03-connection-lifecycle.md)).
Resets close it within one round trip.

## Reference implementation

Package `identity` ([docs]).
Tests that check this spec: `identity/identity_test.go`.
The examples were produced with `identity.PrivKey.Public`, `transport.PeerIDFromKey` and the reset-key derivation in `transport.NewEndpoint`.

[EIP-778]: https://eips.ethereum.org/EIPS/eip-778
[docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p/identity
