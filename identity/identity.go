// Package identity implements the secp256k1 node identity keys shared by the
// transport handshake and Ethereum Node Records, and the libp2p peer ID derived
// from a public key.
//
// Two signature schemes use the same key. Sign and Verify hash a message with
// SHA-256 and use DER signatures, as the libp2p TLS handshake requires.
// SignHash and VerifyHash take a caller-computed 32-byte hash and use 64-byte
// r||s signatures with low S, as the ENR v4 identity scheme requires.
package identity

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	decrecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"google.golang.org/protobuf/encoding/protowire"
)

// secp256k1KeyType is the libp2p crypto.proto enum value for secp256k1.
const secp256k1KeyType = 2

// PrivKey is a secp256k1 identity private key.
type PrivKey struct{ key *secp256k1.PrivateKey }

// PubKey is a secp256k1 identity public key.
type PubKey struct{ key *secp256k1.PublicKey }

// GenPrivKey generates a key from a cryptographically secure random source.
func GenPrivKey() (*PrivKey, error) {
	key, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, err
	}
	return &PrivKey{key: key}, nil
}

// ParsePrivKey parses the 32-byte secret scalar produced by [PrivKey.Bytes].
// Out-of-range and zero scalars are rejected rather than reduced.
func ParsePrivKey(secret []byte) (*PrivKey, error) {
	key := secp256k1.PrivKeyFromBytes(secret)
	// PrivKeyFromBytes reduces its input modulo the group order, so a round
	// trip that changes the bytes means the input was out of range or not 32
	// bytes long. The zero scalar round-trips but has no public key.
	if !bytes.Equal(key.Serialize(), secret) || key.Key.IsZero() {
		return nil, errors.New("invalid secp256k1 private key")
	}
	return &PrivKey{key: key}, nil
}

// Bytes returns the 32-byte secret scalar.
func (k *PrivKey) Bytes() []byte { return k.key.Serialize() }

// Public returns the public key for k.
func (k *PrivKey) Public() *PubKey { return &PubKey{key: k.key.PubKey()} }

// Sign returns a DER signature over the SHA-256 hash of message.
func (k *PrivKey) Sign(message []byte) []byte {
	hash := sha256.Sum256(message)
	return decrecdsa.Sign(k.key, hash[:]).Serialize()
}

// SignHash returns a 64-byte r||s signature with low S over a 32-byte hash.
func (k *PrivKey) SignHash(hash []byte) []byte {
	// SignCompact prefixes a recovery byte, which neither scheme carries.
	return decrecdsa.SignCompact(k.key, hash, true)[1:]
}

// ParsePubKey parses a 33-byte compressed SEC1 public key.
func ParsePubKey(compressed []byte) (*PubKey, error) {
	if len(compressed) != secp256k1.PubKeyBytesLenCompressed {
		return nil, fmt.Errorf("invalid secp256k1 key: compressed key is %d bytes, want %d", len(compressed), secp256k1.PubKeyBytesLenCompressed)
	}
	key, err := secp256k1.ParsePubKey(compressed)
	if err != nil {
		return nil, fmt.Errorf("invalid secp256k1 key: %w", err)
	}
	return &PubKey{key: key}, nil
}

// UnmarshalPubKey parses a libp2p crypto.proto PublicKey, as produced by
// [PubKey.Marshal]. It rejects malformed messages, other key types, and key
// data that is not the 33-byte compressed form, so one key has one encoding.
func UnmarshalPubKey(wire []byte) (*PubKey, error) {
	var keyType uint64
	var keyData []byte
	for len(wire) > 0 {
		number, wireType, n := protowire.ConsumeTag(wire)
		if n < 0 {
			return nil, fmt.Errorf("malformed public key: %w", protowire.ParseError(n))
		}
		wire = wire[n:]
		switch {
		case number == 1 && wireType == protowire.VarintType:
			keyType, n = protowire.ConsumeVarint(wire)
		case number == 2 && wireType == protowire.BytesType:
			keyData, n = protowire.ConsumeBytes(wire)
		default:
			n = protowire.ConsumeFieldValue(number, wireType, wire)
		}
		if n < 0 {
			return nil, fmt.Errorf("malformed public key: %w", protowire.ParseError(n))
		}
		wire = wire[n:]
	}
	if keyType != secp256k1KeyType || keyData == nil {
		return nil, errors.New("unsupported key type: secp256k1 required")
	}
	return ParsePubKey(keyData)
}

// Bytes returns the 33-byte compressed SEC1 encoding of k.
func (k *PubKey) Bytes() []byte { return k.key.SerializeCompressed() }

// Marshal returns the canonical libp2p crypto.proto PublicKey encoding of k.
func (k *PubKey) Marshal() []byte {
	wire := protowire.AppendTag(nil, 1, protowire.VarintType)
	wire = protowire.AppendVarint(wire, secp256k1KeyType)
	wire = protowire.AppendTag(wire, 2, protowire.BytesType)
	return protowire.AppendBytes(wire, k.Bytes())
}

// Verify reports whether signature is a valid DER signature over the SHA-256
// hash of message.
func (k *PubKey) Verify(message, signature []byte) bool {
	sig, err := decrecdsa.ParseDERSignature(signature)
	if err != nil {
		return false
	}
	hash := sha256.Sum256(message)
	return sig.Verify(hash[:], k.key)
}

// VerifyHash reports whether signature is a valid 64-byte r||s signature with
// low S over a 32-byte hash. High-S signatures are rejected so that each
// message has exactly one valid signature.
func (k *PubKey) VerifyHash(hash, signature []byte) bool {
	if len(signature) != 64 {
		return false
	}
	var r, s secp256k1.ModNScalar
	if r.SetByteSlice(signature[:32]) || r.IsZero() || s.SetByteSlice(signature[32:]) || s.IsZero() || s.IsOverHalfOrder() {
		return false
	}
	return decrecdsa.NewSignature(&r, &s).Verify(hash, k.key)
}

// PeerID is the binary multihash representation used by libp2p peer.ID. A
// PeerID is not the human-readable base-encoded form of that multihash.
type PeerID string

// PeerIDFromKey returns the inline identity-multihash peer ID of key.
func PeerIDFromKey(key *PubKey) PeerID {
	raw := key.Marshal()
	id := []byte{0} // identity multihash code
	id = protowire.AppendVarint(id, uint64(len(raw)))
	return PeerID(append(id, raw...))
}
