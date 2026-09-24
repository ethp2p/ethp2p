// Package enr parses, verifies, and signs Ethereum Node Records using the v4
// identity scheme. Records preserve unknown values as raw canonical RLP so
// applications can add typed views without changing the wire representation.
package enr

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	decrecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/ethp2p/ethp2p/transport"
	"golang.org/x/crypto/sha3"
)

const maxRecordSize = 300

// Record is an immutable, signature-verified Ethereum Node Record.
type Record struct {
	raw       []byte
	seq       uint64
	signature [64]byte
	pub       *secp256k1.PublicKey
	keys      []string
	values    map[string][]byte
}

type signedEntry struct {
	name  string
	value []byte
}

// Parse decodes the unpadded base64url text form of an ENR and verifies it.
func Parse(text string) (*Record, error) {
	if !strings.HasPrefix(text, "enr:") {
		return nil, errors.New("ENR text must start with enr:")
	}
	encoded := text[len("enr:"):]
	if len(encoded) == 0 || len(encoded) > base64.RawURLEncoding.EncodedLen(maxRecordSize) {
		return nil, errors.New("invalid ENR base64 length")
	}
	for _, character := range []byte(encoded) {
		if !((character >= 'A' && character <= 'Z') ||
			(character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_') {
			return nil, errors.New("ENR must use unpadded base64url")
		}
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode ENR base64url: %w", err)
	}
	return Decode(raw)
}

// Decode parses canonical RLP bytes and verifies the v4 signature.
func Decode(raw []byte) (*Record, error) {
	if len(raw) > maxRecordSize {
		return nil, fmt.Errorf("ENR exceeds %d-byte limit", maxRecordSize)
	}
	owned := append([]byte(nil), raw...)
	root, rest, err := decodeRLP(owned)
	if err != nil {
		return nil, describeRLPError(err)
	}
	if len(rest) != 0 {
		return nil, errors.New("trailing bytes after ENR")
	}
	if root.kind != rlpList {
		return nil, errors.New("ENR must be an RLP list")
	}
	if len(root.children) < 2 || (len(root.children)-2)%2 != 0 {
		return nil, errors.New("ENR must contain a signature, sequence, and key/value pairs")
	}
	signature := root.children[0]
	if signature.kind != rlpString || len(signature.payload) != 64 {
		return nil, errors.New("ENR signature must contain 64 bytes")
	}
	seq, err := decodeUint(root.children[1])
	if err != nil {
		return nil, fmt.Errorf("invalid ENR sequence: %w", err)
	}

	values := make(map[string][]byte, (len(root.children)-2)/2)
	keys := make([]string, 0, (len(root.children)-2)/2)
	var previous []byte
	for i := 2; i < len(root.children); i += 2 {
		key, value := root.children[i], root.children[i+1]
		if key.kind != rlpString {
			return nil, errors.New("ENR key must be an RLP byte string")
		}
		if i > 2 && bytes.Compare(previous, key.payload) >= 0 {
			return nil, errors.New("ENR keys must be strictly ascending")
		}
		previous = key.payload
		name := string(key.payload)
		keys = append(keys, name)
		values[name] = append([]byte(nil), value.raw...)
	}

	idRaw, ok := values["id"]
	if !ok {
		return nil, errors.New("ENR is missing id")
	}
	id, err := decodeSingleRLP(idRaw)
	if err != nil || id.kind != rlpString || string(id.payload) != "v4" {
		return nil, errors.New("ENR id must be v4")
	}
	publicKeyRaw, ok := values["secp256k1"]
	if !ok {
		return nil, errors.New("ENR is missing secp256k1 public key")
	}
	publicKeyItem, err := decodeSingleRLP(publicKeyRaw)
	if err != nil || publicKeyItem.kind != rlpString || len(publicKeyItem.payload) != 33 {
		return nil, errors.New("ENR secp256k1 key must be a 33-byte compressed public key")
	}
	publicKey, err := secp256k1.ParsePubKey(publicKeyItem.payload)
	if err != nil {
		return nil, fmt.Errorf("invalid ENR secp256k1 public key: %w", err)
	}

	content := make([]byte, 0, len(owned)-len(signature.raw))
	for _, field := range root.children[1:] {
		content = append(content, field.raw...)
	}
	hash := keccak256(encodeRLPList(content))
	if err := verifySignature(signature.payload, hash, publicKey); err != nil {
		return nil, err
	}
	var signatureBytes [64]byte
	copy(signatureBytes[:], signature.payload)
	return &Record{
		raw:       owned,
		seq:       seq,
		signature: signatureBytes,
		pub:       publicKey,
		keys:      keys,
		values:    values,
	}, nil
}

// Encode returns a copy of the original canonical RLP bytes.
func (r *Record) Encode() []byte {
	if r == nil {
		return nil
	}
	return append([]byte(nil), r.raw...)
}

// String returns the unpadded base64url text form of the record.
func (r *Record) String() string {
	if r == nil {
		return ""
	}
	return "enr:" + base64.RawURLEncoding.EncodeToString(r.raw)
}

// Seq returns the record sequence number.
func (r *Record) Seq() uint64 {
	if r == nil {
		return 0
	}
	return r.seq
}

// PublicKey returns a copy of the secp256k1 identity public key verified by
// Decode or Sign.
func (r *Record) PublicKey() *secp256k1.PublicKey {
	if r == nil || r.pub == nil {
		return nil
	}
	pub := *r.pub
	return &pub
}

// PeerID derives the transport peer ID from the record's secp256k1 identity.
func (r *Record) PeerID() transport.PeerID {
	if r == nil || r.pub == nil {
		return ""
	}
	return transport.NewPubKey(r.pub).PeerID()
}

// QUIC returns valid IPv4 ip/quic and IPv6 ip6/quic6 endpoints in that order.
// Callers should try each returned endpoint.
func (r *Record) QUIC() []netip.AddrPort {
	var endpoints []netip.AddrPort
	if addr, ok, err := Get(r, IP); ok && err == nil && addr.IsValid() && !addr.IsUnspecified() && !addr.IsMulticast() {
		if port, ok, err := Get(r, QUIC); ok && err == nil && port != 0 {
			endpoints = append(endpoints, netip.AddrPortFrom(addr, port))
		}
	}
	if addr, ok, err := Get(r, IP6); ok && err == nil && addr.IsValid() && !addr.IsUnspecified() && !addr.IsMulticast() && !addr.Is4In6() {
		if port, ok, err := Get(r, QUIC6); ok && err == nil && port != 0 {
			endpoints = append(endpoints, netip.AddrPortFrom(addr, port))
		}
	}
	return endpoints
}

func (r *Record) encodeCanonical() []byte {
	if r == nil {
		return nil
	}
	body := encodeRLPString(r.signature[:])
	body = append(body, encodeUint64(r.seq)...)
	for _, key := range r.keys {
		body = append(body, encodeRLPString([]byte(key))...)
		body = append(body, r.values[key]...)
	}
	return encodeRLPList(body)
}

// Sign creates and verifies a v4 record, adding the required id and identity
// entries and sorting all entries by their key bytes.
func Sign(key *secp256k1.PrivateKey, seq uint64, entries ...Entry) (*Record, error) {
	if key == nil || key.Key.IsZero() {
		return nil, errors.New("invalid ENR signing key")
	}
	pairs := make([]signedEntry, 0, len(entries)+2)
	seen := make(map[string]struct{}, len(entries)+2)
	for _, entry := range entries {
		if entry.name == "id" || entry.name == "secp256k1" {
			return nil, fmt.Errorf("ENR key %q is reserved", entry.name)
		}
		if _, duplicate := seen[entry.name]; duplicate {
			return nil, fmt.Errorf("duplicate ENR key %q", entry.name)
		}
		seen[entry.name] = struct{}{}
		if entry.err != nil {
			return nil, fmt.Errorf("encode ENR key %q: %w", entry.name, entry.err)
		}
		if entry.encode == nil {
			return nil, fmt.Errorf("ENR key %q has no encoder", entry.name)
		}
		value, err := entry.encode()
		if err != nil {
			return nil, fmt.Errorf("encode ENR key %q: %w", entry.name, err)
		}
		if _, err := decodeSingleRLP(value); err != nil {
			return nil, fmt.Errorf("encode ENR key %q: invalid RLP value: %w", entry.name, err)
		}
		pairs = append(pairs, signedEntry{name: entry.name, value: value})
	}
	pairs = append(pairs,
		signedEntry{name: "id", value: encodeRLPString([]byte("v4"))},
		signedEntry{name: "secp256k1", value: encodeRLPString(key.PubKey().SerializeCompressed())},
	)
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare([]byte(pairs[i].name), []byte(pairs[j].name)) < 0
	})

	content := encodeRLPList(encodeSignedContent(seq, pairs))
	hash := keccak256(content)
	compact := decrecdsa.SignCompact(key, hash, true)
	signature := compact[1:]

	body := encodeRLPString(signature)
	body = append(body, encodeUint64(seq)...)
	for _, pair := range pairs {
		body = append(body, encodeRLPString([]byte(pair.name))...)
		body = append(body, pair.value...)
	}
	raw := encodeRLPList(body)
	if len(raw) > maxRecordSize {
		return nil, fmt.Errorf("signed ENR is %d bytes; maximum is %d", len(raw), maxRecordSize)
	}
	return Decode(raw)
}

func encodeSignedContent(seq uint64, entries []signedEntry) []byte {
	content := encodeUint64(seq)
	for _, entry := range entries {
		content = append(content, encodeRLPString([]byte(entry.name))...)
		content = append(content, entry.value...)
	}
	return content
}

func verifySignature(signature, hash []byte, publicKey *secp256k1.PublicKey) error {
	var r, s secp256k1.ModNScalar
	if r.SetByteSlice(signature[:32]) || r.IsZero() || s.SetByteSlice(signature[32:]) || s.IsZero() {
		return errors.New("invalid ENR signature scalar")
	}
	if s.IsOverHalfOrder() {
		return errors.New("high-S ENR signature")
	}
	if !decrecdsa.NewSignature(&r, &s).Verify(hash, publicKey) {
		return errors.New("invalid ENR signature")
	}
	return nil
}

func keccak256(message []byte) []byte {
	hash := sha3.NewLegacyKeccak256()
	_, _ = hash.Write(message)
	return hash.Sum(nil)
}
