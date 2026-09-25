// Package enr parses, verifies, and signs Ethereum Node Records using the v4
// identity scheme. Records preserve unknown values as raw canonical RLP so
// applications can add typed views without changing the wire representation.
package enr

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sort"
	"strings"

	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
	"golang.org/x/crypto/sha3"
)

const maxRecordSize = 300

// Record is an immutable, signature-verified Ethereum Node Record.
type Record struct {
	raw       []byte
	seq       uint64
	signature [64]byte
	pub       *identity.PubKey
	keys      []string
	values    map[string][]byte
}

type signedPair struct {
	key   string
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
	if err != nil || publicKeyItem.kind != rlpString {
		return nil, errors.New("ENR secp256k1 key must be an RLP byte string")
	}
	publicKey, err := identity.ParsePubKey(publicKeyItem.payload)
	if err != nil {
		return nil, fmt.Errorf("invalid ENR secp256k1 public key: %w", err)
	}

	content := make([]byte, 0, len(owned)-len(signature.raw))
	for _, field := range root.children[1:] {
		content = append(content, field.raw...)
	}
	hash := keccak256(encodeRLPList(content))
	if !publicKey.VerifyHash(hash, signature.payload) {
		return nil, errors.New("invalid ENR signature")
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

// PublicKey returns the identity public key verified by Decode or Sign.
func (r *Record) PublicKey() *identity.PubKey { return r.pub }

// PeerID derives the transport peer ID from the record's identity key.
func (r *Record) PeerID() transport.PeerID { return transport.PeerIDFromKey(r.pub) }

// QUIC returns valid IPv4 ip/quic and IPv6 ip6/quic6 endpoints in that order.
// Callers should try each returned endpoint.
func (r *Record) QUIC() []netip.AddrPort {
	var endpoints []netip.AddrPort
	if addr, ok, err := r.Get(IP); ok && err == nil && addr.IsValid() && !addr.IsUnspecified() && !addr.IsMulticast() {
		if port, ok, err := r.Get(QUIC); ok && err == nil && port != 0 {
			endpoints = append(endpoints, netip.AddrPortFrom(addr, port))
		}
	}
	if addr, ok, err := r.Get(IP6); ok && err == nil && addr.IsValid() && !addr.IsUnspecified() && !addr.IsMulticast() && !addr.Is4In6() {
		if port, ok, err := r.Get(QUIC6); ok && err == nil && port != 0 {
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

// Update returns a new record signed by key with sequence number Seq()+1. It
// keeps r's entries and replaces or adds those in pairs. key must be the key
// that signed r.
func (r *Record) Update(key *identity.PrivKey, pairs ...Pair) (*Record, error) {
	if key == nil {
		return nil, errors.New("nil ENR signing key")
	}
	if !key.Public().Equal(r.pub) {
		return nil, errors.New("key did not sign this ENR")
	}
	if r.seq == math.MaxUint64 {
		return nil, errors.New("ENR sequence number exhausted")
	}
	replaced := make(map[string]bool, len(pairs))
	for _, pair := range pairs {
		replaced[pair.key] = true
	}
	merged := make([]Pair, 0, len(r.keys)+len(pairs))
	for _, k := range r.keys {
		if k == "id" || k == "secp256k1" || replaced[k] {
			continue
		}
		raw := r.values[k]
		merged = append(merged, Pair{key: k, encode: func() ([]byte, error) { return raw, nil }})
	}
	return Sign(key, r.seq+1, append(merged, pairs...)...)
}

// Sign creates and verifies a v4 record, adding the required id and identity
// pairs and sorting all pairs by key.
func Sign(key *identity.PrivKey, seq uint64, pairs ...Pair) (*Record, error) {
	if key == nil {
		return nil, errors.New("nil ENR signing key")
	}
	signed := make([]signedPair, 0, len(pairs)+2)
	seen := make(map[string]struct{}, len(pairs)+2)
	for _, pair := range pairs {
		if pair.key == "id" || pair.key == "secp256k1" {
			return nil, fmt.Errorf("ENR key %q is reserved", pair.key)
		}
		if _, duplicate := seen[pair.key]; duplicate {
			return nil, fmt.Errorf("duplicate ENR key %q", pair.key)
		}
		seen[pair.key] = struct{}{}
		if pair.err != nil {
			return nil, fmt.Errorf("encode ENR key %q: %w", pair.key, pair.err)
		}
		if pair.encode == nil {
			return nil, fmt.Errorf("ENR key %q has no encoder", pair.key)
		}
		value, err := pair.encode()
		if err != nil {
			return nil, fmt.Errorf("encode ENR key %q: %w", pair.key, err)
		}
		if _, err := decodeSingleRLP(value); err != nil {
			return nil, fmt.Errorf("encode ENR key %q: invalid RLP value: %w", pair.key, err)
		}
		signed = append(signed, signedPair{key: pair.key, value: value})
	}
	signed = append(signed,
		signedPair{key: "id", value: encodeRLPString([]byte("v4"))},
		signedPair{key: "secp256k1", value: encodeRLPString(key.Public().Bytes())},
	)
	sort.Slice(signed, func(i, j int) bool {
		return signed[i].key < signed[j].key
	})

	content := encodeRLPList(encodeSignedContent(seq, signed))
	hash := keccak256(content)
	signature := key.SignHash(hash)

	body := encodeRLPString(signature)
	body = append(body, encodeUint64(seq)...)
	for _, pair := range signed {
		body = append(body, encodeRLPString([]byte(pair.key))...)
		body = append(body, pair.value...)
	}
	raw := encodeRLPList(body)
	if len(raw) > maxRecordSize {
		return nil, fmt.Errorf("signed ENR is %d bytes; maximum is %d", len(raw), maxRecordSize)
	}
	return Decode(raw)
}

func encodeSignedContent(seq uint64, pairs []signedPair) []byte {
	content := encodeUint64(seq)
	for _, pair := range pairs {
		content = append(content, encodeRLPString([]byte(pair.key))...)
		content = append(content, pair.value...)
	}
	return content
}

func keccak256(message []byte) []byte {
	hash := sha3.NewLegacyKeccak256()
	_, _ = hash.Write(message)
	return hash.Sum(nil)
}
