package quictls

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
)

// KeyType is the wire enum for identity key types (libp2p crypto.proto).
type KeyType byte

const (
	KeyTypeRSA       KeyType = 0
	KeyTypeEd25519   KeyType = 1
	KeyTypeSecp256k1 KeyType = 2
	KeyTypeECDSA     KeyType = 3
)

// Key is a public identity key: wire-encodable and verifiable.
type Key interface {
	Type() KeyType
	// Bytes returns the raw key data (the protobuf PublicKey Data field).
	Bytes() []byte
	// Verify reports whether sig is a valid signature over msg.
	Verify(msg, sig []byte) bool
}

// Signer is a private identity key.
type Signer interface {
	Public() Key
	Sign(msg []byte) ([]byte, error)
}

// ID is a peer identity fingerprint: a multihash of the protobuf-encoded
// public key (identity hash if ≤42 bytes, else sha2-256). Byte-identical
// to libp2p peer IDs.
type ID []byte

func (id ID) String() string { return fmt.Sprintf("%x", []byte(id)) }

// NewEd25519Signer generates a new Ed25519 identity key.
func NewEd25519Signer() (Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return ed25519Signer{priv: priv}, nil
}

// NewECDSASigner generates a new ECDSA (P-256) identity key.
func NewECDSASigner() (Signer, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	return ecdsaSigner{priv: priv}, nil
}

// NewRSASigner generates a new RSA identity key. Bits must be at least
// 2048, matching libp2p's minimum.
func NewRSASigner(bits int) (Signer, error) {
	if bits < 2048 {
		return nil, fmt.Errorf("quictls: rsa keys must be >= 2048 bits")
	}
	priv, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err
	}
	return rsaSigner{priv: priv}, nil
}

type ed25519Key struct{ pub ed25519.PublicKey }

func (k ed25519Key) Type() KeyType           { return KeyTypeEd25519 }
func (k ed25519Key) Bytes() []byte           { return k.pub }
func (k ed25519Key) Verify(m, s []byte) bool { return ed25519.Verify(k.pub, m, s) }

type ed25519Signer struct{ priv ed25519.PrivateKey }

func (s ed25519Signer) Public() Key {
	return ed25519Key{pub: s.priv.Public().(ed25519.PublicKey)}
}
func (s ed25519Signer) Sign(m []byte) ([]byte, error) { return ed25519.Sign(s.priv, m), nil }

type ecdsaKey struct {
	pub  *ecdsa.PublicKey
	wire []byte
}

func (k ecdsaKey) Type() KeyType { return KeyTypeECDSA }
func (k ecdsaKey) Bytes() []byte { return k.wire }
func (k ecdsaKey) Verify(m, s []byte) bool {
	var sig struct{ R, S *big.Int }
	if _, err := asn1.Unmarshal(s, &sig); err != nil {
		return false
	}
	hash := sha256.Sum256(m)
	return ecdsa.Verify(k.pub, hash[:], sig.R, sig.S)
}

type ecdsaSigner struct{ priv *ecdsa.PrivateKey }

func (s ecdsaSigner) Public() Key {
	wire, _ := x509.MarshalPKIXPublicKey(&s.priv.PublicKey)
	return ecdsaKey{pub: &s.priv.PublicKey, wire: wire}
}
func (s ecdsaSigner) Sign(m []byte) ([]byte, error) {
	hash := sha256.Sum256(m)
	r, s2, err := ecdsa.Sign(rand.Reader, s.priv, hash[:])
	if err != nil {
		return nil, err
	}
	return asn1.Marshal(struct{ R, S *big.Int }{r, s2})
}

type rsaKey struct {
	pub  *rsa.PublicKey
	wire []byte
}

func (k rsaKey) Type() KeyType { return KeyTypeRSA }
func (k rsaKey) Bytes() []byte { return k.wire }
func (k rsaKey) Verify(m, s []byte) bool {
	hash := sha256.Sum256(m)
	return rsa.VerifyPKCS1v15(k.pub, crypto.SHA256, hash[:], s) == nil
}

type rsaSigner struct{ priv *rsa.PrivateKey }

func (s rsaSigner) Public() Key {
	wire, _ := x509.MarshalPKIXPublicKey(&s.priv.PublicKey)
	return rsaKey{pub: &s.priv.PublicKey, wire: wire}
}
func (s rsaSigner) Sign(m []byte) ([]byte, error) {
	hash := sha256.Sum256(m)
	return rsa.SignPKCS1v15(rand.Reader, s.priv, crypto.SHA256, hash[:])
}

// marshalKey encodes a public key as the protobuf PublicKey message
// { KeyType Type = 1; bytes Data = 2; }.
func marshalKey(k Key) []byte {
	b := k.Bytes()
	out := make([]byte, 0, len(b)+5)
	out = append(out, 0x08, byte(k.Type()), 0x12)
	out = binary.AppendUvarint(out, uint64(len(b)))
	return append(out, b...)
}

// ParseKey decodes a protobuf PublicKey message.
func ParseKey(wire []byte) (Key, error) {
	var typ KeyType = 0xff
	var data []byte
	for len(wire) > 0 {
		tag, n := binary.Uvarint(wire)
		if n <= 0 {
			return nil, errors.New("quictls: malformed public key")
		}
		wire = wire[n:]
		field, wt := int(tag>>3), int(tag&7)
		switch {
		case field == 1 && wt == 0:
			v, m := binary.Uvarint(wire)
			if m <= 0 {
				return nil, errors.New("quictls: malformed public key")
			}
			wire = wire[m:]
			typ = KeyType(v)
		case field == 2 && wt == 2:
			l, m := binary.Uvarint(wire)
			if m <= 0 || l > uint64(len(wire)-m) {
				return nil, errors.New("quictls: malformed public key")
			}
			wire = wire[m:]
			data = wire[:l]
			wire = wire[l:]
		default:
			n, err := skipField(wire, wt)
			if err != nil {
				return nil, err
			}
			wire = wire[n:]
		}
	}
	if typ == 0xff || data == nil {
		return nil, errors.New("quictls: public key missing type or data")
	}
	parse, ok := keyTypes[typ]
	if !ok {
		return nil, fmt.Errorf("quictls: unsupported key type %d", typ)
	}
	return parse(data)
}

func skipField(b []byte, wt int) (int, error) {
	switch wt {
	case 0:
		_, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, errors.New("quictls: malformed public key")
		}
		return n, nil
	case 1:
		if len(b) < 8 {
			return 0, errors.New("quictls: malformed public key")
		}
		return 8, nil
	case 2:
		l, n := binary.Uvarint(b)
		if n <= 0 || l > uint64(len(b)-n) {
			return 0, errors.New("quictls: malformed public key")
		}
		return n + int(l), nil
	case 5:
		if len(b) < 4 {
			return 0, errors.New("quictls: malformed public key")
		}
		return 4, nil
	}
	return 0, errors.New("quictls: malformed public key")
}

var keyTypes = map[KeyType]func([]byte) (Key, error){
	KeyTypeEd25519: parseEd25519,
	KeyTypeECDSA:   parseECDSA,
	KeyTypeRSA:     parseRSA,
}

// RegisterKeyType registers a parser for a key type not built in (e.g.
// secp256k1). Call from an init function; the parser receives the raw key
// data from the wire.
func RegisterKeyType(t KeyType, parse func([]byte) (Key, error)) {
	keyTypes[t] = parse
}

func parseEd25519(b []byte) (Key, error) {
	if len(b) != ed25519.PublicKeySize {
		return nil, errors.New("quictls: invalid ed25519 key")
	}
	return ed25519Key{pub: ed25519.PublicKey(b)}, nil
}

func parseECDSA(b []byte) (Key, error) {
	pub, err := x509.ParsePKIXPublicKey(b)
	if err != nil {
		return nil, fmt.Errorf("quictls: invalid ecdsa key: %s", err)
	}
	ec, ok := pub.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("quictls: invalid ecdsa key")
	}
	return ecdsaKey{pub: ec, wire: b}, nil
}

func parseRSA(b []byte) (Key, error) {
	pub, err := x509.ParsePKIXPublicKey(b)
	if err != nil {
		return nil, fmt.Errorf("quictls: invalid rsa key: %s", err)
	}
	r, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("quictls: invalid rsa key")
	}
	return rsaKey{pub: r, wire: b}, nil
}

const maxInlineKeyLength = 42

// IDFromKey computes the identity fingerprint of a public key: a multihash
// of its protobuf encoding (identity hash if ≤42 bytes, else sha2-256).
func IDFromKey(k Key) ID {
	wire := marshalKey(k)
	if len(wire) <= maxInlineKeyLength {
		return append([]byte{0x00, byte(len(wire))}, wire...)
	}
	sum := sha256.Sum256(wire)
	return append([]byte{0x12, 0x20}, sum[:]...)
}
