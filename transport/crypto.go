package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
	"math/big"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	decrecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	// secp256k1KeyType is the libp2p crypto.proto enum value for secp256k1.
	secp256k1KeyType = 2
	// certValidityPeriod matches go-libp2p's 100-year certificate lifetime.
	certValidityPeriod = 100 * 365 * 24 * time.Hour
)

// PubKey is a secp256k1 identity public key. A zero PubKey is invalid:
// Verify returns false on it, the other methods panic.
type PubKey struct {
	pub *secp256k1.PublicKey
}

// Bytes returns the 33-byte compressed SEC1 encoding of k.
func (k *PubKey) Bytes() []byte {
	return k.pub.SerializeCompressed()
}

// DecodePubKey parses a libp2p crypto.proto PublicKey. It rejects malformed
// messages and key types other than secp256k1.
func DecodePubKey(wire []byte) (*PubKey, error) {
	var keyType uint64
	var keyData []byte
	for len(wire) > 0 {
		number, wireType, n := protowire.ConsumeTag(wire)
		if n < 0 {
			return nil, fmt.Errorf("malformed public key: %v", protowire.ParseError(n))
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
			return nil, fmt.Errorf("malformed public key: %v", protowire.ParseError(n))
		}
		wire = wire[n:]
	}
	if keyType != secp256k1KeyType || keyData == nil {
		return nil, errPubKeyRequired
	}
	pub, err := secp256k1.ParsePubKey(keyData)
	if err != nil {
		return nil, fmt.Errorf("invalid secp256k1 key: %w", err)
	}
	return &PubKey{pub: pub}, nil
}

// Verify reports whether signature is a valid DER-encoded secp256k1 signature
// for the SHA-256 hash of message.
func (k *PubKey) Verify(message, signature []byte) bool {
	if k == nil || k.pub == nil {
		return false
	}
	sig, err := decrecdsa.ParseDERSignature(signature)
	if err != nil {
		return false
	}
	hash := sha256.Sum256(message)
	return sig.Verify(hash[:], k.pub)
}

// PeerID returns the inline identity-multihash peer ID for k.
func (k *PubKey) PeerID() PeerID {
	wire := k.MarshalKey()
	if wire == nil {
		return ""
	}
	id := []byte{0}
	id = protowire.AppendVarint(id, uint64(len(wire)))
	return PeerID(append(id, wire...))
}

// MarshalKey returns the canonical libp2p crypto.proto PublicKey encoding of k.
func (k *PubKey) MarshalKey() []byte {
	key := k.Bytes()
	wire := protowire.AppendTag(nil, 1, protowire.VarintType)
	wire = protowire.AppendVarint(wire, secp256k1KeyType)
	wire = protowire.AppendTag(wire, 2, protowire.BytesType)
	return protowire.AppendBytes(wire, key)
}

// PrivKey is a secp256k1 identity private key. A zero PrivKey is invalid:
// Sign returns an error on it, the other methods panic.
type PrivKey struct {
	priv *secp256k1.PrivateKey
}

// GenPrivKey generates a secp256k1 identity key with a cryptographically secure
// random source.
func GenPrivKey() (*PrivKey, error) {
	priv, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, err
	}
	return &PrivKey{priv: priv}, nil
}

// Bytes returns the 32-byte secp256k1 secret scalar encoding of k.
func (k *PrivKey) Bytes() []byte {
	return k.priv.Serialize()
}

// Public returns the public key corresponding to k.
func (k *PrivKey) Public() *PubKey {
	return &PubKey{pub: k.priv.PubKey()}
}

// Sign hashes message with SHA-256 and returns a DER-encoded secp256k1
// signature. It returns an error for an invalid private key.
func (k *PrivKey) Sign(message []byte) ([]byte, error) {
	if k == nil || k.priv == nil {
		return nil, errNilSigner
	}
	hash := sha256.Sum256(message)
	return decrecdsa.Sign(k.priv, hash[:]).Serialize(), nil
}

// issueCertificate creates an ephemeral P-256 certificate key and signs that
// key with k. The certificate carries the identity public key and signature in
// extensionID, making it wire-compatible with the libp2p TLS handshake.
func (k *PrivKey) issueCertificate() (*tls.Certificate, error) {
	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	certKeyPub, err := x509.MarshalPKIXPublicKey(certKey.Public())
	if err != nil {
		return nil, err
	}
	signature, err := k.Sign(append([]byte(certificatePrefix), certKeyPub...))
	if err != nil {
		return nil, err
	}
	value, err := asn1.Marshal(signedKey{PubKey: k.Public().MarshalKey(), Signature: signature})
	if err != nil {
		return nil, err
	}
	template, err := defaultCertTemplate()
	if err != nil {
		return nil, err
	}
	template.ExtraExtensions = []pkix.Extension{{Id: extensionID, Value: value}}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, certKey.Public(), certKey)
	if err != nil {
		return nil, err
	}
	return &tls.Certificate{Certificate: [][]byte{certDER}, PrivateKey: certKey}, nil
}

// defaultCertTemplate returns a self-signed certificate template that matches
// go-libp2p's serial-number and validity choices.
func defaultCertTemplate() (*x509.Certificate, error) {
	limit := big.NewInt(1 << 62)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, err
	}
	subjectSerial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, err
	}
	return &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(certValidityPeriod),
		Subject:      pkix.Name{SerialNumber: subjectSerial.String()},
	}, nil
}
