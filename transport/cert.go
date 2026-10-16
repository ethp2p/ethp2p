package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"time"

	"github.com/ethp2p/ethp2p/identity"
)

const (
	// certLibp2pPubkeyDst is the domain-separation tag from the libp2p TLS spec.
	certLibp2pPubkeyDst = "libp2p-tls-handshake:"
	// certValidityPeriod matches go-libp2p's 100-year certificate lifetime.
	certValidityPeriod = 100 * 365 * 24 * time.Hour
)

// certLibp2pPubkeyExt identifies the certificate extension that binds the long-lived
// identity key to the ephemeral TLS certificate key.
var certLibp2pPubkeyExt = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 53594, 1, 1}

// issueCertificate creates an ephemeral P-256 certificate key and signs that
// key with the identity key. The certificate carries the identity public key
// and signature in certLibp2pPubkeyExt, making it wire-compatible with the libp2p TLS
// handshake.
func issueCertificate(key *identity.PrivKey) (*tls.Certificate, error) {
	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	certKeyPub, err := x509.MarshalPKIXPublicKey(certKey.Public())
	if err != nil {
		return nil, err
	}
	signature := key.Sign(append([]byte(certLibp2pPubkeyDst), certKeyPub...))
	value, err := asn1.Marshal(signedKey{PubKey: key.Public().Marshal(), Signature: signature})
	if err != nil {
		return nil, err
	}
	template, err := certTemplate()
	if err != nil {
		return nil, err
	}
	template.ExtraExtensions = []pkix.Extension{{Id: certLibp2pPubkeyExt, Value: value}}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, certKey.Public(), certKey)
	if err != nil {
		return nil, err
	}
	return &tls.Certificate{Certificate: [][]byte{certDER}, PrivateKey: certKey}, nil
}

// certTemplate returns a self-signed certificate template that matches
// go-libp2p's serial-number and validity choices.
func certTemplate() (*x509.Certificate, error) {
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
