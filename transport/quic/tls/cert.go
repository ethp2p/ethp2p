package quictls

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"time"
)

const (
	// certValidityPeriod matches go-libp2p: ~100 years.
	certValidityPeriod = 100 * 365 * 24 * time.Hour
	// certificatePrefix is the domain separation prefix for the identity
	// signature over the certificate key.
	certificatePrefix = "libp2p-tls-handshake:"
	// alpn is the ALPN protocol negotiated by the handshake.
	alpn = "libp2p"
)

// extensionID is the x509 extension OID carrying the identity key
// (1.3.6.1.4.1.53594.1.1, libp2p TLS spec).
var extensionID = []int{1, 3, 6, 1, 4, 1, 53594, 1, 1}

type signedKey struct {
	PubKey    []byte
	Signature []byte
}

// GenerateSignedExtension signs the certificate's public key with the
// identity key and returns it as an x509 extension.
func GenerateSignedExtension(sk Signer, pubKey crypto.PublicKey) (pkix.Extension, error) {
	keyBytes := marshalKey(sk.Public())
	certKeyPub, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		return pkix.Extension{}, err
	}
	signature, err := sk.Sign(append([]byte(certificatePrefix), certKeyPub...))
	if err != nil {
		return pkix.Extension{}, err
	}
	value, err := asn1.Marshal(signedKey{PubKey: keyBytes, Signature: signature})
	if err != nil {
		return pkix.Extension{}, err
	}
	return pkix.Extension{Id: extensionID, Critical: false, Value: value}, nil
}

// keyToCertificate generates a fresh ECDSA P-256 key and a self-signed
// certificate tying it to the identity key.
func keyToCertificate(sk Signer, certTmpl *x509.Certificate) (*tls.Certificate, error) {
	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	extension, err := GenerateSignedExtension(sk, certKey.Public())
	if err != nil {
		return nil, err
	}
	certTmpl.ExtraExtensions = append(certTmpl.ExtraExtensions, extension)
	certDER, err := x509.CreateCertificate(rand.Reader, certTmpl, certTmpl, certKey.Public(), certKey)
	if err != nil {
		return nil, err
	}
	return &tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  certKey,
	}, nil
}

func certTemplate() (*x509.Certificate, error) {
	bigNum := big.NewInt(1 << 62)
	sn, err := rand.Int(rand.Reader, bigNum)
	if err != nil {
		return nil, err
	}
	subjectSN, err := rand.Int(rand.Reader, bigNum)
	if err != nil {
		return nil, err
	}
	return &x509.Certificate{
		SerialNumber: sn,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(certValidityPeriod),
		// RFC 3280 requires the issuer field to be set.
		Subject: pkix.Name{SerialNumber: subjectSN.String()},
	}, nil
}
