// Package quictls implements the wire-level libp2p QUIC/TLS handshake:
// TLS 1.3 with a self-signed certificate that carries the peer's identity
// key in a custom x509 extension. It is wire-compatible with go-libp2p
// (ALPN "libp2p", extension OID 1.3.6.1.4.1.53594.1.1) but imports nothing
// outside the standard library.
package quictls

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"slices"
)

// Config configures the handshake.
type Config struct {
	// AdvertiseEthp2pALPN advertises the "ethp2p_0" ALPN in addition to
	// "libp2p", so ethp2p-aware peers can negotiate it. go-libp2p peers
	// don't know it and fall back to "libp2p".
	AdvertiseEthp2pALPN bool
}

// Identity holds an identity key and the certificate that ties it to the
// TLS handshake.
type Identity struct {
	config tls.Config
	pub    Key
}

// NewIdentity creates an Identity from a signer, generating a fresh
// self-signed certificate.
func NewIdentity(sk Signer, cfg Config) (*Identity, error) {
	if sk == nil {
		return nil, errors.New("quictls: nil signer")
	}
	tmpl, err := certTemplate()
	if err != nil {
		return nil, err
	}
	cert, err := keyToCertificate(sk, tmpl)
	if err != nil {
		return nil, err
	}
	nextProtos := []string{alpn}
	if cfg.AdvertiseEthp2pALPN {
		nextProtos = []string{alpnEthp2p, alpn}
	}
	return &Identity{
		pub: sk.Public(),
		config: tls.Config{
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: true, // we verify the cert chain ourselves
			ClientAuth:         tls.RequireAnyClientCert,
			Certificates:       []tls.Certificate{*cert},
			VerifyPeerCertificate: func(_ [][]byte, _ [][]*x509.Certificate) error {
				panic("quictls: tls config not specialized for peer")
			},
			NextProtos:             nextProtos,
			SessionTicketsDisabled: true,
		},
	}, nil
}

// PublicKey returns the identity's public key.
func (i *Identity) PublicKey() Key { return i.pub }

// ID returns the identity's fingerprint.
func (i *Identity) ID() ID { return IDFromKey(i.pub) }

// ClientConfig returns a single-use tls.Config that pins the remote
// identity to expect and delivers the verified remote key on the returned
// channel. The handshake fails if the peer's identity does not match.
func (i *Identity) ClientConfig(expect ID) (*tls.Config, <-chan Key) {
	keyCh := make(chan Key, 1)
	conf := i.config.Clone()
	conf.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) (err error) {
		defer func() {
			if rerr := recover(); rerr != nil {
				err = fmt.Errorf("quictls: panic when processing peer certificate: %s", rerr)
			}
		}()
		defer close(keyCh)
		chain, err := parseChain(rawCerts)
		if err != nil {
			return err
		}
		key, id, err := VerifyPeerCert(chain)
		if err != nil {
			return err
		}
		if expect != nil && !bytes.Equal(expect, id) {
			return ErrPeerMismatch{Expected: expect, Actual: id}
		}
		keyCh <- key
		return nil
	}
	return conf, keyCh
}

// ServerConfig returns a tls.Config for listening. It verifies the peer's
// certificate chain during the handshake but accepts any identity; call
// VerifyPeerCert on the connection's TLS state after Accept to learn it.
func (i *Identity) ServerConfig() *tls.Config {
	conf := i.config.Clone()
	conf.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		chain, err := parseChain(rawCerts)
		if err != nil {
			return err
		}
		_, _, err = VerifyPeerCert(chain)
		return err
	}
	return conf
}

// ErrPeerMismatch is returned when a peer's identity does not match the
// expected one.
type ErrPeerMismatch struct {
	Expected ID
	Actual   ID
}

func (e ErrPeerMismatch) Error() string {
	return fmt.Sprintf("quictls: peer ID mismatch: expected %s, actual %s", e.Expected, e.Actual)
}

// VerifyPeerCert verifies a peer's certificate chain and returns the
// peer's identity key and ID. The chain must be a single self-signed
// certificate carrying the identity key extension.
func VerifyPeerCert(chain []*x509.Certificate) (Key, ID, error) {
	if len(chain) != 1 {
		return nil, nil, errors.New("quictls: expected one certificate in the chain")
	}
	cert := chain[0]
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	i := slices.IndexFunc(cert.Extensions, func(ext pkix.Extension) bool {
		return slices.Equal(ext.Id, extensionID)
	})
	if i < 0 {
		return nil, nil, errors.New("quictls: expected certificate to contain the key extension")
	}
	keyExt := cert.Extensions[i]
	cert.UnhandledCriticalExtensions = slices.DeleteFunc(cert.UnhandledCriticalExtensions, func(o asn1.ObjectIdentifier) bool {
		return o.Equal(keyExt.Id)
	})
	if _, err := cert.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		return nil, nil, fmt.Errorf("quictls: certificate verification failed: %s", err)
	}
	var sk signedKey
	if _, err := asn1.Unmarshal(keyExt.Value, &sk); err != nil {
		return nil, nil, fmt.Errorf("quictls: unmarshalling signed certificate failed: %s", err)
	}
	key, err := ParseKey(sk.PubKey)
	if err != nil {
		return nil, nil, fmt.Errorf("quictls: unmarshalling public key failed: %s", err)
	}
	certKeyPub, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
	if err != nil {
		return nil, nil, err
	}
	if !key.Verify(append([]byte(certificatePrefix), certKeyPub...), sk.Signature) {
		return nil, nil, errors.New("quictls: signature invalid")
	}
	return key, IDFromKey(key), nil
}

func parseChain(rawCerts [][]byte) ([]*x509.Certificate, error) {
	chain := make([]*x509.Certificate, len(rawCerts))
	for i, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return nil, err
		}
		chain[i] = cert
	}
	return chain, nil
}
