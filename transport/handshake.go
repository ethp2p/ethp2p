package transport

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
	"slices"

	"github.com/ethp2p/ethp2p/identity"
)

const (
	// AlpnEthp2p identifies the shared ethp2p transport protocol.
	AlpnEthp2p = "ethp2p_0"
	// AlpnLibp2p identifies the stock libp2p QUIC transport protocol.
	AlpnLibp2p = "libp2p"
)

// certificatePrefix is the domain-separation tag from the libp2p TLS spec.
const certificatePrefix = "libp2p-tls-handshake:"

// extensionID identifies the certificate extension that binds the long-lived
// identity key to the ephemeral TLS certificate key.
var extensionID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 53594, 1, 1}

// signedKey is the ASN.1 payload carried in extensionID.
type signedKey struct {
	PubKey    []byte
	Signature []byte
}

// handshaker owns one identity certificate and the TLS configuration shared
// by all dials and accepts for that identity. It produces one handshake per
// connection.
type handshaker struct {
	publicKey *identity.PubKey
	peerID    PeerID
	config    tls.Config
}

// remoteIdentitySlot is the per-connection rendezvous between the handshake
// verify callback, which authenticates the remote key, and the dial or accept
// path, which reads it back. The callback runs during the handshake and the
// reader runs only after the handshake completes, so the write happens-before
// the read without further synchronization.
//
// The slot holds the public key rather than a peer ID because the key is the
// authenticated value and the peer ID is derived from it.
//
// The dial path owns its slot as a local variable. The accept path has nothing
// local that spans both the TLS configuration and the accepted connection, so
// it carries its slot on the connection context instead.
type remoteIdentitySlot struct{ key *identity.PubKey }

// remoteIdentityContextKey is the connection-context key under which NewShared's
// ConnContext hook installs the accept-side slot.
type remoteIdentityContextKey struct{}

// newHandshaker configures TLS 1.3 for key, preferring ethp2p over libp2p.
func newHandshaker(key *identity.PrivKey) (*handshaker, error) {
	cert, err := issueCertificate(key)
	if err != nil {
		return nil, err
	}
	pubkey := key.Public()
	return &handshaker{
		publicKey: pubkey,
		peerID:    PeerIDFromKey(pubkey),
		config: tls.Config{
			MinVersion:             tls.VersionTLS13,
			InsecureSkipVerify:     true,
			ClientAuth:             tls.RequireAnyClientCert,
			Certificates:           []tls.Certificate{*cert},
			NextProtos:             []string{AlpnEthp2p, AlpnLibp2p},
			SessionTicketsDisabled: true,
		},
	}, nil
}

// serverConfig returns the listener configuration whose top-level ALPN is
// libp2p. GetConfigForClient replaces it with the full preference order, so the
// listener accepts both ethp2p_0 and libp2p, and hands the per-connection
// identity slot installed by ConnContext to the verify callback.
func (h *handshaker) serverConfig() *tls.Config {
	return &tls.Config{
		NextProtos: []string{AlpnLibp2p},
		GetConfigForClient: func(info *tls.ClientHelloInfo) (*tls.Config, error) {
			slot, _ := info.Context().Value(remoteIdentityContextKey{}).(*remoteIdentitySlot)
			return h.connConfig(slot, ""), nil
		},
	}
}

// connConfig returns a single-connection TLS config whose verify callback
// authenticates the presented certificate, pins it to expect when nonempty, and
// records the identity into slot.
//
// It is the security boundary for inbound and outbound handshakes: the peer
// identity is trustworthy because this callback rejected the connection
// otherwise, and it is authenticated here exactly once per connection. A nil
// slot costs a second authentication when the identity is read back.
func (h *handshaker) connConfig(slot *remoteIdentitySlot, expect PeerID) *tls.Config {
	config := h.config.Clone()
	config.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		chain, err := parseChain(rawCerts)
		if err != nil {
			return err
		}
		key, err := authenticate(chain)
		if err != nil {
			return err
		}
		if actual := PeerIDFromKey(key); expect != "" && expect != actual {
			return ErrPeerMismatch{Expected: expect, Actual: actual}
		}
		if slot != nil {
			slot.key = key
		}
		return nil
	}
	return config
}

// authenticate verifies the single self-signed certificate required by
// the libp2p TLS spec and returns its authenticated identity key.
func authenticate(chain []*x509.Certificate) (*identity.PubKey, error) {
	if len(chain) != 1 {
		return nil, errCertChain
	}
	cert := chain[0]
	i := slices.IndexFunc(cert.Extensions, func(ext pkix.Extension) bool {
		return ext.Id.Equal(extensionID)
	})
	if i < 0 {
		return nil, errNoKeyExtension
	}
	keyExt := cert.Extensions[i]
	verifying := *cert
	verifying.UnhandledCriticalExtensions = slices.DeleteFunc(slices.Clone(cert.UnhandledCriticalExtensions), func(oid asn1.ObjectIdentifier) bool {
		return oid.Equal(keyExt.Id)
	})
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	if _, err := verifying.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		return nil, fmt.Errorf("certificate verification failed: %w", err)
	}
	var binding signedKey
	if _, err := asn1.Unmarshal(keyExt.Value, &binding); err != nil {
		return nil, fmt.Errorf("unmarshalling signed certificate failed: %w", err)
	}
	key, err := identity.UnmarshalPubKey(binding.PubKey)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling public key failed: %w", err)
	}
	certKey, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
	if err != nil {
		return nil, err
	}
	if !key.Verify(append([]byte(certificatePrefix), certKey...), binding.Signature) {
		return nil, errInvalidSig
	}
	return key, nil
}

// parseChain parses the raw certificates supplied to VerifyPeerCertificate.
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
