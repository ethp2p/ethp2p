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

type signedKey struct {
	PubKey    []byte
	Signature []byte
}

// handshaker owns one identity certificate and the TLS configuration shared
// by all dials and accepts for that identity. It produces one handshake per
// connection.
type handshaker struct {
	peerID identity.PeerID
	config tls.Config
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
type remoteIdentitySlot struct{ pubKey *identity.PubKey }

type remoteIdentityContextKey struct{}

// newHandshaker configures TLS 1.3 for key, preferring ethp2p over libp2p.
// InsecureSkipVerify turns off chain verification only: connConfig's
// VerifyPeerCertificate does all checking. RequireAnyClientCert makes the
// listener receive the dialer's certificate at all.
func newHandshaker(key *identity.PrivKey) (*handshaker, error) {
	cert, err := issueCertificate(key)
	if err != nil {
		return nil, err
	}
	return &handshaker{
		peerID: key.Public().PeerID(),
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
			slot := info.Context().Value(remoteIdentityContextKey{}).(*remoteIdentitySlot)
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
// otherwise, and it is authenticated here exactly once per connection.
// Build one per dial and per accept: the callback closes over slot and expect,
// so a shared config would mix identities and pins.
func (h *handshaker) connConfig(slot *remoteIdentitySlot, expect identity.PeerID) *tls.Config {
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
		if actual := key.PeerID(); expect != "" && expect != actual {
			return fmt.Errorf("peer ID mismatch: expected %x, actual %x", []byte(expect), []byte(actual))
		}
		slot.pubKey = key
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
		return ext.Id.Equal(certLibp2pPubkeyExt)
	})
	if i < 0 {
		return nil, errNoKeyExtension
	}
	keyExt := cert.Extensions[i]
	// The standard library rejects unhandled critical extensions; the identity
	// extension is handled here, so verify a copy without it in that list.
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
	// The signer signed the PKIX DER re-encoding of the key, not the raw bytes.
	certKey, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
	if err != nil {
		return nil, err
	}
	if !key.Verify(append([]byte(certLibp2pPubkeyDst), certKey...), binding.Signature) {
		return nil, errInvalidSig
	}
	return key, nil
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
