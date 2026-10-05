package quictls

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
)

func newTestIdentity(t *testing.T, advertiseEthp2p bool) *Identity {
	t.Helper()
	signer, err := NewEd25519Signer()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := NewIdentity(signer, Config{AdvertiseEthp2pALPN: advertiseEthp2p})
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func TestIDFromKey(t *testing.T) {
	// ed25519 protobuf encoding is 36 bytes → identity multihash (0x00).
	signer, err := NewEd25519Signer()
	if err != nil {
		t.Fatal(err)
	}
	id := IDFromKey(signer.Public())
	if len(id) != 38 || id[0] != 0x00 || id[1] != 36 {
		t.Fatalf("expected identity multihash, got %x", id)
	}

	// secp256k1 protobuf encoding is 37 bytes → identity multihash (0x00).
	secpSigner, err := NewSecp256k1Signer()
	if err != nil {
		t.Fatal(err)
	}
	sid := IDFromKey(secpSigner.Public())
	if len(sid) != 39 || sid[0] != 0x00 || sid[1] != 37 {
		t.Fatalf("expected identity multihash, got %x", sid)
	}

	// rsa 2048 protobuf encoding is > 42 bytes → sha2-256 multihash (0x12 0x20).
	rsaSigner, err := NewRSASigner(2048)
	if err != nil {
		t.Fatal(err)
	}
	rid := IDFromKey(rsaSigner.Public())
	if len(rid) != 34 || rid[0] != 0x12 || rid[1] != 0x20 {
		t.Fatalf("expected sha2-256 multihash, got %x", rid)
	}
}

func TestParseKeyRoundtrip(t *testing.T) {
	ed25519Signer, err := NewEd25519Signer()
	if err != nil {
		t.Fatal(err)
	}
	ecdsaSigner, err := NewECDSASigner()
	if err != nil {
		t.Fatal(err)
	}
	rsaSigner, err := NewRSASigner(2048)
	if err != nil {
		t.Fatal(err)
	}
	secpSigner, err := NewSecp256k1Signer()
	if err != nil {
		t.Fatal(err)
	}
	signers := []Signer{ed25519Signer, ecdsaSigner, rsaSigner, secpSigner}
	for _, s := range signers {
		k := s.Public()
		parsed, err := ParseKey(marshalKey(k))
		if err != nil {
			t.Fatalf("parse: %s", err)
		}
		if parsed.Type() != k.Type() || !bytes.Equal(parsed.Bytes(), k.Bytes()) {
			t.Fatalf("roundtrip mismatch: %v %x vs %x", parsed.Type(), parsed.Bytes(), k.Bytes())
		}
		msg := []byte("hello")
		sig, err := s.Sign(msg)
		if err != nil {
			t.Fatal(err)
		}
		if !parsed.Verify(msg, sig) {
			t.Fatal("signature verification failed")
		}
		if parsed.Verify(msg, []byte("bogus")) {
			t.Fatal("bogus signature verified")
		}
	}
}

func TestParseKeyUnknownType(t *testing.T) {
	// type 9, data 0x00
	if _, err := ParseKey([]byte{0x08, 0x09, 0x12, 0x01, 0x00}); err == nil {
		t.Fatal("expected error for unknown key type")
	}
	// truncated
	if _, err := ParseKey([]byte{0x08, 0x01, 0x12, 0x20, 0x00}); err == nil {
		t.Fatal("expected error for truncated key")
	}
}

func TestVerifyPeerCertSelf(t *testing.T) {
	identity := newTestIdentity(t, false)
	raw := identity.config.Certificates[0].Certificate[0]
	cert, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	key, id, err := VerifyPeerCert([]*x509.Certificate{cert})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(id, identity.ID()) {
		t.Fatalf("ID mismatch: %x vs %x", id, identity.ID())
	}
	if !bytes.Equal(key.Bytes(), identity.PublicKey().Bytes()) {
		t.Fatal("key mismatch")
	}
}

func TestVerifyPeerCertTampered(t *testing.T) {
	identity := newTestIdentity(t, false)
	raw := identity.config.Certificates[0].Certificate[0]
	bad := append([]byte(nil), raw...)
	bad[len(bad)/2] ^= 0xff
	cert, err := x509.ParseCertificate(bad)
	if err == nil {
		// parse succeeded; verification must still fail
		if _, _, err := VerifyPeerCert([]*x509.Certificate{cert}); err == nil {
			t.Fatal("expected error for tampered certificate")
		}
	}
}

func TestVerifyPeerCertMissingExtension(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "no-extension"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := VerifyPeerCert([]*x509.Certificate{cert}); err == nil {
		t.Fatal("expected error for certificate without key extension")
	}
}

func TestHandshakeRoundtrip(t *testing.T) {
	for _, tc := range []struct {
		name       string
		advertise  bool
		negotiated string
	}{
		{"libp2p-only", false, "libp2p"},
		{"ethp2p_0", true, "ethp2p_0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestIdentity(t, tc.advertise)
			server := newTestIdentity(t, tc.advertise)

			conf, keyCh := client.ClientConfig(server.ID())
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			done := make(chan error, 1)
			serverState := make(chan tls.ConnectionState, 1)
			go func() {
				srv := tls.Server(serverConn, server.ServerConfig())
				if err := srv.Handshake(); err != nil {
					done <- err
					return
				}
				serverState <- srv.ConnectionState()
				done <- nil
			}()

			cli := tls.Client(clientConn, conf)
			if err := cli.Handshake(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}

			// client derived the server's identity from the handshake
			key := <-keyCh
			if !bytes.Equal(server.ID(), IDFromKey(key)) {
				t.Fatal("client derived wrong peer ID")
			}
			// server re-derives the client's identity from the TLS state
			state := <-serverState
			_, id, err := VerifyPeerCert(state.PeerCertificates)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(client.ID(), id) {
				t.Fatal("server derived wrong peer ID")
			}
			// both sides advertise the same ALPNs; the server's preference wins
			if state.NegotiatedProtocol != tc.negotiated {
				t.Fatalf("negotiated %q, want %q", state.NegotiatedProtocol, tc.negotiated)
			}
		})
	}
}

func TestClientConfigPeerMismatch(t *testing.T) {
	client := newTestIdentity(t, false)
	server := newTestIdentity(t, false)

	conf, _ := client.ClientConfig(ID("wrong"))
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	done := make(chan error, 1)
	go func() {
		srv := tls.Server(serverConn, server.ServerConfig())
		done <- srv.Handshake()
	}()

	if err := tls.Client(clientConn, conf).Handshake(); err == nil {
		t.Fatal("expected handshake to fail on peer ID mismatch")
	}
	<-done
}
