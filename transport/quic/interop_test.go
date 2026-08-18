package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"testing"
	"time"

	quictls "github.com/ethp2p/ethp2p/transport/quic/tls"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

func newTestIdentity(t *testing.T, advertiseEthp2p bool) *quictls.Identity {
	t.Helper()
	signer, err := quictls.NewEd25519Signer()
	require.NoError(t, err)
	identity, err := quictls.NewIdentity(signer, quictls.Config{AdvertiseEthp2pALPN: advertiseEthp2p})
	require.NoError(t, err)
	return identity
}

// libp2pKeyGens generates a go-libp2p identity key per key type.
var libp2pKeyGens = map[string]func() (crypto.PrivKey, error){
	"ed25519": func() (crypto.PrivKey, error) {
		sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
		return sk, err
	},
	"rsa": func() (crypto.PrivKey, error) {
		sk, _, err := crypto.GenerateRSAKeyPair(2048, rand.Reader)
		return sk, err
	},
	"ecdsa": func() (crypto.PrivKey, error) {
		sk, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
		return sk, err
	},
	"secp256k1": func() (crypto.PrivKey, error) {
		sk, _, err := crypto.GenerateSecp256k1Key(rand.Reader)
		return sk, err
	},
}

// interopKeyTypes is the sorted set of identity key types under test.
var interopKeyTypes = slices.Sorted(maps.Keys(libp2pKeyGens))

// newLibp2pHost builds a go-libp2p host with the given identity key type
// and returns its dial address (host:port of the first QUIC address).
func newLibp2pHost(t *testing.T, keyType string) (host.Host, string) {
	t.Helper()
	gen, ok := libp2pKeyGens[keyType]
	require.True(t, ok, "unknown key type %q", keyType)
	sk, err := gen()
	require.NoError(t, err)
	h, err := libp2p.New(libp2p.Identity(sk))
	require.NoError(t, err)
	t.Cleanup(func() { h.Close() })
	return h, dialAddr(t, quicAddrOf(t, h))
}

// quicAddrOf returns h's first QUIC address, skipping WebTransport addrs
// (/quic-v1/webtransport/...), which negotiate ALPN h3, not libp2p.
func quicAddrOf(t *testing.T, h host.Host) ma.Multiaddr {
	t.Helper()
	for _, a := range h.Addrs() {
		if _, err := a.ValueForProtocol(ma.P_QUIC_V1); err != nil {
			continue
		}
		if _, err := a.ValueForProtocol(ma.P_WEBTRANSPORT); err == nil {
			continue
		}
		return a
	}
	t.Fatal("host has no QUIC address")
	return nil
}

// dialAddr converts a /ip{4,6}/udp/port/quic-v1 multiaddr to host:port.
func dialAddr(t *testing.T, addr ma.Multiaddr) string {
	t.Helper()
	ip, err := addr.ValueForProtocol(ma.P_IP4)
	if err != nil {
		ip, err = addr.ValueForProtocol(ma.P_IP6)
		require.NoError(t, err)
	}
	port, err := addr.ValueForProtocol(ma.P_UDP)
	require.NoError(t, err)
	return net.JoinHostPort(ip, port)
}

// listenQUIC starts a QUIC listener on 127.0.0.1 with the given TLS
// config and returns it with its multiaddr.
func listenQUIC(t *testing.T, conf *tls.Config) (*quic.Listener, ma.Multiaddr) {
	t.Helper()
	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { udpConn.Close() })
	ln, err := quic.Listen(udpConn, conf, &quic.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	addr := ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic-v1", udpConn.LocalAddr().(*net.UDPAddr).Port))
	return ln, addr
}

// dial opens a QUIC connection to addr, pinning id (nil accepts any),
// and returns the conn plus the peer key delivered by the handshake.
func dial(identity *quictls.Identity, id quictls.ID, addr string) (*quic.Conn, quictls.Key, error) {
	conf, keyCh := identity.ClientConfig(id)
	conn, err := quic.DialAddr(context.Background(), addr, conf, &quic.Config{})
	if err != nil {
		return nil, nil, err
	}
	return conn, <-keyCh, nil
}

// dialAndVerify is dial plus an assertion that the peer key matches id.
func dialAndVerify(t *testing.T, identity *quictls.Identity, id quictls.ID, addr string) *quic.Conn {
	t.Helper()
	conn, key, err := dial(identity, id, addr)
	require.NoError(t, err)
	require.Equal(t, id, quictls.IDFromKey(key))
	return conn
}

// dialPeer makes h dial id at addr, returning the dial error.
func dialPeer(t *testing.T, h host.Host, id peer.ID, addr ma.Multiaddr) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h.Peerstore().AddAddrs(id, []ma.Multiaddr{addr}, peerstore.TempAddrTTL)
	_, err := h.Network().DialPeer(ctx, id)
	return err
}

// TestInteropDialLibp2pHost: our client dials a real go-libp2p QUIC
// listener and verifies the host's identity from its certificate.
func TestInteropDialLibp2pHost(t *testing.T) {
	for _, keyType := range interopKeyTypes {
		for _, advertise := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/advertiseEthp2p=%t", keyType, advertise), func(t *testing.T) {
				h, addr := newLibp2pHost(t, keyType)
				identity := newTestIdentity(t, advertise)

				conn := dialAndVerify(t, identity, quictls.ID(h.ID()), addr)
				defer conn.CloseWithError(0, "")
				// go-libp2p does not know "ethp2p_0", so the fallback ALPN must win.
				require.Equal(t, "libp2p", conn.ConnectionState().TLS.NegotiatedProtocol)
			})
		}
	}
}

// TestInteropAcceptLibp2pDial: a go-libp2p host dials our QUIC listener,
// pinning our identity; we derive the host's identity from its certificate.
func TestInteropAcceptLibp2pDial(t *testing.T) {
	for _, keyType := range interopKeyTypes {
		for _, advertise := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/advertiseEthp2p=%t", keyType, advertise), func(t *testing.T) {
				h, _ := newLibp2pHost(t, keyType)
				identity := newTestIdentity(t, advertise)

				ln, addr := listenQUIC(t, identity.ServerConfig())
				// DialPeer (not Connect): Connect would wait for the identify protocol,
				// which a bare QUIC listener doesn't speak.
				require.NoError(t, dialPeer(t, h, peer.ID(identity.ID()), addr))

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				conn, err := ln.Accept(ctx)
				require.NoError(t, err)
				defer conn.CloseWithError(0, "")
				key, id, err := quictls.VerifyPeerCert(conn.ConnectionState().TLS.PeerCertificates)
				require.NoError(t, err)
				require.Equal(t, quictls.ID(h.ID()), id)
				require.Equal(t, quictls.ID(h.ID()), quictls.IDFromKey(key))
				// go-libp2p does not know "ethp2p_0", so the fallback ALPN must win.
				require.Equal(t, "libp2p", conn.ConnectionState().TLS.NegotiatedProtocol)
			})
		}
	}
}

// TestInteropWrongPeerID: pinning the wrong identity fails the handshake.
func TestInteropWrongPeerID(t *testing.T) {
	_, addr := newLibp2pHost(t, "ed25519")
	identity := newTestIdentity(t, false)

	_, _, err := dial(identity, quictls.ID("definitely-not-the-peer"), addr)
	require.Error(t, err)
}

// TestInteropWrongPeerIDReverse: a go-libp2p client dials our listener
// pinning a wrong peer ID; our certificate doesn't match, so the dial
// fails on their side.
func TestInteropWrongPeerIDReverse(t *testing.T) {
	h, _ := newLibp2pHost(t, "ed25519")
	identity := newTestIdentity(t, false)

	_, addr := listenQUIC(t, identity.ServerConfig())
	require.Error(t, dialPeer(t, h, peer.ID("definitely-not-our-identity"), addr))
}

// TestInteropSamePeerIDFreshCert: two hosts built from the same key have
// the same peer ID but fresh certificates; we pin by ID, so both dials
// succeed and the certs differ.
func TestInteropSamePeerIDFreshCert(t *testing.T) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	newHost := func() host.Host {
		h, err := libp2p.New(libp2p.Identity(sk))
		require.NoError(t, err)
		t.Cleanup(func() { h.Close() })
		return h
	}
	h1, h2 := newHost(), newHost()
	require.Equal(t, h1.ID(), h2.ID())

	identity := newTestIdentity(t, false)
	var certs [][]byte
	for _, h := range []host.Host{h1, h2} {
		conn := dialAndVerify(t, identity, quictls.ID(h.ID()), dialAddr(t, quicAddrOf(t, h)))
		certs = append(certs, conn.ConnectionState().TLS.PeerCertificates[0].Raw)
		conn.CloseWithError(0, "")
	}
	require.NotEqual(t, certs[0], certs[1])
}

// TestInteropClientConfigAcceptAny: ClientConfig with a nil expectation
// accepts any identity.
func TestInteropClientConfigAcceptAny(t *testing.T) {
	h, addr := newLibp2pHost(t, "ed25519")
	identity := newTestIdentity(t, false)

	conn, key, err := dial(identity, nil, addr)
	require.NoError(t, err)
	defer conn.CloseWithError(0, "")
	require.Equal(t, quictls.ID(h.ID()), quictls.IDFromKey(key))
}

// TestInteropNoCommonALPNOutbound: our client dials a QUIC server that
// only offers "h3"; no common ALPN, so the handshake fails cleanly.
func TestInteropNoCommonALPNOutbound(t *testing.T) {
	server := newTestIdentity(t, true)
	conf := server.ServerConfig()
	conf.NextProtos = []string{"h3"}
	_, addr := listenQUIC(t, conf)

	client := newTestIdentity(t, true)
	_, _, err := dial(client, nil, dialAddr(t, addr))
	require.Error(t, err)
}

// TestInteropNoCommonALPNInbound: a QUIC client offering only "h3" dials
// our listener; no common ALPN, so the handshake fails on the client side.
func TestInteropNoCommonALPNInbound(t *testing.T) {
	identity := newTestIdentity(t, true)
	_, addr := listenQUIC(t, identity.ServerConfig())

	cconf := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true,
		NextProtos:         []string{"h3"},
	}
	_, err := quic.DialAddr(context.Background(), dialAddr(t, addr), cconf, &quic.Config{})
	require.Error(t, err)
}

// TestInteropIPv6: dial a go-libp2p host listening on IPv6 loopback.
func TestInteropIPv6(t *testing.T) {
	probe, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %s", err)
	}
	probe.Close()

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip6/::1/udp/0/quic-v1"))
	require.NoError(t, err)
	t.Cleanup(func() { h.Close() })

	identity := newTestIdentity(t, false)
	conn := dialAndVerify(t, identity, quictls.ID(h.ID()), dialAddr(t, quicAddrOf(t, h)))
	defer conn.CloseWithError(0, "")
}

// TestInteropParallelDials: many concurrent dials to one host; each
// ClientConfig is single-use, so this exercises per-dial state.
func TestInteropParallelDials(t *testing.T) {
	h, addr := newLibp2pHost(t, "ed25519")
	identity := newTestIdentity(t, false)

	const n = 8
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			conn, key, err := dial(identity, quictls.ID(h.ID()), addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.CloseWithError(0, "")
			if !bytes.Equal(quictls.ID(h.ID()), quictls.IDFromKey(key)) {
				errs <- errors.New("peer ID mismatch")
				return
			}
			errs <- nil
		}()
	}
	for i := 0; i < n; i++ {
		require.NoError(t, <-errs)
	}
}

// TestInteropDialBlackhole: dialing a non-routable address fails cleanly
// within the context timeout, without hanging.
func TestInteropDialBlackhole(t *testing.T) {
	identity := newTestIdentity(t, false)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// 192.0.2.1 is TEST-NET-1 (RFC 5737): guaranteed non-routable.
	conf, _ := identity.ClientConfig(nil)
	_, err := quic.DialAddr(ctx, "192.0.2.1:1", conf, &quic.Config{})
	require.Error(t, err)
}
