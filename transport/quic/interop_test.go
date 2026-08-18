package quic

import (
	"context"
	"crypto/rand"
	"crypto/tls"
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

// newLibp2pHost builds a go-libp2p host with the given identity key type.
func newLibp2pHost(t *testing.T, keyType string) host.Host {
	t.Helper()
	gen, ok := libp2pKeyGens[keyType]
	require.True(t, ok, "unknown key type %q", keyType)
	sk, err := gen()
	require.NoError(t, err)
	h, err := libp2p.New(libp2p.Identity(sk))
	require.NoError(t, err)
	t.Cleanup(func() { h.Close() })
	return h
}

func quicAddrOf(t *testing.T, h host.Host) ma.Multiaddr {
	t.Helper()
	for _, a := range h.Addrs() {
		if _, err := a.ValueForProtocol(ma.P_QUIC_V1); err != nil {
			continue
		}
		// skip WebTransport addrs (/quic-v1/webtransport/...): they
		// negotiate ALPN h3, not libp2p
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

// TestInteropDialLibp2pHost: our client dials a real go-libp2p QUIC
// listener and verifies the host's identity from its certificate.
func TestInteropDialLibp2pHost(t *testing.T) {
	for _, keyType := range slices.Sorted(maps.Keys(libp2pKeyGens)) {
		for _, advertise := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/advertiseEthp2p=%t", keyType, advertise), func(t *testing.T) {
				h := newLibp2pHost(t, keyType)
				identity := newTestIdentity(t, advertise)

				conf, keyCh := identity.ClientConfig(quictls.ID(h.ID()))
				conn, err := quic.DialAddr(context.Background(), dialAddr(t, quicAddrOf(t, h)), conf, &quic.Config{})
				require.NoError(t, err)
				defer conn.CloseWithError(0, "")

				key := <-keyCh
				require.Equal(t, quictls.ID(h.ID()), quictls.IDFromKey(key))
				// go-libp2p does not know "ethp2p_0", so the fallback ALPN must win.
				require.Equal(t, "libp2p", conn.ConnectionState().TLS.NegotiatedProtocol)
			})
		}
	}
}

// TestInteropAcceptLibp2pDial: a go-libp2p host dials our QUIC listener,
// pinning our identity; we derive the host's identity from its certificate.
func TestInteropAcceptLibp2pDial(t *testing.T) {
	for _, keyType := range slices.Sorted(maps.Keys(libp2pKeyGens)) {
		for _, advertise := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/advertiseEthp2p=%t", keyType, advertise), func(t *testing.T) {
				h := newLibp2pHost(t, keyType)
				identity := newTestIdentity(t, advertise)

				udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				require.NoError(t, err)
				defer udpConn.Close()
				ln, err := quic.Listen(udpConn, identity.ServerConfig(), &quic.Config{})
				require.NoError(t, err)
				defer ln.Close()

				addr := ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic-v1", udpConn.LocalAddr().(*net.UDPAddr).Port))
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				// DialPeer (not Connect): Connect would wait for the identify protocol,
				// which a bare QUIC listener doesn't speak.
				h.Peerstore().AddAddrs(peer.ID(identity.ID()), []ma.Multiaddr{addr}, peerstore.TempAddrTTL)
				_, err = h.Network().DialPeer(ctx, peer.ID(identity.ID()))
				require.NoError(t, err)

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
	h := newLibp2pHost(t, "ed25519")
	identity := newTestIdentity(t, false)

	conf, _ := identity.ClientConfig(quictls.ID("definitely-not-the-peer"))
	_, err := quic.DialAddr(context.Background(), dialAddr(t, quicAddrOf(t, h)), conf, &quic.Config{})
	require.Error(t, err)
}

// TestInteropWrongPeerIDReverse: a go-libp2p client dials our listener
// pinning a wrong peer ID; our certificate doesn't match, so the dial
// fails on their side.
func TestInteropWrongPeerIDReverse(t *testing.T) {
	h := newLibp2pHost(t, "ed25519")
	identity := newTestIdentity(t, false)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer udpConn.Close()
	ln, err := quic.Listen(udpConn, identity.ServerConfig(), &quic.Config{})
	require.NoError(t, err)
	defer ln.Close()

	addr := ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic-v1", udpConn.LocalAddr().(*net.UDPAddr).Port))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wrong := peer.ID("definitely-not-our-identity")
	h.Peerstore().AddAddrs(wrong, []ma.Multiaddr{addr}, peerstore.TempAddrTTL)
	_, err = h.Network().DialPeer(ctx, wrong)
	require.Error(t, err)
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
		conf, keyCh := identity.ClientConfig(quictls.ID(h.ID()))
		conn, err := quic.DialAddr(context.Background(), dialAddr(t, quicAddrOf(t, h)), conf, &quic.Config{})
		require.NoError(t, err)
		key := <-keyCh
		require.Equal(t, quictls.ID(h.ID()), quictls.IDFromKey(key))
		certs = append(certs, conn.ConnectionState().TLS.PeerCertificates[0].Raw)
		conn.CloseWithError(0, "")
	}
	require.NotEqual(t, certs[0], certs[1])
}

// TestInteropClientConfigAcceptAny: ClientConfig with a nil expectation
// accepts any identity.
func TestInteropClientConfigAcceptAny(t *testing.T) {
	h := newLibp2pHost(t, "ed25519")
	identity := newTestIdentity(t, false)

	conf, keyCh := identity.ClientConfig(nil)
	conn, err := quic.DialAddr(context.Background(), dialAddr(t, quicAddrOf(t, h)), conf, &quic.Config{})
	require.NoError(t, err)
	defer conn.CloseWithError(0, "")

	key := <-keyCh
	require.Equal(t, quictls.ID(h.ID()), quictls.IDFromKey(key))
}

// TestInteropNoCommonALPNOutbound: our client dials a QUIC server that
// only offers "h3"; no common ALPN, so the handshake fails cleanly.
func TestInteropNoCommonALPNOutbound(t *testing.T) {
	server := newTestIdentity(t, true)
	conf := server.ServerConfig()
	conf.NextProtos = []string{"h3"}

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer udpConn.Close()
	ln, err := quic.Listen(udpConn, conf, &quic.Config{})
	require.NoError(t, err)
	defer ln.Close()

	client := newTestIdentity(t, true)
	cconf, _ := client.ClientConfig(nil)
	addr := fmt.Sprintf("127.0.0.1:%d", udpConn.LocalAddr().(*net.UDPAddr).Port)
	_, err = quic.DialAddr(context.Background(), addr, cconf, &quic.Config{})
	require.Error(t, err)
}

// TestInteropNoCommonALPNInbound: a QUIC client offering only "h3" dials
// our listener; no common ALPN, so the handshake fails on the client side.
func TestInteropNoCommonALPNInbound(t *testing.T) {
	identity := newTestIdentity(t, true)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer udpConn.Close()
	ln, err := quic.Listen(udpConn, identity.ServerConfig(), &quic.Config{})
	require.NoError(t, err)
	defer ln.Close()

	cconf := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true,
		NextProtos:         []string{"h3"},
	}
	addr := fmt.Sprintf("127.0.0.1:%d", udpConn.LocalAddr().(*net.UDPAddr).Port)
	_, err = quic.DialAddr(context.Background(), addr, cconf, &quic.Config{})
	require.Error(t, err)
}
