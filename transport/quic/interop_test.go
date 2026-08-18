package quic

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	quictls "github.com/ethp2p/ethp2p/transport/quic/tls"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

func newTestIdentity(t *testing.T) *quictls.Identity {
	t.Helper()
	signer, err := quictls.NewEd25519Signer()
	require.NoError(t, err)
	identity, err := quictls.NewIdentity(signer)
	require.NoError(t, err)
	return identity
}

func newLibp2pHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New()
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
	h := newLibp2pHost(t)
	identity := newTestIdentity(t)

	conf, keyCh := identity.ClientConfig(quictls.ID(h.ID()))
	conn, err := quic.DialAddr(context.Background(), dialAddr(t, quicAddrOf(t, h)), conf, &quic.Config{})
	require.NoError(t, err)
	defer conn.CloseWithError(0, "")

	key := <-keyCh
	require.Equal(t, quictls.ID(h.ID()), quictls.IDFromKey(key))
}

// TestInteropAcceptLibp2pDial: a go-libp2p host dials our QUIC listener,
// pinning our identity; we derive the host's identity from its certificate.
func TestInteropAcceptLibp2pDial(t *testing.T) {
	h := newLibp2pHost(t)
	identity := newTestIdentity(t)

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
}

// TestInteropWrongPeerID: pinning the wrong identity fails the handshake.
func TestInteropWrongPeerID(t *testing.T) {
	h := newLibp2pHost(t)
	identity := newTestIdentity(t)

	conf, _ := identity.ClientConfig(quictls.ID("definitely-not-the-peer"))
	_, err := quic.DialAddr(context.Background(), dialAddr(t, quicAddrOf(t, h)), conf, &quic.Config{})
	require.Error(t, err)
}
