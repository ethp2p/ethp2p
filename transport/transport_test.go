package transport

import (
	"crypto/x509"
	"errors"
	"testing"

	"github.com/quic-go/quic-go"
)

func TestAuthenticatePeerReturnsPublicKey(t *testing.T) {
	key := testKey(t)
	handshake, err := newHandshaker(key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(handshake.config.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := authenticate([]*x509.Certificate{certificate})
	if err != nil {
		t.Fatal(err)
	}
	if !publicKey.Equal(key.Public()) {
		t.Fatal("authenticated public key differs from certificate identity")
	}
}

func TestEthp2pDialFallsBackToLegacyLibp2p(t *testing.T) {
	ctx := testContext(t)
	_, clientLib, clientEth, _ := newEndpoint(t)
	clientListener := listen(t, clientLib, clientEth)
	stockKey := testKey(t)
	stockHandshake, err := newHandshaker(stockKey)
	if err != nil {
		t.Fatal(err)
	}
	stockTLS := stockHandshake.config.Clone()
	stockTLS.NextProtos = []string{AlpnLibp2p}
	stockPC := testPacketConn(t)
	stock := &quic.Transport{Conn: stockPC}
	t.Cleanup(func() { _ = stock.Close() })
	stockListener, err := stock.Listen(stockTLS, &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stockListener.Close() })

	ethp2p, err := clientEth.Dial(ctx, stockPC.LocalAddr(), stockHandshake.peerID)
	if !errors.Is(err, ErrDialLegacyPeer) {
		t.Fatalf("dial error = %v, want %v", err, ErrDialLegacyPeer)
	}
	if ethp2p != nil {
		t.Fatal("ethp2p connection returned after libp2p fallback")
	}
	libp2p, err := clientListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertRawALPN(t, libp2p, AlpnLibp2p)
	stockConn, err := stockListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stockConn.CloseWithError(appNoError, "test done") })
}
