package transport

import (
	"errors"
	"testing"
)

func TestEthp2pDialPublishesBothViews(t *testing.T) {
	ctx := testContext(t)
	_, clientLib, clientEth, _ := newEndpoint(t)
	_, serverLib, serverEth, serverPC := newEndpoint(t)
	listener := listen(t, serverLib, serverEth)

	// The client attaches its lib side so the libp2p view published by the
	// dial is observable alongside the returned ethp2p view.
	clientListener := listen(t, clientLib, clientEth)

	clientEthConn, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	serverLibConn, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clientLibConn, err := clientListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	serverEthConn, err := serverEth.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}

	assertSharedALPN(t, clientLibConn)
	assertSharedALPN(t, serverLibConn)
	assertPeer(t, clientEthConn, serverEth.shared.PeerID())
	assertPeer(t, serverEthConn, clientEth.shared.PeerID())
	if !clientEthConn.SupportsDatagrams() || !serverEthConn.SupportsDatagrams() {
		t.Fatal("shared QUIC configuration did not negotiate datagrams")
	}
}

func TestEthp2pAcceptStartsSharedListener(t *testing.T) {
	ctx := testContext(t)
	_, _, clientEth, _ := newEndpoint(t)
	_, serverLib, serverEth, serverPC := newEndpoint(t)

	type dialResult struct {
		conn *Conn
		err  error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		conn, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.shared.PeerID())
		dialed <- dialResult{conn: conn, err: err}
	}()

	serverConn, err := serverEth.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result := <-dialed
	if result.err != nil {
		t.Fatal(result.err)
	}
	serverListener := listen(t, serverLib, serverEth)
	serverLibConn, err := serverListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertSharedALPN(t, serverLibConn)
	assertPeer(t, result.conn, serverEth.shared.PeerID())
	assertPeer(t, serverConn, clientEth.shared.PeerID())
}

func TestSecondListenFails(t *testing.T) {
	_, serverLib, serverEth, _ := newEndpoint(t)
	listen(t, serverLib, serverEth)
	if _, err := serverLib.Listen(nil, nil); !errors.Is(err, errAlreadyListening) {
		t.Fatalf("second Listen error = %v, want %v", err, errAlreadyListening)
	}
}

func TestListenerCloseDetachesLibp2p(t *testing.T) {
	ctx := testContext(t)
	_, clientEth, _ := newEthp2pEndpoint(t)
	_, serverLib, serverEth, serverPC := newEndpoint(t)
	listener := listen(t, serverLib, serverEth)

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := listener.Accept(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Accept after Close = %v, want %v", err, ErrClosed)
	}
	if _, err := serverLib.Listen(nil, nil); !errors.Is(err, errAlreadyListening) {
		t.Fatalf("Listen after detach = %v, want %v", err, errAlreadyListening)
	}
	clientConn, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	serverEthConn, err := serverEth.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertPeer(t, serverEthConn, clientEth.shared.PeerID())
	raw := clientConn.conn
	if err := clientConn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := serverEthConn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-raw.Context().Done():
	case <-ctx.Done():
		t.Fatal("physical connection remained open after both ethp2p views closed")
	}
}

func TestShutdownEndsAccept(t *testing.T) {
	ctx := testContext(t)
	serverSt, serverLib, serverEth, _ := newEndpoint(t)
	listener := listen(t, serverLib, serverEth)

	// Attach the eth side so both queues exist.
	accepted := make(chan error, 1)
	go func() {
		_, err := serverEth.Accept(ctx)
		accepted <- err
	}()
	if err := serverSt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-accepted; !errors.Is(err, ErrClosed) {
		t.Fatalf("eth Accept after Close = %v, want %v", err, ErrClosed)
	}
	if _, err := listener.Accept(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("lib Accept after Close = %v, want %v", err, ErrClosed)
	}
	if _, err := serverLib.Listen(nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Listen after Close = %v, want %v", err, ErrClosed)
	}
	if err := serverSt.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
}
