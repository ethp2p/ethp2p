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

	clientEthConn, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.PeerID())
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
	assertPeer(t, clientEthConn, serverEth.PeerID())
	assertPeer(t, serverEthConn, clientEth.PeerID())
	if !clientEthConn.SupportsDatagrams() || !serverEthConn.SupportsDatagrams() {
		t.Fatal("shared QUIC configuration did not negotiate datagrams")
	}
}

func TestEthp2pAcceptStartsSharedListener(t *testing.T) {
	ctx := testContext(t)
	_, _, clientEth, _ := newEndpoint(t)
	_, serverLib, serverEth, serverPC := newEndpoint(t)

	type dialResult struct {
		conn Conn
		err  error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		conn, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.PeerID())
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
	assertPeer(t, result.conn, serverEth.PeerID())
	assertPeer(t, serverConn, clientEth.PeerID())
}

func TestSecondListenFails(t *testing.T) {
	_, serverLib, serverEth, _ := newEndpoint(t)
	listen(t, serverLib, serverEth)
	if _, err := serverLib.Listen(nil, nil); !errors.Is(err, errAlreadyListening) {
		t.Fatalf("second Listen error = %v, want %v", err, errAlreadyListening)
	}
}

func TestListenerCloseIsRejected(t *testing.T) {
	ctx := testContext(t)
	_, _, clientEth, _ := newEndpoint(t)
	_, serverLib, serverEth, serverPC := newEndpoint(t)
	listener := listen(t, serverLib, serverEth)

	// Listener.Close is rejected: listening stops only with the whole
	// transport, and delivery keeps working after the rejection.
	if err := listener.Close(); err == nil {
		t.Fatal("listener.Close succeeded, want rejection")
	}
	if _, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.PeerID()); err != nil {
		t.Fatal(err)
	}
	if _, err := listener.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	serverEthConn, err := serverEth.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertPeer(t, serverEthConn, clientEth.PeerID())
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
	if err := <-accepted; !errors.Is(err, errClosed) {
		t.Fatalf("eth Accept after Close = %v, want %v", err, errClosed)
	}
	if _, err := listener.Accept(ctx); !errors.Is(err, errClosed) {
		t.Fatalf("lib Accept after Close = %v, want %v", err, errClosed)
	}
	if _, err := serverLib.Listen(nil, nil); !errors.Is(err, errClosed) {
		t.Fatalf("Listen after Close = %v, want %v", err, errClosed)
	}
	if err := serverSt.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
}
