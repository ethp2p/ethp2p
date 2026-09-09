package ethp2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

const (
	selectorAlpha  protocol.Selector = 3
	selectorCommon protocol.Selector = 7
	selectorGamma  protocol.Selector = 11
	testTimeout                      = 10 * time.Second
)

type testEndpoint struct {
	shared *transport.TransportShared
	eth    *transport.TransportEth
	packet *net.UDPConn
}

type testPair struct {
	client     *testEndpoint
	server     *testEndpoint
	clientConn transport.Conn
	serverConn transport.Conn
	clientLib  quicreuse.QUICConn
	serverLib  quicreuse.QUICConn
}

func newTestEndpoint(t *testing.T) *testEndpoint {
	t.Helper()

	key, err := transport.GenPrivKey()
	if err != nil {
		t.Fatal(err)
	}
	packet, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	shared, err := transport.NewShared(key, packet)
	if err != nil {
		_ = packet.Close()
		t.Fatal(err)
	}
	endpoint := &testEndpoint{shared: shared, eth: shared.Ethp2p(), packet: packet}
	t.Cleanup(func() {
		// TransportShared deliberately does not own the packet connection.
		_ = endpoint.shared.Close()
		_ = endpoint.packet.Close()
	})
	return endpoint
}

func newTestPair(t *testing.T) *testPair {
	t.Helper()
	client := newTestEndpoint(t)
	server := newTestEndpoint(t)

	clientListener, err := client.shared.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	serverListener, err := server.shared.Libp2p().Listen(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	type dialResult struct {
		conn transport.Conn
		err  error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		conn, err := client.eth.Dial(ctx, server.eth.Addr(), server.eth.PeerID())
		dialed <- dialResult{conn: conn, err: err}
	}()

	serverConn, err := server.eth.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dial := <-dialed
	if dial.err != nil {
		t.Fatal(dial.err)
	}
	clientLib, err := clientListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	serverLib, err := serverListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return &testPair{
		client:     client,
		server:     server,
		clientConn: dial.conn,
		serverConn: serverConn,
		clientLib:  clientLib,
		serverLib:  serverLib,
	}
}

func startServe(t *testing.T, stack *Stack, conn transport.Conn) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- stack.ServeConn(ctx, conn) }()
	return cancel, done
}

func waitServe(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(testTimeout):
		t.Fatal("ServeConn did not return")
		return context.DeadlineExceeded
	}
}

func awaitPeer(t *testing.T, peers <-chan *Peer) *Peer {
	t.Helper()
	select {
	case peer := <-peers:
		if peer == nil {
			t.Fatal("received nil peer")
		}
		return peer
	case <-time.After(testTimeout):
		t.Fatal("peer notification did not arrive")
		return nil
	}
}

func selectorWire(selectors ...protocol.Selector) []byte {
	wire := make([]byte, 0, len(selectors)*binary.MaxVarintLen64)
	for _, selector := range selectors {
		wire = binary.AppendUvarint(wire, uint64(selector))
	}
	return wire
}

func writeRawUni(t *testing.T, conn transport.Conn, wire []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	stream, err := conn.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(wire); err != nil {
		t.Fatal(err)
	}
	_ = stream.Close()
}

func openRawUni(t *testing.T, conn transport.Conn, wire []byte) transport.SendStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	stream, err := conn.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(wire); err != nil {
		t.Fatal(err)
	}
	return stream
}

func writeSelectorPayload(t *testing.T, conn transport.Conn, selector protocol.Selector, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	stream, err := conn.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteSelector(stream, selector); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func stopServe(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	if err := waitServe(t, done); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("ServeConn after cancellation = %v", err)
	}
}

func TestStackSetupFreezesOnFirstServe(t *testing.T) {
	pair := newTestPair(t)
	var stack Stack
	subsystem, err := stack.RegisterSubsystem("before-serve", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	peers := make(chan *Peer, 1)
	streams := make(chan StreamEvent, 1)
	if err := subsystem.NotifyPeers(peers); err != nil {
		t.Fatal(err)
	}
	if err := subsystem.NotifyStreams(streams); err != nil {
		t.Fatal(err)
	}
	if err := subsystem.SetPolicy(func(*Peer) bool { return true }); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := stack.ServeConn(ctx, pair.serverConn); err == nil {
		t.Fatal("ServeConn with canceled context returned nil")
	}
	if _, err := stack.RegisterSubsystem("after-serve", selectorGamma); err == nil {
		t.Fatal("RegisterSubsystem after ServeConn succeeded")
	}
	if err := subsystem.NotifyPeers(make(chan *Peer)); err == nil {
		t.Fatal("NotifyPeers after ServeConn succeeded")
	}
	if err := subsystem.NotifyStreams(make(chan StreamEvent)); err == nil {
		t.Fatal("NotifyStreams after ServeConn succeeded")
	}
	if err := subsystem.SetPolicy(nil); err == nil {
		t.Fatal("SetPolicy after ServeConn succeeded")
	}
}

func TestStackRegisterSubsystemRejectsInvalidRegistrationsAtomically(t *testing.T) {
	var stack Stack
	if _, err := stack.RegisterSubsystem("duplicate", selectorAlpha, selectorAlpha); err == nil {
		t.Fatal("duplicate selectors in one subsystem were accepted")
	}
	if _, err := stack.RegisterSubsystem("alpha", selectorAlpha); err != nil {
		t.Fatal(err)
	}
	if _, err := stack.RegisterSubsystem("alpha", selectorGamma); err == nil {
		t.Fatal("duplicate subsystem name was accepted")
	}
	if _, err := stack.RegisterSubsystem("gamma", selectorGamma); err != nil {
		t.Fatalf("failed to register selector after rejected duplicate name: %v", err)
	}
	if _, err := stack.RegisterSubsystem("collision", selectorAlpha); err == nil {
		t.Fatal("selector assigned to another subsystem was accepted")
	}
}

func TestStackServeConnReportsNoProtocols(t *testing.T) {
	pair := newTestPair(t)
	var client, server Stack
	clientCancel, clientDone := startServe(t, &client, pair.clientConn)
	serverCancel, serverDone := startServe(t, &server, pair.serverConn)

	serverErr := waitServe(t, serverDone)
	clientErr := waitServe(t, clientDone)
	clientCancel()
	serverCancel()
	if !errors.Is(serverErr, ErrNoProtocols) {
		t.Fatalf("server ServeConn = %v, want ErrNoProtocols", serverErr)
	}
	if !errors.Is(clientErr, ErrNoProtocols) {
		t.Fatalf("client ServeConn = %v, want ErrNoProtocols", clientErr)
	}
}

func TestStackAdvertisesCanonicalSelectorsAndNotifiesAfterExchange(t *testing.T) {
	pair := newTestPair(t)
	var stack Stack
	subsystem, err := stack.RegisterSubsystem("canonical", selectorGamma, selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	peers := make(chan *Peer, 1)
	if err := subsystem.NotifyPeers(peers); err != nil {
		t.Fatal(err)
	}

	serveCancel, serveDone := startServe(t, &stack, pair.serverConn)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	remoteAdvertisement := openRawUni(t, pair.clientConn, selectorWire(selectorAlpha, selectorGamma))
	advertisement, err := pair.clientConn.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(advertisement)
	if err != nil {
		t.Fatal(err)
	}
	if want := selectorWire(selectorAlpha, selectorGamma); !bytes.Equal(got, want) {
		t.Fatalf("first selector advertisement = %x, want %x", got, want)
	}
	select {
	case peer := <-peers:
		t.Fatalf("peer notification arrived before advertisement FIN: %v", peer)
	default:
	}
	if err := remoteAdvertisement.Close(); err != nil {
		t.Fatal(err)
	}
	peer := awaitPeer(t, peers)
	if !slices.Equal(peer.Selectors, []protocol.Selector{selectorAlpha, selectorGamma}) {
		t.Fatalf("peer selectors = %v, want [%d %d]", peer.Selectors, selectorAlpha, selectorGamma)
	}
	if peer.Conn != pair.serverConn {
		t.Fatal("peer notification did not retain the borrowed connection")
	}
	if peer.Context == nil {
		t.Fatal("peer notification has nil context")
	}
	stopServe(t, serveCancel, serveDone)
}

func TestStackExchangesAndIntersectsOnBothEnds(t *testing.T) {
	pair := newTestPair(t)
	var client, server Stack
	clientSubsystem, err := client.RegisterSubsystem("client", selectorGamma, selectorCommon)
	if err != nil {
		t.Fatal(err)
	}
	serverSubsystem, err := server.RegisterSubsystem("server", selectorAlpha, selectorCommon)
	if err != nil {
		t.Fatal(err)
	}
	clientPeers := make(chan *Peer, 1)
	serverPeers := make(chan *Peer, 1)
	if err := clientSubsystem.NotifyPeers(clientPeers); err != nil {
		t.Fatal(err)
	}
	if err := serverSubsystem.NotifyPeers(serverPeers); err != nil {
		t.Fatal(err)
	}

	clientCancel, clientDone := startServe(t, &client, pair.clientConn)
	serverCancel, serverDone := startServe(t, &server, pair.serverConn)
	clientPeer := awaitPeer(t, clientPeers)
	serverPeer := awaitPeer(t, serverPeers)
	for name, peer := range map[string]*Peer{"client": clientPeer, "server": serverPeer} {
		if !slices.Equal(peer.Selectors, []protocol.Selector{selectorCommon}) {
			t.Errorf("%s peer selectors = %v, want [%d]", name, peer.Selectors, selectorCommon)
		}
		if peer.Context == nil {
			t.Errorf("%s peer context is nil", name)
		}
	}
	if clientPeer.Conn != pair.clientConn || serverPeer.Conn != pair.serverConn {
		t.Fatal("peer notification did not retain each endpoint's borrowed connection")
	}
	stopServe(t, clientCancel, clientDone)
	stopServe(t, serverCancel, serverDone)
}

func TestStackNotifiesOnlySubsystemsWithSharedSelectors(t *testing.T) {
	pair := newTestPair(t)
	var client, server Stack
	clientSubsystem, err := client.RegisterSubsystem("client", selectorCommon)
	if err != nil {
		t.Fatal(err)
	}
	matchingSubsystem, err := server.RegisterSubsystem("matching", selectorCommon)
	if err != nil {
		t.Fatal(err)
	}
	otherSubsystem, err := server.RegisterSubsystem("other", selectorGamma)
	if err != nil {
		t.Fatal(err)
	}
	clientPeers := make(chan *Peer, 1)
	matchingPeers := make(chan *Peer, 1)
	otherPeers := make(chan *Peer, 1)
	if err := clientSubsystem.NotifyPeers(clientPeers); err != nil {
		t.Fatal(err)
	}
	if err := matchingSubsystem.NotifyPeers(matchingPeers); err != nil {
		t.Fatal(err)
	}
	if err := otherSubsystem.NotifyPeers(otherPeers); err != nil {
		t.Fatal(err)
	}

	clientCancel, clientDone := startServe(t, &client, pair.clientConn)
	serverCancel, serverDone := startServe(t, &server, pair.serverConn)
	clientPeer := awaitPeer(t, clientPeers)
	matchingPeer := awaitPeer(t, matchingPeers)
	if !slices.Equal(clientPeer.Selectors, []protocol.Selector{selectorCommon}) {
		t.Fatalf("client peer selectors = %v, want [%d]", clientPeer.Selectors, selectorCommon)
	}
	if !slices.Equal(matchingPeer.Selectors, []protocol.Selector{selectorCommon}) {
		t.Fatalf("matching peer selectors = %v, want [%d]", matchingPeer.Selectors, selectorCommon)
	}
	select {
	case peer := <-otherPeers:
		t.Fatalf("subsystem without a shared selector was notified: %v", peer)
	default:
	}
	stopServe(t, clientCancel, clientDone)
	stopServe(t, serverCancel, serverDone)
}

func TestStackRoutesAgreedUniStreamAndPreservesPayload(t *testing.T) {
	pair := newTestPair(t)
	var client, server Stack
	clientSubsystem, err := client.RegisterSubsystem("client", selectorCommon, selectorGamma)
	if err != nil {
		t.Fatal(err)
	}
	serverSubsystem, err := server.RegisterSubsystem("server", selectorCommon, selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	clientPeers := make(chan *Peer, 1)
	serverPeers := make(chan *Peer, 1)
	serverStreams := make(chan StreamEvent, 1)
	if err := clientSubsystem.NotifyPeers(clientPeers); err != nil {
		t.Fatal(err)
	}
	if err := serverSubsystem.NotifyPeers(serverPeers); err != nil {
		t.Fatal(err)
	}
	if err := serverSubsystem.NotifyStreams(serverStreams); err != nil {
		t.Fatal(err)
	}

	clientCancel, clientDone := startServe(t, &client, pair.clientConn)
	serverCancel, serverDone := startServe(t, &server, pair.serverConn)
	clientPeer := awaitPeer(t, clientPeers)
	serverPeer := awaitPeer(t, serverPeers)
	if !slices.Equal(clientPeer.Selectors, []protocol.Selector{selectorCommon}) ||
		!slices.Equal(serverPeer.Selectors, []protocol.Selector{selectorCommon}) {
		t.Fatalf("selector intersection = client %v, server %v", clientPeer.Selectors, serverPeer.Selectors)
	}

	payload := []byte("payload follows the selector")
	writeSelectorPayload(t, pair.clientConn, selectorCommon, payload)
	select {
	case event := <-serverStreams:
		if event.Peer != serverPeer {
			t.Fatal("stream event did not retain the subsystem's peer pointer")
		}
		if event.Selector != selectorCommon {
			t.Fatalf("stream selector = %d, want %d", event.Selector, selectorCommon)
		}
		got, err := io.ReadAll(event.Stream)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("stream payload = %q, want %q", got, payload)
		}
	case <-time.After(testTimeout):
		t.Fatal("matching stream was not delivered")
	}

	stopServe(t, clientCancel, clientDone)
	stopServe(t, serverCancel, serverDone)
	select {
	case <-serverPeer.Context.Done():
	case <-time.After(testTimeout):
		t.Fatal("peer context did not end with ServeConn")
	}
}

func TestStackRoutesBidirectionalStreamAndPreservesResponse(t *testing.T) {
	pair := newTestPair(t)
	var stack Stack
	subsystem, err := stack.RegisterSubsystem("request-response", selectorCommon)
	if err != nil {
		t.Fatal(err)
	}
	peers := make(chan *Peer, 1)
	streams := make(chan StreamEvent, 1)
	if err := subsystem.NotifyPeers(peers); err != nil {
		t.Fatal(err)
	}
	if err := subsystem.NotifyStreams(streams); err != nil {
		t.Fatal(err)
	}
	serveCancel, serveDone := startServe(t, &stack, pair.serverConn)
	defer stopServe(t, serveCancel, serveDone)
	writeRawUni(t, pair.clientConn, selectorWire(selectorCommon))
	peer := awaitPeer(t, peers)

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	stream, err := pair.clientConn.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Reset()
	_ = stream.SetReadDeadline(time.Now().Add(testTimeout))
	_ = stream.SetWriteDeadline(time.Now().Add(testTimeout))
	if err := protocol.WriteSelector(stream, selectorCommon); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(stream, "request"); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-streams:
		if event.Peer != peer || event.Selector != selectorCommon {
			t.Fatalf("wrong stream identity: %+v", event)
		}
		bidi, ok := event.Stream.(transport.Stream)
		if !ok {
			t.Fatal("bidirectional stream lost its write interface")
		}
		defer bidi.Reset()
		_ = bidi.SetReadDeadline(time.Now().Add(testTimeout))
		_ = bidi.SetWriteDeadline(time.Now().Add(testTimeout))
		request, err := io.ReadAll(bidi)
		if err != nil || string(request) != "request" {
			t.Fatalf("request = %q, err = %v", request, err)
		}
		if _, err := io.WriteString(bidi, "response"); err != nil {
			t.Fatal(err)
		}
		if err := bidi.Close(); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("bidirectional stream was not delivered")
	}
	response, err := io.ReadAll(stream)
	if err != nil || string(response) != "response" {
		t.Fatalf("response = %q, err = %v", response, err)
	}
}

func TestStackUnmatchedUniSelectorIsReset(t *testing.T) {
	pair := newTestPair(t)
	var client, server Stack
	clientSubsystem, err := client.RegisterSubsystem("client", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	serverSubsystem, err := server.RegisterSubsystem("server", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	clientPeers := make(chan *Peer, 1)
	serverPeers := make(chan *Peer, 1)
	serverStreams := make(chan StreamEvent, 1)
	if err := clientSubsystem.NotifyPeers(clientPeers); err != nil {
		t.Fatal(err)
	}
	if err := serverSubsystem.NotifyPeers(serverPeers); err != nil {
		t.Fatal(err)
	}
	if err := serverSubsystem.NotifyStreams(serverStreams); err != nil {
		t.Fatal(err)
	}
	clientCancel, clientDone := startServe(t, &client, pair.clientConn)
	serverCancel, serverDone := startServe(t, &server, pair.serverConn)
	awaitPeer(t, clientPeers)
	awaitPeer(t, serverPeers)

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	stream, err := pair.clientConn.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteSelector(stream, selectorGamma); err != nil {
		t.Fatal(err)
	}
	if err := stream.SetWriteDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatal(err)
	}
	var writeErr error
	chunk := bytes.Repeat([]byte{0xa5}, 64<<10)
	for range 512 {
		if _, writeErr = stream.Write(chunk); writeErr != nil {
			break
		}
	}
	if writeErr == nil {
		t.Fatal("unmatched stream was not reset while writing")
	}
	streamErr, ok := errors.AsType[*quic.StreamError](writeErr)
	if !ok || !streamErr.Remote {
		t.Fatalf("unmatched stream write error = %v, want remote stream reset", writeErr)
	}
	_ = stream.Close()
	select {
	case event := <-serverStreams:
		t.Fatalf("unmatched selector delivered as %d", event.Selector)
	default:
	}

	stopServe(t, clientCancel, clientDone)
	stopServe(t, serverCancel, serverDone)
}

func TestStackMalformedAdvertisementDoesNotNotifyPeer(t *testing.T) {
	pair := newTestPair(t)
	var stack Stack
	subsystem, err := stack.RegisterSubsystem("server", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	peers := make(chan *Peer, 1)
	if err := subsystem.NotifyPeers(peers); err != nil {
		t.Fatal(err)
	}
	serveCancel, serveDone := startServe(t, &stack, pair.serverConn)

	// The second value sorts before the first, so this is not a valid complete
	// selector advertisement even though both individual values are valid.
	writeRawUni(t, pair.clientConn, selectorWire(selectorGamma, selectorAlpha))
	err = waitServe(t, serveDone)
	serveCancel()
	if err == nil {
		t.Fatal("malformed advertisement allowed ServeConn to continue")
	}
	select {
	case peer := <-peers:
		t.Fatalf("malformed advertisement notified peer %v", peer)
	default:
	}
}

func TestStackCancellationDuringAdvertisementReadReturnsPromptly(t *testing.T) {
	pair := newTestPair(t)
	var stack Stack
	subsystem, err := stack.RegisterSubsystem("server", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	peers := make(chan *Peer, 1)
	if err := subsystem.NotifyPeers(peers); err != nil {
		t.Fatal(err)
	}
	serveCancel, serveDone := startServe(t, &stack, pair.serverConn)
	partialAdvertisement := openRawUni(t, pair.clientConn, selectorWire(selectorAlpha))
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	advertisement, err := pair.clientConn.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(advertisement); err != nil {
		t.Fatal(err)
	}
	// Let the read block after the write half has completed, so an error
	// from canceling the writer cannot hide a lost read-cancellation error.
	select {
	case err := <-serveDone:
		t.Fatalf("ServeConn returned before advertisement FIN: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	serveCancel()
	if err := waitServe(t, serveDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("ServeConn during partial advertisement = %v, want cancellation", err)
	}
	_ = partialAdvertisement.Close()
	select {
	case peer := <-peers:
		t.Fatalf("partial advertisement notified peer: %v", peer)
	default:
	}
}

func TestStackPeerPolicyRejectsBeforeNotification(t *testing.T) {
	pair := newTestPair(t)
	var client, server Stack
	clientSubsystem, err := client.RegisterSubsystem("client", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	serverSubsystem, err := server.RegisterSubsystem("server", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	clientPeers := make(chan *Peer, 1)
	serverPeers := make(chan *Peer, 1)
	if err := clientSubsystem.NotifyPeers(clientPeers); err != nil {
		t.Fatal(err)
	}
	if err := serverSubsystem.NotifyPeers(serverPeers); err != nil {
		t.Fatal(err)
	}
	if err := serverSubsystem.SetPolicy(func(*Peer) bool { return false }); err != nil {
		t.Fatal(err)
	}

	clientCancel, clientDone := startServe(t, &client, pair.clientConn)
	serverCancel, serverDone := startServe(t, &server, pair.serverConn)
	serverErr := waitServe(t, serverDone)
	if !errors.Is(serverErr, ErrNoProtocols) {
		t.Fatalf("rejected server ServeConn = %v, want ErrNoProtocols", serverErr)
	}
	awaitPeer(t, clientPeers)
	select {
	case peer := <-serverPeers:
		t.Fatalf("rejected peer was notified: %v", peer)
	default:
	}
	serverCancel()
	stopServe(t, clientCancel, clientDone)
}

func TestStackPeerNotificationBackpressureCancelsWithoutClosingChannel(t *testing.T) {
	pair := newTestPair(t)
	var stack Stack
	subsystem, err := stack.RegisterSubsystem("server", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	peers := make(chan *Peer, 1)
	peers <- nil // Keep the application-owned notification queue full.
	if err := subsystem.NotifyPeers(peers); err != nil {
		t.Fatal(err)
	}
	serveCancel, serveDone := startServe(t, &stack, pair.serverConn)
	writeRawUni(t, pair.clientConn, selectorWire(selectorAlpha))
	select {
	case err := <-serveDone:
		t.Fatalf("ServeConn returned before cancellation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	serveCancel()
	if err := waitServe(t, serveDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("ServeConn after full peer queue = %v, want cancellation", err)
	}
	if peer := <-peers; peer != nil {
		t.Fatalf("preloaded peer queue entry changed to %v", peer)
	}
	close(peers)
}

func TestStackStreamNotificationBackpressureCancelsAndResets(t *testing.T) {
	pair := newTestPair(t)
	var client, server Stack
	clientSubsystem, err := client.RegisterSubsystem("client", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	serverSubsystem, err := server.RegisterSubsystem("server", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	clientPeers := make(chan *Peer, 1)
	serverPeers := make(chan *Peer, 1)
	streams := make(chan StreamEvent)
	if err := clientSubsystem.NotifyPeers(clientPeers); err != nil {
		t.Fatal(err)
	}
	if err := serverSubsystem.NotifyPeers(serverPeers); err != nil {
		t.Fatal(err)
	}
	if err := serverSubsystem.NotifyStreams(streams); err != nil {
		t.Fatal(err)
	}
	clientCancel, clientDone := startServe(t, &client, pair.clientConn)
	serverCancel, serverDone := startServe(t, &server, pair.serverConn)
	awaitPeer(t, clientPeers)
	awaitPeer(t, serverPeers)

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	stream, err := pair.clientConn.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteSelector(stream, selectorAlpha); err != nil {
		t.Fatal(err)
	}
	if err := stream.SetWriteDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() {
		chunk := bytes.Repeat([]byte{0x5a}, 64<<10)
		var writeErr error
		for range 512 {
			if _, writeErr = stream.Write(chunk); writeErr != nil {
				break
			}
		}
		writeDone <- writeErr
	}()
	// Give serveUni enough time to consume the selector and block on the
	// application-owned, unbuffered stream channel.
	time.Sleep(100 * time.Millisecond)
	serverCancel()
	if err := waitServe(t, serverDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("ServeConn after full stream queue = %v, want cancellation", err)
	}
	writeErr := <-writeDone
	if writeErr == nil {
		t.Fatal("queued stream was not reset after cancellation")
	}
	streamErr, ok := errors.AsType[*quic.StreamError](writeErr)
	if !ok || !streamErr.Remote {
		t.Fatalf("queued stream write error = %v, want remote stream reset", writeErr)
	}
	_ = stream.Close()
	select {
	case event := <-streams:
		t.Fatalf("stream delivered after cancellation: %v", event)
	default:
	}
	stopServe(t, clientCancel, clientDone)
}

func TestStackCancellationPreservesBorrowedAndSharedViews(t *testing.T) {
	pair := newTestPair(t)
	var client, server Stack
	clientSubsystem, err := client.RegisterSubsystem("client", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	serverSubsystem, err := server.RegisterSubsystem("server", selectorAlpha)
	if err != nil {
		t.Fatal(err)
	}
	clientPeers := make(chan *Peer, 1)
	serverPeers := make(chan *Peer, 1)
	if err := clientSubsystem.NotifyPeers(clientPeers); err != nil {
		t.Fatal(err)
	}
	if err := serverSubsystem.NotifyPeers(serverPeers); err != nil {
		t.Fatal(err)
	}
	clientCancel, clientDone := startServe(t, &client, pair.clientConn)
	serverCancel, serverDone := startServe(t, &server, pair.serverConn)
	awaitPeer(t, clientPeers)
	awaitPeer(t, serverPeers)

	// A libp2p stream still uses its existing length-delimited classifier. Its
	// success here proves that the shared physical connection remains usable by
	// the other view while Stack borrows only the ethp2p view.
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	libOut, err := pair.clientLib.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	libWire := lengthFrame([]byte("/shared-libp2p"))
	if _, err := libOut.Write(libWire); err != nil {
		t.Fatal(err)
	}
	if err := libOut.Close(); err != nil {
		t.Fatal(err)
	}
	libIn, err := pair.serverLib.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotLib, err := io.ReadAll(libIn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotLib, libWire) {
		t.Fatalf("shared libp2p stream = %x, want %x", gotLib, libWire)
	}

	// Release both libp2p views. The physical connection now survives only if
	// ServeConn leaves its borrowed ethp2p views open.
	if err := pair.clientLib.CloseWithError(0, "test complete"); err != nil {
		t.Fatal(err)
	}
	if err := pair.serverLib.CloseWithError(0, "test complete"); err != nil {
		t.Fatal(err)
	}
	stopServe(t, clientCancel, clientDone)
	stopServe(t, serverCancel, serverDone)

	payload := []byte("borrowed connection remains open")
	stream, err := pair.serverConn.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteSelector(stream, selectorAlpha); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	received, err := pair.clientConn.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(received)
	if err != nil {
		t.Fatal(err)
	}
	if want := append(selectorWire(selectorAlpha), payload...); !bytes.Equal(got, want) {
		t.Fatalf("borrowed ethp2p stream = %x, want %x", got, want)
	}
}

func lengthFrame(payload []byte) []byte {
	frame := binary.AppendUvarint(nil, uint64(len(payload)))
	return append(frame, payload...)
}
