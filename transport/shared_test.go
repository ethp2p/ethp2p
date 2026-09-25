package transport

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
)

// TestBidiReadReportsStreamReset exercises cancellation after classification,
// when the remote ethp2p view has already received the stream.
func TestBidiReadReportsStreamReset(t *testing.T) {
	ctx := testContext(t)
	_, _, clientEth, _ := newEndpoint(t)
	_, _, serverEth, serverPC := newEndpoint(t)
	accepted := make(chan struct {
		conn *Conn
		err  error
	}, 1)
	go func() {
		conn, err := serverEth.Accept(ctx)
		accepted <- struct {
			conn *Conn
			err  error
		}{conn, err}
	}()

	clientConn, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	result := <-accepted
	if result.err != nil {
		t.Fatal(result.err)
	}
	serverConn := result.conn
	out, err := clientConn.OpenStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	in, selector := nextStream(t, serverConn)
	if selector != 1 {
		t.Fatalf("selector = %d, want 1", selector)
	}
	out.CancelWrite(0x42)
	_, err = io.Copy(io.Discard, in)
	reset, ok := errors.AsType[*StreamResetError](err)
	if !ok || reset.Code != 0x42 {
		t.Fatalf("read after cancellation = %v, want StreamResetError code 0x42", err)
	}
}

func TestEthp2pNegotiationRoutesBothProtocols(t *testing.T) {
	ctx := testContext(t)
	_, clientLib, clientEth, _ := newEndpoint(t)
	_, serverLib, serverEth, serverPC := newEndpoint(t)
	clientListener := listen(t, clientLib, clientEth)
	serverListener := listen(t, serverLib, serverEth)

	clientEthConn, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	clientLibConn, err := clientListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	serverLibConn, err := serverListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	serverEthConn, err := serverEth.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}

	libPayload := []byte("/multistream/1.0.0\n")
	libWire := frame(libPayload)
	libOut, err := clientLibConn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := libOut.Write(libWire); err != nil {
		t.Fatal(err)
	}
	libIn, err := serverLibConn.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotLib := make([]byte, len(libWire))
	if _, err := io.ReadFull(libIn, gotLib); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotLib, libWire) {
		t.Fatalf("libp2p stream = %x, want %x", gotLib, libWire)
	}

	ethWire := []byte("ok")
	ethOut, err := clientEthConn.OpenStream(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ethOut.Write(ethWire); err != nil {
		t.Fatal(err)
	}
	ethIn, selector := nextStream(t, serverEthConn)
	if selector != 1 {
		t.Fatalf("selector = %d, want 1", selector)
	}
	gotEth := make([]byte, len(ethWire))
	if _, err := io.ReadFull(ethIn, gotEth); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotEth, ethWire) {
		t.Fatalf("ethp2p stream = %x, want original %x", gotEth, ethWire)
	}

	uniPayload := []byte("unidirectional")
	uniOut, err := clientEthConn.OpenUniStream(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uniOut.Write(uniPayload); err != nil {
		t.Fatal(err)
	}
	if err := uniOut.Close(); err != nil {
		t.Fatal(err)
	}
	uniIn, selector := nextStream(t, serverEthConn)
	if selector != 2 {
		t.Fatalf("uni selector = %d, want 2", selector)
	}
	gotUni, err := io.ReadAll(uniIn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotUni, uniPayload) {
		t.Fatalf("ethp2p unidirectional stream = %x, want %x", gotUni, uniPayload)
	}
}

// bidi is the common stream surface for echo and round-trip on either view.
type bidi interface {
	io.Reader
	io.Writer
	Close() error
	SetDeadline(time.Time) error
}

// TestSimultaneousStreamsOnBothViews exercises open, write, read, and close
// on the libp2p and ethp2p views of one shared connection at the same time,
// initiated from both ends, plus unidirectional streams on ethp2p.
func TestSimultaneousStreamsOnBothViews(t *testing.T) {
	ctx := testContext(t)
	_, clientLib, clientEth, _ := newEndpoint(t)
	_, serverLib, serverEth, serverPC := newEndpoint(t)
	clientListener := listen(t, clientLib, clientEth)
	serverListener := listen(t, serverLib, serverEth)

	clientEthConn, err := clientEth.Dial(ctx, serverPC.LocalAddr(), serverEth.shared.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	clientLibConn, err := clientListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	serverLibConn, err := serverListener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	serverEthConn, err := serverEth.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Keep the burst within libp2p's bounded delivery queue. ethp2p delivery
	// instead retains streams and relies on QUIC credit for backpressure.
	const streams = 4
	payloadLen := 64 << 10
	deadline := time.Now().Add(4 * time.Second)

	var wg sync.WaitGroup
	errs := make(chan error, 128)
	fail := func(err error) {
		if err != nil {
			errs <- err
		}
	}

	// echo reads a stream to FIN, writes the payload back, and closes.
	// Each caller runs in a goroutine tracked by wg.
	echo := func(s bidi) {
		fail(s.SetDeadline(deadline))
		got, err := io.ReadAll(s)
		if err != nil {
			fail(err)
			return
		}
		if _, err := s.Write(got); err != nil {
			fail(err)
			return
		}
		fail(s.Close())
	}

	// roundTrip writes a payload, closes, and verifies the echo.
	roundTrip := func(s bidi, payload []byte) {
		fail(s.SetDeadline(deadline))
		if _, err := s.Write(payload); err != nil {
			fail(err)
			return
		}
		if err := s.Close(); err != nil {
			fail(err)
			return
		}
		got, err := io.ReadAll(s)
		if err != nil {
			fail(err)
			return
		}
		if !bytes.Equal(got, payload) {
			fail(fmt.Errorf("echo = %d bytes, want %d", len(got), len(payload)))
		}
	}

	// Receiving side: accept and echo concurrently on both views, from
	// both ends. The merged ethp2p queue interleaves directions, so each
	// ethp2p receiver routes by type: bidirectional streams echo, and
	// unidirectional streams are read against the shared payload.
	uniPayload := bytes.Repeat([]byte{0x55}, payloadLen)
	wg.Go(func() {
		for range 2 * streams {
			rs, _, err := waitNextStream(ctx, serverEthConn)
			if err != nil {
				fail(err)
				return
			}
			if s, ok := rs.(Stream); ok {
				echo(s)
				continue
			}
			fail(rs.SetReadDeadline(deadline))
			got, err := io.ReadAll(rs)
			if err != nil {
				fail(err)
				continue
			}
			if !bytes.Equal(got, uniPayload) {
				fail(fmt.Errorf("unidirectional echo = %d bytes, want %d", len(got), len(uniPayload)))
			}
		}
	})
	wg.Go(func() {
		for range streams {
			rs, _, err := waitNextStream(ctx, clientEthConn)
			if err != nil {
				fail(err)
				return
			}
			s, ok := rs.(Stream)
			if !ok {
				fail(fmt.Errorf("ethp2p stream %T does not implement Stream", rs))
				continue
			}
			echo(s)
		}
	})
	for range streams {
		wg.Go(func() {
			s, err := serverLibConn.AcceptStream(ctx)
			if err != nil {
				fail(err)
				return
			}
			echo(s)
		})
		wg.Go(func() {
			s, err := clientLibConn.AcceptStream(ctx)
			if err != nil {
				fail(err)
				return
			}
			echo(s)
		})
	}

	// Sending side: round-trip streams opened from both ends on both
	// views. ethp2p opens write the selector frame before the payload.
	for i := range streams {
		libWire := append(frame([]byte("/multistream/1.0.0\n")), frame(append([]byte("/echo/"), bytes.Repeat([]byte{byte(i)}, payloadLen)...))...)
		ethWire := bytes.Repeat([]byte{0xaa}, payloadLen)
		wg.Go(func() {
			s, err := clientLibConn.OpenStreamSync(ctx)
			if err != nil {
				fail(err)
				return
			}
			roundTrip(s, libWire)
		})
		wg.Go(func() {
			s, err := clientEthConn.OpenStream(ctx, wire.Selector(i+1))
			if err != nil {
				fail(err)
				return
			}
			roundTrip(s, ethWire)
		})
		wg.Go(func() {
			s, err := serverLibConn.OpenStreamSync(ctx)
			if err != nil {
				fail(err)
				return
			}
			roundTrip(s, libWire)
		})
		wg.Go(func() {
			s, err := serverEthConn.OpenStream(ctx, wire.Selector(i+1))
			if err != nil {
				fail(err)
				return
			}
			roundTrip(s, ethWire)
		})
	}

	// Unidirectional streams route to ethp2p on the receiving side only;
	// exercise open, write, close against receive and read.
	// Delivery order is arbitrary, so every unidirectional stream carries
	// the same payload.
	for range streams {
		wg.Go(func() {
			s, err := clientEthConn.OpenUniStream(ctx, 3)
			if err != nil {
				fail(err)
				return
			}
			fail(s.SetWriteDeadline(deadline))
			if _, err := s.Write(uniPayload); err != nil {
				fail(err)
				return
			}
			fail(s.Close())
		})
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
