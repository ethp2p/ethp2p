package transport

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/protocol"
	"github.com/quic-go/quic-go"
)

func TestConnectionCloseCode(t *testing.T) {
	for _, test := range []struct {
		wire   uint64
		mapped bool
		want   protocol.Code
	}{
		{0, true, protocol.Unspecified},
		{2, true, protocol.Refused},
		{20, true, protocol.Closing},
		{22, true, protocol.ControlViolation},
		{24, true, protocol.NoSharedProtocols},
		{26, true, protocol.Duplicate},
		{1, false, protocol.Unspecified},
		{3, false, protocol.Unspecified},
		{8, false, protocol.Unspecified},
		{1 << 40, false, protocol.Unspecified},
	} {
		t.Run(fmt.Sprint(test.wire), func(t *testing.T) {
			client, eth, _ := newEthp2pEndpoint(t)
			server, _, _ := newEthp2pEndpoint(t)
			// A raw peer deliberately omits GoAway and Hello: the connection
			// close alone must convey a rejection before admission.
			ln, err := server.raw.Listen(server.handshaker.serverConfig(), server.profile.quicConfig())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			closed := make(chan error, 1)
			go func() {
				c, err := ln.Accept(ctx)
				if err == nil {
					_, err = c.AcceptUniStream(ctx)
				}
				if err == nil {
					err = c.CloseWithError(quic.ApplicationErrorCode(test.wire), "test")
				}
				closed <- err
			}()
			conn, err := eth.Dial(ctx, server.Addr(), server.PeerID())
			if err == nil {
				_, err = conn.PeerHello(ctx)
			}
			if test.mapped {
				view, ok := errors.AsType[*ViewClosedError](err)
				if !ok || view.Code != test.want || !view.Remote {
					t.Fatalf("close = %v", err)
				}
			} else {
				app, ok := errors.AsType[*quic.ApplicationError](err)
				if !ok || uint64(app.ErrorCode) != test.wire || !app.Remote {
					t.Fatalf("unmapped close = %v", err)
				}
			}
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
			_ = client.Close()
		})
	}
}

func TestViewCloseConnectionWireCode(t *testing.T) {
	for _, code := range []protocol.Code{protocol.Unspecified, protocol.Closing, protocol.ControlViolation, protocol.NoSharedProtocols, protocol.Duplicate} {
		t.Run(code.String(), func(t *testing.T) {
			p := newViewPair(t, 16)
			if _, err := p.serverEth.PeerHello(t.Context()); err != nil {
				t.Fatal(err)
			}
			_ = p.clientLib.CloseWithError(appNoError, "release libp2p first")
			if err := p.clientEth.CloseWithCode(code); err != nil {
				t.Fatal(err)
			}
			raw := p.serverEth.(*ethp2pConn).conn
			select {
			case <-raw.Context().Done():
			case <-time.After(3 * time.Second):
				t.Fatal("physical connection remained open")
			}
			app, ok := errors.AsType[*quic.ApplicationError](context.Cause(raw.Context()))
			if !ok || !app.Remote || uint64(app.ErrorCode) != code.Wire() {
				t.Fatalf("wire close = %v", context.Cause(raw.Context()))
			}
			_, err := p.serverEth.PeerHello(t.Context())
			view, ok := errors.AsType[*ViewClosedError](err)
			if !ok || !view.Remote || view.Code != code {
				t.Fatalf("view close = %v", err)
			}
		})
	}
}

func TestLibp2pLastClosePreservesGoAway(t *testing.T) {
	p := newViewPair(t, 16)
	if _, err := p.serverEth.PeerHello(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := p.clientEth.CloseWithCode(protocol.Refused); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, _, err := p.serverEth.AcceptUniStream(ctx)
	view, ok := errors.AsType[*ViewClosedError](err)
	if !ok || view.Code != protocol.Refused {
		t.Fatalf("GoAway = %v", err)
	}
	_ = p.clientLib.CloseWithError(appNoError, "release libp2p last")
	raw := p.serverEth.(*ethp2pConn).conn
	select {
	case <-raw.Context().Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	app, ok := errors.AsType[*quic.ApplicationError](context.Cause(raw.Context()))
	if !ok || app.ErrorCode != appNoError {
		t.Fatalf("libp2p close = %v", context.Cause(raw.Context()))
	}
	_, err = p.serverEth.PeerHello(ctx)
	view, ok = errors.AsType[*ViewClosedError](err)
	if !ok || view.Code != protocol.Refused {
		t.Fatalf("GoAway overwritten: %v", err)
	}
}
