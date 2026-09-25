package transport

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

func TestConnectionCloseCode(t *testing.T) {
	for _, test := range []struct {
		wire   uint64
		mapped bool
		want   wire.Code
	}{
		{0, true, wire.Unspecified},
		{2, true, wire.Refused},
		{20, true, wire.Closing},
		{22, true, wire.ControlViolation},
		{24, true, wire.NoSharedProtocols},
		{26, true, wire.Duplicate},
		{1, false, wire.Unspecified},
		{3, false, wire.Unspecified},
		{8, false, wire.Unspecified},
		{1 << 40, false, wire.Unspecified},
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
			// Dial waits for the peer Hello, so a connection close before any
			// Hello arrives is already the dial error.
			_, err = eth.Dial(ctx, server.Addr(), server.PeerID())
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
	for _, code := range []wire.Code{wire.Unspecified, wire.Closing, wire.ControlViolation, wire.NoSharedProtocols, wire.Duplicate} {
		t.Run(code.String(), func(t *testing.T) {
			p := newViewPair(t, 16)
			// Accept and Dial return only after Hello, so no wait is needed here.
			_ = p.clientLib.CloseWithError(appNoError, "release libp2p first")
			if err := p.clientEth.CloseWithCode(code); err != nil {
				t.Fatal(err)
			}
			raw := p.serverEth.conn
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			select {
			case <-raw.Context().Done():
			case <-time.After(3 * time.Second):
				t.Fatal("physical connection remained open")
			}
			app, ok := errors.AsType[*quic.ApplicationError](context.Cause(raw.Context()))
			if !ok || !app.Remote || uint64(app.ErrorCode) != code.Wire() {
				t.Fatalf("wire close = %v", context.Cause(raw.Context()))
			}
			select {
			case <-p.serverEth.Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if got := p.serverEth.CloseCode(); got != code {
				t.Fatalf("CloseCode = %s, want %s", got, code)
			}
			_, err := p.serverEth.OpenStream(ctx, 1)
			view, ok := errors.AsType[*ViewClosedError](err)
			if !ok || !view.Remote || view.Code != code {
				t.Fatalf("view close = %v", err)
			}
		})
	}
}

func TestLibp2pLastClosePreservesGoAway(t *testing.T) {
	p := newViewPair(t, 16)
	if err := p.clientEth.CloseWithCode(wire.Refused); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	select {
	case <-p.serverEth.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got := p.serverEth.CloseCode(); got != wire.Refused {
		t.Fatalf("CloseCode = %s, want Refused", got)
	}
	_, err := p.serverEth.OpenStream(ctx, 1)
	view, ok := errors.AsType[*ViewClosedError](err)
	if !ok || !view.Remote || view.Code != wire.Refused {
		t.Fatalf("GoAway = %v", err)
	}
	_ = p.clientLib.CloseWithError(appNoError, "release libp2p last")
	raw := p.serverEth.conn
	select {
	case <-raw.Context().Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	app, ok := errors.AsType[*quic.ApplicationError](context.Cause(raw.Context()))
	if !ok || app.ErrorCode != appNoError {
		t.Fatalf("libp2p close = %v", context.Cause(raw.Context()))
	}
	_, err = p.serverEth.OpenStream(ctx, 1)
	view, ok = errors.AsType[*ViewClosedError](err)
	if !ok || !view.Remote || view.Code != wire.Refused {
		t.Fatalf("GoAway overwritten: %v", err)
	}
}
