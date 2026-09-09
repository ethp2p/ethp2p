package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/quic-go/quic-go"
)

var _ quicreuse.QUICTransport = (*TransportLib)(nil)

// TransportLib provides the libp2p view of a shared QUIC endpoint.
type TransportLib struct{ *TransportShared }

// Listen returns the libp2p listener, starting the shared listener if needed.
// It ignores the supplied configurations. Subsequent calls fail, even after
// the returned listener is closed.
func (t *TransportLib) Listen(_ *tls.Config, _ *quic.Config) (quicreuse.QUICListener, error) {
	claimed, err := t.attach(sideLibp2p)
	if err != nil {
		return nil, err
	}
	if !claimed {
		return nil, errAlreadyListening
	}
	return &listenerLib{transport: t.TransportShared, queue: t.libQ}, nil
}

// Dial opens a libp2p-only connection using tlsConf. It rejects any other ALPN
// and ignores the supplied QUIC configuration.
func (t *TransportLib) Dial(ctx context.Context, addr net.Addr, tlsConf *tls.Config, _ *quic.Config) (quicreuse.QUICConn, error) {
	if t.ctx.Err() != nil {
		return nil, errClosed
	}
	raw, err := t.raw.Dial(ctx, addr, tlsConf, quicConfig.Clone())
	if err != nil {
		return nil, err
	}
	state := raw.ConnectionState()
	if state.TLS.NegotiatedProtocol != AlpnLibp2p {
		_ = raw.CloseWithError(appFailure, "unsupported negotiated protocol")
		return nil, fmt.Errorf("unsupported negotiated protocol %q", state.TLS.NegotiatedProtocol)
	}
	return raw, nil
}

// ReadNonQUICPacket reads the next non-QUIC packet from the shared endpoint.
func (t *TransportLib) ReadNonQUICPacket(ctx context.Context, payload []byte) (int, net.Addr, error) {
	return t.raw.ReadNonQUICPacket(ctx, payload)
}

// WriteTo sends a packet to addr through the shared endpoint.
func (t *TransportLib) WriteTo(payload []byte, addr net.Addr) (int, error) {
	return t.raw.WriteTo(payload, addr)
}

// Close is a no-op returning nil. The application owns endpoint shutdown;
// libp2p must not close the shared endpoint while ethp2p still uses it.
// Use [TransportShared.Close] to stop the endpoint.
func (*TransportLib) Close() error { return nil }
