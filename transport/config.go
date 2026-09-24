package transport

import (
	"context"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// Profile is the connection policy for a shared endpoint: the QUIC settings that
// vary by deployment. Identity, protocol negotiation and certificate
// verification are fixed by the transport rather than by a profile, so a profile
// cannot produce an endpoint that negotiates the wrong protocol or skips
// authentication.
//
// The zero Profile is not usable; construct one with Interop or Shadow. A
// profile is read-only after construction and safe to share.
type Profile struct {
	maxIncomingStreams         int64
	maxIncomingUniStreams      int64
	maxStreamReceiveWindow     uint64
	maxConnectionReceiveWindow uint64
	keepAlivePeriod            time.Duration
	maxIdleTimeout             time.Duration
	disablePathMTUDiscovery    bool
	tracer                     func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace
}

// Interop returns the profile for interoperating with stock libp2p peers.
//
// The stream limits are symmetric, unlike libp2p's own asymmetric 256/5. ethp2p
// opens a unidirectional stream for protocol exchange plus one per broadcast
// control channel, so a five-stream unidirectional limit stalls concurrent
// broadcasts: eight parallel broadcasts delivered nothing at that limit, and
// completed in 0.31s once it was raised.
// The stream limits also bound classified streams waiting for a protocol on one connection.
func Interop() Profile {
	return Profile{
		maxIncomingStreams:         256,
		maxIncomingUniStreams:      256,
		maxStreamReceiveWindow:     10 << 20,
		maxConnectionReceiveWindow: 15 << 20,
		keepAlivePeriod:            15 * time.Second,
	}
}

// Shadow returns the profile for Shadow network simulation. It disables path MTU
// discovery because Shadow cannot set the don't-fragment bit, raises the stream
// limits for large topologies, and traces to qlog.
// The stream limits also bound classified streams waiting for a protocol on one connection.
func Shadow() Profile {
	return Profile{
		maxIncomingStreams:      16384,
		maxIncomingUniStreams:   16384,
		maxIdleTimeout:          365 * 24 * time.Hour,
		disablePathMTUDiscovery: true,
		tracer:                  qlog.DefaultConnectionTracer,
	}
}

// quicConfig renders the profile as a quic.Configuration, applying the settings
// that are invariants of this transport rather than policy.
//
// Versions is pinned to QUIC v1 because the shared endpoint must negotiate the
// same version libp2p does, and libp2p rejects any other. Datagrams and partial
// delivery are enabled because ethp2p_0 supports both.
func (p Profile) quicConfig() *quic.Config {
	return &quic.Config{
		Versions:                         []quic.Version{quic.Version1},
		MaxIncomingStreams:               p.maxIncomingStreams,
		MaxIncomingUniStreams:            p.maxIncomingUniStreams,
		MaxStreamReceiveWindow:           p.maxStreamReceiveWindow,
		MaxConnectionReceiveWindow:       p.maxConnectionReceiveWindow,
		KeepAlivePeriod:                  p.keepAlivePeriod,
		MaxIdleTimeout:                   p.maxIdleTimeout,
		DisablePathMTUDiscovery:          p.disablePathMTUDiscovery,
		Tracer:                           p.tracer,
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true,
	}
}
