package transport

import (
	"time"

	"github.com/quic-go/quic-go"
)

// quicConfig is the connection-wide policy, inherited from libp2p.
var quicConfig = &quic.Config{
	MaxIncomingStreams:               256,
	MaxIncomingUniStreams:            5,
	MaxStreamReceiveWindow:           10 << 20,
	MaxConnectionReceiveWindow:       15 << 20,
	KeepAlivePeriod:                  15 * time.Second,
	Versions:                         []quic.Version{quic.Version1},
	EnableDatagrams:                  true,
	EnableStreamResetPartialDelivery: true,
}
