package broadcast

import (
	"errors"
	"fmt"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/wire"
)

// Family is broadcast's family code. Stack owns the Hello exchange and
// routing; broadcast declares its family and protocols and handles the streams
// Stack delivers.
const Family wire.Family = 1

// Broadcast's protocols, numbered within Family. With the frame types below
// they are the broadcast wire contract.
const (
	BCAST wire.Protocol = 1
	SESS  wire.Protocol = 2
	CHUNK wire.Protocol = 3
)

// Frame types, numbered per protocol.
const (
	// BCAST: HANDSHAKE, then any number of SUBSCRIBE and UNSUBSCRIBE.
	handshakeFrame   wire.FrameType = 0
	subscribeFrame   wire.FrameType = 1
	unsubscribeFrame wire.FrameType = 2
	// SESS: OPEN, then any number of UPDATE.
	openFrame   wire.FrameType = 0
	updateFrame wire.FrameType = 1
	// CHUNK: HEADER, then one DATA.
	headerFrame wire.FrameType = 0
	dataFrame   wire.FrameType = 1
)

var (
	// Reconstructed ends a SESS stream after the sender reconstructs its message.
	Reconstructed = wire.ProtocolCode(1)
	// Redundant ends a CHUNK stream when the receiver no longer needs its chunk.
	Redundant = wire.ProtocolCode(2)

	errChunkRedundant = errors.New("chunk is no longer needed")
	errStreamRefused  = errors.New("stream will not be processed")
	// errUnexpectedFrame is a frame type the stream's state does not allow,
	// including FIN where a frame is required and a second DATA. Its stream
	// is refused.
	errUnexpectedFrame = fmt.Errorf("frame type not allowed here: %w", errStreamRefused)
)

func streamFailureCode(err error) wire.Code { return wire.TimeoutOr(err, wire.Unspecified) }

func streamCancellationCode(cause error) wire.Code {
	if streamFailureCode(cause) == wire.Timeout {
		return wire.Timeout
	}
	if errors.Is(cause, errChunkRedundant) {
		return Redundant
	}
	if errors.Is(cause, errStreamRefused) {
		return wire.Refused
	}
	return wire.Unspecified
}

// Register adds broadcast's Family to a stack before the stack starts. A
// stack hosts many families from many protocols; this registers broadcast's
// one family, and may succeed only once per engine. Per the startup
// choreography it is called once, before Stack.Start, and never races Close.
func (e *Engine) Register(stack *ethp2p.Stack) error {
	if e.family != nil {
		return errors.New("broadcast already registered")
	}
	if e.ctx.Err() != nil {
		return errors.New("broadcast engine closed")
	}
	family, err := stack.Register(Family, []ethp2p.ProtocolSpec{
		{Protocol: BCAST, MaxQueued: 1},
		{Protocol: SESS, MaxQueued: 64},
		{Protocol: CHUNK, MaxQueued: 128},
	}, e.deliveryWake)
	if err != nil {
		return err
	}
	e.family = family
	e.Start()
	return nil
}
