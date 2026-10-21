package broadcast

import (
	"errors"
	"fmt"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/wire"
)

// Protocol is broadcast v1's protocol number. Stack owns the Hello exchange
// and routing; broadcast reads every stream past its header. An incompatible
// revision takes a new number, which Hello negotiates.
const Protocol wire.Protocol = 1

// Frame types, one namespace across the protocol. A stream's first frame is
// its opening message and names its purpose: HANDSHAKE opens the control
// stream (then SUBSCRIBE and UNSUBSCRIBE), OPEN a session stream (then
// PREAMBLE, then UPDATE), and HEADER a chunk stream (then one DATA).
const (
	handshakeFrame   wire.FrameType = 0
	subscribeFrame   wire.FrameType = 1
	unsubscribeFrame wire.FrameType = 2
	openFrame        wire.FrameType = 3
	preambleFrame    wire.FrameType = 4
	updateFrame      wire.FrameType = 5
	headerFrame      wire.FrameType = 6
	dataFrame        wire.FrameType = 7
)

var (
	// Reconstructed ends a session stream after the sender reconstructs its message.
	Reconstructed = wire.ProtocolCode(1)
	// Redundant ends a chunk stream when the receiver no longer needs its chunk.
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

// Register adds broadcast's Protocol to a stack before the stack starts, and
// may succeed only once per engine. Per the startup choreography it is called
// once, before Stack.Start, and never races Close.
func (e *Engine) Register(stack *ethp2p.Stack) error {
	if e.proto != nil {
		return errors.New("broadcast already registered")
	}
	if e.ctx.Err() != nil {
		return errors.New("broadcast engine closed")
	}
	proto, err := stack.Register(Protocol, e.deliveryWake)
	if err != nil {
		return err
	}
	e.proto = proto
	e.Start()
	return nil
}
