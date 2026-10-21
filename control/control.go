// Package control implements the ethp2p control protocol's wire format:
// the HELLO and GOAWAY frames that open every view, on the view's first
// stream. It is bytes-in/values-out format code; the session lifecycle
// that reads and writes these frames lives with the supervisor in the
// root package.
package control

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/ethp2p/ethp2p/wire"
	"google.golang.org/protobuf/proto"
)

// ControlProtocol is the protocol of the control protocol. Every view shares it,
// and no Hello announces it.
const ControlProtocol wire.Protocol = 0

// MaxProtocols bounds the number of protocols in one Hello.
const MaxProtocols = 1024

var errInvalidProtocols = errors.New("invalid protocol list")

// ValidateProtocols requires a strictly ascending list of at most MaxProtocols
// protocols, none of them the control protocol or the reserved protocol.
func ValidateProtocols(protocols []wire.Protocol) error {
	if len(protocols) > MaxProtocols {
		return fmt.Errorf("%w: maximum is %d", errInvalidProtocols, MaxProtocols)
	}
	for i, protocol := range protocols {
		if protocol == ControlProtocol || protocol == wire.ReservedProtocol {
			return fmt.Errorf("%w: protocol %d is never announced", errInvalidProtocols, protocol)
		}
		if i > 0 && protocol <= protocols[i-1] {
			return fmt.Errorf("%w: protocols must be strictly ascending", errInvalidProtocols)
		}
	}
	return nil
}

// maxControlFrame is MAX_CONTROL_FRAME, the payload limit of a control frame.
const maxControlFrame = 16 << 10

const (
	helloFrame  wire.FrameType = 0
	goAwayFrame wire.FrameType = 1
)

var (
	errUnexpectedControlFrame = errors.New("control frame type not allowed here")
	errUnknownControlField    = errors.New("control message has unknown fields")
	errNotControlStream       = errors.New("first unidirectional stream is not the control stream")
)

// PeerHello is a peer's decoded HELLO.
type PeerHello struct {
	Protocols []wire.Protocol // strictly ascending; see ValidateProtocols
	Record    []byte          // empty, or an EIP-778 encoding the supervisor checks
}

// HelloBytes returns our control stream's first write: its stream header and
// HELLO frame. It fails when the HELLO exceeds MAX_CONTROL_FRAME.
func HelloBytes(protocols []wire.Protocol, record []byte) ([]byte, error) {
	msg := &Hello{Protocols: make([]uint32, len(protocols)), Record: record}
	for i, protocol := range protocols {
		msg.Protocols[i] = uint32(protocol)
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxControlFrame {
		return nil, wire.ErrFrameTooLarge
	}
	return wire.AppendFrame(wire.AppendHeader(nil, ControlProtocol), helloFrame, raw), nil
}

// ReadHello reads a control stream's header and HELLO frame from in and
// returns the decoded Hello. The stream must be the peer's first
// unidirectional stream: any other protocol is an error.
func ReadHello(in io.Reader) (PeerHello, error) {
	protocol, err := wire.ReadHeader(in)
	if err != nil {
		return PeerHello{}, err
	}
	if protocol != ControlProtocol {
		return PeerHello{}, errNotControlStream
	}
	msg := new(Hello)
	if err := readControlFrame(in, helloFrame, msg); err != nil {
		return PeerHello{}, err
	}
	h := PeerHello{Protocols: make([]wire.Protocol, len(msg.Protocols)), Record: msg.Record}
	for i, protocol := range msg.Protocols {
		h.Protocols[i] = wire.Protocol(protocol)
	}
	if err := ValidateProtocols(h.Protocols); err != nil {
		return PeerHello{}, err
	}
	return h, nil
}

// GoAwayBytes returns the framed GOAWAY write for code. GOAWAY carries
// code.Wire() >> 1. Marshalling this struct cannot meaningfully fail; a
// failure yields nil.
func GoAwayBytes(code wire.Code) []byte {
	raw, err := proto.Marshal(&GoAway{Code: code.Wire() >> 1})
	if err != nil {
		return nil
	}
	return wire.AppendFrame(nil, goAwayFrame, raw)
}

// ReadGoAway reads one frame from in, which must be GOAWAY, and returns its
// stack code. A code too large to be a stack code yields Unspecified. Read
// and format errors propagate raw; classifying them is the caller's job.
func ReadGoAway(in io.Reader) (wire.Code, error) {
	msg := new(GoAway)
	if err := readControlFrame(in, goAwayFrame, msg); err != nil {
		return wire.Unspecified, err
	}
	if msg.Code <= math.MaxUint64>>1 {
		return wire.ParseCode(msg.Code << 1), nil
	}
	return wire.Unspecified, nil
}

// readControlFrame reads one frame of type want into message. ReadFrame never
// reads past it.
func readControlFrame(in io.Reader, want wire.FrameType, message proto.Message) error {
	typ, raw, err := wire.ReadFrame(in, maxControlFrame)
	if err != nil {
		return err
	}
	if typ != want {
		return errUnexpectedControlFrame
	}
	if err := proto.Unmarshal(raw, message); err != nil {
		return err
	}
	if len(message.ProtoReflect().GetUnknown()) != 0 {
		return errUnknownControlField
	}
	return nil
}
