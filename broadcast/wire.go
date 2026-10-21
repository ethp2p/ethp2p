package broadcast

import (
	"errors"
	"fmt"
	"io"

	"github.com/ethp2p/ethp2p/wire"
	"google.golang.org/protobuf/proto"
)

// maxBroadcastFrame is the largest payload of a broadcast frame.
const maxBroadcastFrame = 1 << 20

// maxOpeningFrame bounds OPEN and HEADER, the opening messages read before a
// stream is admitted, so streams held before the handshake stay small.
// HANDSHAKE keeps maxBroadcastFrame: only that bounds its channel list.
const maxOpeningFrame = 16 << 10

func openingLimit(typ wire.FrameType) int {
	if typ == handshakeFrame {
		return maxBroadcastFrame
	}
	return maxOpeningFrame
}

// isPeerFault reports whether a frame read failed on bytes the peer sent
// wrong: FIN before or inside the frame, an oversized payload, or a type or
// length that is not a minimal uvarint. A read deadline, a reset, connection
// closure and local cancellation are not the peer's fault.
func isPeerFault(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, wire.ErrFrameTooLarge) || errors.Is(err, wire.ErrFrameEncoding)
}

// writeFrame writes msg as one frame of type typ to w in one Write call.
func writeFrame(w io.Writer, typ wire.FrameType, msg proto.Message) error {
	frame, err := appendFrame(nil, typ, msg)
	if err != nil {
		return err
	}
	// io.Writer returns an error for any short write.
	_, err = w.Write(frame)
	return err
}

// appendFrame appends msg to dst as one frame of type typ.
func appendFrame(dst []byte, typ wire.FrameType, msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal %T: %w", msg, err)
	}
	return wire.AppendFrame(dst, typ, data), nil
}

// readFrame reads one frame of type want into msg without consuming bytes after
// the frame. A frame of another type is errUnexpectedFrame; FIN before the
// frame is io.EOF.
func readFrame(r io.Reader, want wire.FrameType, msg proto.Message) error {
	typ, data, err := wire.ReadFrame(r, maxBroadcastFrame)
	if err != nil {
		return err
	}
	if typ != want {
		return errUnexpectedFrame
	}
	return unmarshalFrame(data, msg)
}

func unmarshalFrame(data []byte, msg proto.Message) error {
	if err := proto.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("unmarshal %T: %w", msg, err)
	}
	return nil
}

// readChunkData reads the DATA frame that follows a chunk stream's HEADER, then
// the stream's FIN. The frame must be DATA with exactly dataLen bytes and the
// only frame after HEADER; otherwise the error is errUnexpectedFrame. dataLen
// bounds the allocation, so the caller admits it first.
func readChunkData(r io.Reader, dataLen uint32) ([]byte, error) {
	typ, data, err := wire.ReadFrame(r, int(dataLen))
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, wire.ErrFrameTooLarge):
		// FIN where DATA belongs, or a DATA longer than announced.
		return nil, errUnexpectedFrame
	case err != nil:
		return nil, err
	case typ != dataFrame || len(data) != int(dataLen):
		return nil, errUnexpectedFrame
	}
	var next [1]byte
	switch _, err := io.ReadFull(r, next[:]); {
	case errors.Is(err, io.EOF):
		return data, nil
	case err == nil:
		return nil, errUnexpectedFrame
	default:
		return nil, err
	}
}
