package broadcast

import (
	"fmt"
	"io"

	"github.com/ethp2p/ethp2p/wire"
	"google.golang.org/protobuf/proto"
)

const MaxFrameSize = 1 << 20 // 1MB

// WriteFrame writes a uvarint-length-prefixed protobuf message to w in one
// Write call.
func WriteFrame(w io.Writer, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal %T: %w", msg, err)
	}

	frame := wire.AppendFrame(nil, data)
	n, err := w.Write(frame)
	if err == nil && n != len(frame) {
		return io.ErrShortWrite
	}
	return err
}

// ReadFrame reads one uvarint-length-prefixed protobuf message from r without
// consuming bytes after the frame.
func ReadFrame(r io.Reader, msg proto.Message) error {
	data, err := wire.ReadFrame(r, MaxFrameSize)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("unmarshal %T: %w", msg, err)
	}
	return nil
}
