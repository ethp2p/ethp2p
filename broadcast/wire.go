package broadcast

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

const MaxFrameSize = 1 << 20 // 1MB

var ErrFrameTooLarge = errors.New("frame too large")

// WriteFrame writes a length-prefixed protobuf message to w.
// The 4-byte big-endian length and serialized payload are coalesced
// into a single Write call.
func WriteFrame(w io.Writer, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal %T: %w", msg, err)
	}

	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)

	_, err = w.Write(buf)
	return err
}

// ReadFrame reads a length-prefixed protobuf message from r.
func ReadFrame(r io.Reader, msg proto.Message) error {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length > MaxFrameSize {
		return ErrFrameTooLarge
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	if err := proto.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("unmarshal %T: %w", msg, err)
	}
	return nil
}

