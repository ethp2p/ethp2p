package broadcast

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/protocol"
	"google.golang.org/protobuf/proto"
)

type writeCounter struct {
	bytes.Buffer
	calls int
}

func (w *writeCounter) Write(p []byte) (int, error) {
	w.calls++
	return w.Buffer.Write(p)
}

func TestWriteFrameUsesProtocolFramingInOneWrite(t *testing.T) {
	msg := &bcastpb.Chunk_Header{Channel: "channel", DataLength: 3}
	writer := &writeCounter{}
	if err := WriteFrame(writer, msg); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if writer.calls != 1 {
		t.Fatalf("Write calls = %d, want 1", writer.calls)
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	want := protocol.AppendFrame(nil, payload)
	if !bytes.Equal(writer.Bytes(), want) {
		t.Fatalf("frame = %x, want %x", writer.Bytes(), want)
	}
}

func TestReadFrameLeavesChunkDataAfterHeader(t *testing.T) {
	const chunk = "raw chunk bytes"
	header := &bcastpb.Chunk_Header{Channel: "channel", DataLength: uint32(len(chunk))}
	var wire bytes.Buffer
	if err := WriteFrame(&wire, header); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if _, err := wire.WriteString(chunk); err != nil {
		t.Fatal(err)
	}

	reader := bytes.NewReader(wire.Bytes())
	var gotHeader bcastpb.Chunk_Header
	if err := ReadFrame(reader, &gotHeader); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if !proto.Equal(&gotHeader, header) {
		t.Fatalf("header = %v, want %v", &gotHeader, header)
	}
	gotChunk := make([]byte, len(chunk))
	if _, err := io.ReadFull(reader, gotChunk); err != nil {
		t.Fatalf("read raw chunk: %v", err)
	}
	if string(gotChunk) != chunk {
		t.Fatalf("chunk = %q, want %q", gotChunk, chunk)
	}
}

func TestReadFrameUsesProtocolOversizeSentinel(t *testing.T) {
	prefix := binary.AppendUvarint(nil, uint64(MaxFrameSize+1))
	if err := ReadFrame(bytes.NewReader(prefix), &bcastpb.Bcast{}); !errors.Is(err, protocol.ErrFrameTooLarge) {
		t.Fatalf("ReadFrame oversized payload error = %v, want protocol.ErrFrameTooLarge", err)
	}
}
