package broadcast

import (
	"bytes"
	"errors"
	"testing"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/wire"
)

func TestFrameReadUsesProtocolOversizeSentinel(t *testing.T) {
	head := wire.AppendFrameHead(nil, headerFrame, maxBroadcastFrame+1)
	if err := readFrame(bytes.NewReader(head), headerFrame, &bcastpb.Chunk_Header{}); !errors.Is(err, wire.ErrFrameTooLarge) {
		t.Fatalf("readFrame oversized payload error = %v, want wire.ErrFrameTooLarge", err)
	}
}
