package protocol

import (
	"bytes"
	"testing"

	pb "github.com/ethp2p/ethp2p/protocol/pb"
)

func TestWriteReadSelector_RoundTrip(t *testing.T) {
	for _, p := range []pb.Protocol{pb.Protocol_PROTOCOL_BCAST, pb.Protocol_PROTOCOL_SESS, pb.Protocol_PROTOCOL_CHUNK} {
		var buf bytes.Buffer
		if err := WriteSelector(&buf, p); err != nil {
			t.Fatalf("WriteSelector(%v): %v", p, err)
		}
		if got := buf.Len(); got != 1 {
			t.Fatalf("selector length = %d, want 1", got)
		}
		got, err := ReadSelector(&buf)
		if err != nil {
			t.Fatalf("ReadSelector: %v", err)
		}
		if got != p {
			t.Fatalf("got %v, want %v", got, p)
		}
	}
}

func TestReadSelector_TooShort(t *testing.T) {
	buf := bytes.NewReader(nil)
	_, err := ReadSelector(buf)
	if err == nil {
		t.Fatal("expected error for truncated input")
	}
}

func TestReadSelector_Unspecified(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteSelector(&buf, pb.Protocol_PROTOCOL_UNSPECIFIED); err != nil {
		t.Fatal(err)
	}
	_, err := ReadSelector(&buf)
	if err == nil {
		t.Fatal("expected error for unspecified protocol")
	}
}
