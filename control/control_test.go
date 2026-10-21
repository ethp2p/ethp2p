package control

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/ethp2p/ethp2p/wire"
	"google.golang.org/protobuf/proto"
)

func TestValidateProtocols(t *testing.T) {
	if err := ValidateProtocols([]wire.Protocol{1, 2, 18, 20, math.MaxUint32}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateProtocols(nil); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		entries []wire.Protocol
	}{
		{name: "non-ascending", entries: []wire.Protocol{2, 1}},
		{name: "duplicate", entries: []wire.Protocol{3, 3}},
		{name: "control protocol", entries: []wire.Protocol{ControlProtocol}},
		{name: "reserved protocol", entries: []wire.Protocol{1, wire.ReservedProtocol}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateProtocols(test.entries); !errors.Is(err, errInvalidProtocols) {
				t.Fatalf("ValidateProtocols error = %v, want %v", err, errInvalidProtocols)
			}
		})
	}
	protocols := make([]wire.Protocol, MaxProtocols)
	for i := range protocols {
		protocols[i] = wire.Protocol(i + 1000)
	}
	if err := ValidateProtocols(protocols); err != nil {
		t.Fatalf("ValidateProtocols(%d protocols): %v", MaxProtocols, err)
	}
	if err := ValidateProtocols(append(protocols, 2024)); !errors.Is(err, errInvalidProtocols) {
		t.Fatalf("ValidateProtocols too many = %v", err)
	}
}

func TestHelloRoundTrip(t *testing.T) {
	raw, err := HelloBytes([]wire.Protocol{1, 7, 20}, []byte{0xaa, 0xbb})
	if err != nil {
		t.Fatal(err)
	}
	h, err := ReadHello(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Protocols) != 3 || h.Protocols[0] != 1 || h.Protocols[1] != 7 || h.Protocols[2] != 20 {
		t.Fatalf("protocols = %v", h.Protocols)
	}
	if !bytes.Equal(h.Record, []byte{0xaa, 0xbb}) {
		t.Fatalf("record = %x", h.Record)
	}
}

func TestReadHelloRejectsNonControlStream(t *testing.T) {
	msg, err := proto.Marshal(&Hello{Protocols: []uint32{1}})
	if err != nil {
		t.Fatal(err)
	}
	raw := wire.AppendFrame(wire.AppendHeader(nil, 9), helloFrame, msg)
	if _, err := ReadHello(bytes.NewReader(raw)); !errors.Is(err, errNotControlStream) {
		t.Fatalf("ReadHello error = %v, want %v", err, errNotControlStream)
	}
}

func TestGoAwayRoundTrip(t *testing.T) {
	for _, code := range []wire.Code{wire.Unspecified, wire.Closing, wire.ControlViolation} {
		got, err := ReadGoAway(bytes.NewReader(GoAwayBytes(code)))
		if err != nil {
			t.Fatal(err)
		}
		if got != code {
			t.Fatalf("code = %v, want %v", got, code)
		}
	}
}

func TestReadGoAwayRejectsHelloFrame(t *testing.T) {
	msg, err := proto.Marshal(&Hello{Protocols: []uint32{1}})
	if err != nil {
		t.Fatal(err)
	}
	raw := wire.AppendFrame(nil, helloFrame, msg)
	if _, err := ReadGoAway(bytes.NewReader(raw)); !errors.Is(err, errUnexpectedControlFrame) {
		t.Fatalf("ReadGoAway error = %v, want %v", err, errUnexpectedControlFrame)
	}
}
