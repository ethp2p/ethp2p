package control

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/ethp2p/ethp2p/wire"
	"google.golang.org/protobuf/proto"
)

func TestValidateFamilies(t *testing.T) {
	if err := ValidateFamilies([]wire.Family{1, 2, 18, 20, math.MaxUint32}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFamilies(nil); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		entries []wire.Family
	}{
		{name: "non-ascending", entries: []wire.Family{2, 1}},
		{name: "duplicate", entries: []wire.Family{3, 3}},
		{name: "control family", entries: []wire.Family{ControlFamily}},
		{name: "reserved family", entries: []wire.Family{1, wire.ReservedFamily}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateFamilies(test.entries); !errors.Is(err, errInvalidFamilies) {
				t.Fatalf("ValidateFamilies error = %v, want %v", err, errInvalidFamilies)
			}
		})
	}
	families := make([]wire.Family, MaxFamilies)
	for i := range families {
		families[i] = wire.Family(i + 1000)
	}
	if err := ValidateFamilies(families); err != nil {
		t.Fatalf("ValidateFamilies(%d families): %v", MaxFamilies, err)
	}
	if err := ValidateFamilies(append(families, 2024)); !errors.Is(err, errInvalidFamilies) {
		t.Fatalf("ValidateFamilies too many = %v", err)
	}
}

func TestHelloRoundTrip(t *testing.T) {
	raw, err := HelloBytes([]wire.Family{1, 7, 20}, []byte{0xaa, 0xbb})
	if err != nil {
		t.Fatal(err)
	}
	h, err := ReadHello(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Families) != 3 || h.Families[0] != 1 || h.Families[1] != 7 || h.Families[2] != 20 {
		t.Fatalf("families = %v", h.Families)
	}
	if !bytes.Equal(h.Record, []byte{0xaa, 0xbb}) {
		t.Fatalf("record = %x", h.Record)
	}
}

func TestReadHelloRejectsNonControlStream(t *testing.T) {
	msg, err := proto.Marshal(&Hello{Families: []uint32{1}})
	if err != nil {
		t.Fatal(err)
	}
	raw := wire.AppendFrame(wire.AppendSelector(nil, wire.Selector{Family: 9}), helloFrame, msg)
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
	msg, err := proto.Marshal(&Hello{Families: []uint32{1}})
	if err != nil {
		t.Fatal(err)
	}
	raw := wire.AppendFrame(nil, helloFrame, msg)
	if _, err := ReadGoAway(bytes.NewReader(raw)); !errors.Is(err, errUnexpectedControlFrame) {
		t.Fatalf("ReadGoAway error = %v, want %v", err, errUnexpectedControlFrame)
	}
}
