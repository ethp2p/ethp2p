package broadcast

import (
	"testing"

	"github.com/ethp2p/ethp2p/protocol"
)

func TestEngineProtocols(t *testing.T) {
	engine := NewEngine(EngineConfig{})
	t.Cleanup(func() { _ = engine.Close() })

	set := engine.Protocols()
	if set.Bind == nil {
		t.Fatal("protocol bind function is nil")
	}
	want := []protocol.Descriptor{
		{Codepoint: 1, Name: "ethp2p/bcast"},
		{Codepoint: 2, Name: "ethp2p/session"},
		{Codepoint: 3, Name: "ethp2p/chunk"},
	}
	if len(set.Descriptors) != len(want) {
		t.Fatalf("protocol count = %d, want %d", len(set.Descriptors), len(want))
	}
	for i := range want {
		if set.Descriptors[i] != want[i] {
			t.Fatalf("protocol %d = %+v, want %+v", i, set.Descriptors[i], want[i])
		}
	}

	set.Descriptors[0] = protocol.Descriptor{}
	if got := engine.Protocols().Descriptors[0]; got != want[0] {
		t.Fatalf("Protocols returned shared descriptor storage: %+v", got)
	}
}
