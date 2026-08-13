package ethp2p

import (
	"testing"

	"github.com/ethp2p/ethp2p/transport"
)

type fakeKey struct{}

func (fakeKey) Sign(data []byte) ([]byte, error) { return data, nil }

func testIdentity() transport.PeerID {
	return transport.PeerID("test-peer")
}

func TestStackInit(t *testing.T) {
	s := &Stack{PeerID: testIdentity(), Key: fakeKey{}}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if s.ID() != "test-peer" {
		t.Fatalf("peer ID = %q, want %q", s.ID(), "test-peer")
	}
	if _, err := s.BroadcastEngine(); err != nil {
		t.Fatal(err)
	}
}

func TestStackInitRejectsEmptyPeerID(t *testing.T) {
	if err := (&Stack{Key: fakeKey{}}).Init(); err == nil {
		t.Fatal("accepted empty peer ID")
	}
}

func TestStackInitRejectsNilKey(t *testing.T) {
	if err := (&Stack{PeerID: testIdentity()}).Init(); err == nil {
		t.Fatal("accepted nil key")
	}
}

func TestBroadcastEngineRequiresInit(t *testing.T) {
	if _, err := (&Stack{}).BroadcastEngine(); err == nil {
		t.Fatal("engine created on uninitialized stack")
	}
}

func TestBroadcastEngineIsSingleton(t *testing.T) {
	s := &Stack{PeerID: testIdentity(), Key: fakeKey{}}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	first, err := s.BroadcastEngine()
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.BroadcastEngine()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("BroadcastEngine returned different instances")
	}
}
