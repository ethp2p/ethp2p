package compat

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNewStackDerivesIdentityFromLibp2pKey(t *testing.T) {
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewStack(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	wantID, err := peer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if s.ID() != string(wantID) {
		t.Fatalf("peer ID = %q, want %q", s.ID(), wantID)
	}
}

func TestNewStackRejectsNilKey(t *testing.T) {
	if _, err := NewStack(nil); err == nil {
		t.Fatal("accepted nil key")
	}
}
