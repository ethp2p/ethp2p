package broadcast

import (
	"testing"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/wire"
)

func TestBroadcastSelectors(t *testing.T) {
	selectors := []wire.Selector{BCAST, SESS, CHUNK}
	if !supportsBroadcastSelectors(selectors) {
		t.Fatalf("broadcast selectors rejected: %v", selectors)
	}

	for _, selector := range selectors {
		if !isBroadcastSelector(selector) {
			t.Errorf("broadcast selector %d rejected", selector)
		}
	}
}

func TestBroadcastSelectorsRequireCompleteSet(t *testing.T) {
	for _, selectors := range [][]wire.Selector{
		{BCAST, SESS},
		{BCAST, CHUNK},
		{SESS, CHUNK},
	} {
		if supportsBroadcastSelectors(selectors) {
			t.Errorf("incomplete selector set accepted: %v", selectors)
		}
	}
}

func TestSupportsBroadcastRequiresAuthenticatedPeerID(t *testing.T) {
	peer := new(ethp2p.Peer)
	if supportsBroadcast(peer) {
		t.Fatal("accepted peer with empty authenticated ID")
	}
	peer = newOutcomeFixture(t).peer
	if !supportsBroadcast(peer) {
		t.Fatal("rejected peer with complete selectors and authenticated ID")
	}
}
