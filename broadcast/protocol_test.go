package broadcast

import (
	"testing"

	"github.com/ethp2p/ethp2p/protocol"
)

func TestBroadcastSelectors(t *testing.T) {
	selectors := []protocol.Selector{BCAST, SESS, CHUNK}
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
	for _, selectors := range [][]protocol.Selector{
		{BCAST, SESS},
		{BCAST, CHUNK},
		{SESS, CHUNK},
	} {
		if supportsBroadcastSelectors(selectors) {
			t.Errorf("incomplete selector set accepted: %v", selectors)
		}
	}
}
