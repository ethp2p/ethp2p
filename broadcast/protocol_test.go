package broadcast

import (
	"testing"

	"github.com/ethp2p/ethp2p/wire"
)

func TestBroadcastSelectors(t *testing.T) {
	for _, selector := range []wire.Selector{BCAST, SESS, CHUNK} {
		if !isBroadcastSelector(selector) {
			t.Errorf("broadcast selector %d rejected", selector)
		}
	}
}
