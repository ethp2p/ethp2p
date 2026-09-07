package interop

import (
	"context"
	"testing"
	"time"
)

func TestInteropEthp2pWithoutLibp2pListener(t *testing.T) {
	left := newSharedHostMode(t, withoutListener)
	right := newSharedHostMode(t, withoutListener)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	pair := connectEth(t, left.eth, right.eth, right.udp.LocalAddr())
	if err := exchangeEth(ctx, pair.dialed, pair.accepted, ethPayload("no libp2p listener")); err != nil {
		t.Fatal(err)
	}
}
