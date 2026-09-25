package devhook

import (
	"context"
	"testing"
)

func TestWaitDisabledDoesNotChangeCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var hooks *Hooks
	if !hooks.Wait(ctx, Site{Point: PointChunkRead}) {
		t.Fatal("nil hooks must pass without checking context")
	}
	if !(new(Hooks)).Wait(ctx, Site{Point: PointChunkRead}) {
		t.Fatal("nil gate callback must pass without checking context")
	}
}
