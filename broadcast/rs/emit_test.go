package rs

import "testing"

func TestChunkHeap_OrderingAndUpdates(t *testing.T) {
	h := newEmitPlanner()
	h.Insert(emitEntry{Idx: 3, Times: 5})
	h.Insert(emitEntry{Idx: 1, Times: 2})
	h.Insert(emitEntry{Idx: 7, Times: 8})
	if top, ok := h.Top(); !ok || top.Idx != 1 || top.Times != 2 || h.Len() != 3 {
		t.Fatalf("Top = %+v, %v; Len = %d", top, ok, h.Len())
	}

	// Re-inserting an index updates it rather than duplicating it.
	h.Insert(emitEntry{Idx: 3, Times: 1})
	if top, _ := h.Top(); top.Idx != 3 || h.Len() != 3 {
		t.Fatalf("after update Top = %+v; Len = %d", top, h.Len())
	}

	// Incrementing the minimum twice makes the next entry the minimum.
	h.Increment(3)
	h.Increment(3)
	if top, _ := h.Top(); top.Idx != 1 {
		t.Fatalf("after increments Top = %+v", top)
	}

	h.Delete(1)
	if top, _ := h.Top(); top.Idx != 3 || h.Len() != 2 {
		t.Fatalf("after delete Top = %+v; Len = %d", top, h.Len())
	}

	for _, want := range []int{3, 7} {
		if got := h.PopFront(); got.Idx != want {
			t.Fatalf("PopFront = %d, want %d", got.Idx, want)
		}
	}
	if h.Len() != 0 {
		t.Fatalf("Len after draining = %d, want 0", h.Len())
	}
}

func TestChunkHeap_EmptyAndMissing(t *testing.T) {
	h := newEmitPlanner()
	if _, ok := h.Top(); ok {
		t.Fatal("Top on empty heap returned true")
	}
	h.Insert(emitEntry{Idx: 0, Times: 1})
	h.Increment(99)
	h.Delete(99)
	if h.Len() != 1 {
		t.Fatalf("Len = %d, want 1 after missing increment/delete", h.Len())
	}
}
