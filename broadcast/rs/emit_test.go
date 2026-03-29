package rs

import "testing"

func TestChunkHeap_InsertAndTop(t *testing.T) {
	h := newEmitPlanner()

	h.Insert(emitEntry{Idx: 3, Times: 5})
	h.Insert(emitEntry{Idx: 1, Times: 2})
	h.Insert(emitEntry{Idx: 7, Times: 8})

	ec, ok := h.Top()
	if !ok {
		t.Fatal("Top on non-empty heap returned false")
	}
	if ec.Idx != 1 || ec.Times != 2 {
		t.Fatalf("Top = {Idx:%d, Times:%d}, want {1, 2}", ec.Idx, ec.Times)
	}
	if h.Len() != 3 {
		t.Fatalf("Len = %d, want 3", h.Len())
	}
}

func TestChunkHeap_TopEmpty(t *testing.T) {
	h := newEmitPlanner()
	_, ok := h.Top()
	if ok {
		t.Fatal("Top on empty heap should return false")
	}
}

func TestChunkHeap_PopFrontOrder(t *testing.T) {
	h := newEmitPlanner()
	h.Insert(emitEntry{Idx: 0, Times: 10})
	h.Insert(emitEntry{Idx: 1, Times: 1})
	h.Insert(emitEntry{Idx: 2, Times: 5})

	want := []int{1, 2, 0}
	for i, wantIdx := range want {
		ec := h.PopFront()
		if ec.Idx != wantIdx {
			t.Fatalf("PopFront[%d].Idx = %d, want %d", i, ec.Idx, wantIdx)
		}
	}
	if h.Len() != 0 {
		t.Fatalf("Len after draining = %d, want 0", h.Len())
	}
}

func TestChunkHeap_Increment(t *testing.T) {
	h := newEmitPlanner()
	h.Insert(emitEntry{Idx: 0, Times: 1})
	h.Insert(emitEntry{Idx: 1, Times: 2})

	// Incrementing idx=0 twice makes its count 3, so idx=1 (count=2) becomes top.
	h.Increment(0)
	h.Increment(0)

	ec, _ := h.Top()
	if ec.Idx != 1 {
		t.Fatalf("Top.Idx = %d, want 1 after incrementing 0", ec.Idx)
	}
}

func TestChunkHeap_IncrementMissing(t *testing.T) {
	h := newEmitPlanner()
	h.Insert(emitEntry{Idx: 0, Times: 1})
	// Should not panic.
	h.Increment(99)
	if h.Len() != 1 {
		t.Fatalf("Len = %d, want 1", h.Len())
	}
}

func TestChunkHeap_Delete(t *testing.T) {
	h := newEmitPlanner()
	h.Insert(emitEntry{Idx: 0, Times: 1})
	h.Insert(emitEntry{Idx: 1, Times: 2})
	h.Insert(emitEntry{Idx: 2, Times: 3})

	h.Delete(0)
	if h.Len() != 2 {
		t.Fatalf("Len = %d, want 2 after delete", h.Len())
	}

	ec, _ := h.Top()
	if ec.Idx != 1 {
		t.Fatalf("Top.Idx = %d, want 1 after deleting 0", ec.Idx)
	}
}

func TestChunkHeap_DeleteMissing(t *testing.T) {
	h := newEmitPlanner()
	h.Insert(emitEntry{Idx: 0, Times: 1})
	// Should not panic.
	h.Delete(99)
	if h.Len() != 1 {
		t.Fatalf("Len = %d, want 1", h.Len())
	}
}

func TestChunkHeap_InsertUpdate(t *testing.T) {
	h := newEmitPlanner()
	h.Insert(emitEntry{Idx: 0, Times: 5})
	h.Insert(emitEntry{Idx: 1, Times: 3})

	// Re-insert idx=0 with lower count; should become new top.
	h.Insert(emitEntry{Idx: 0, Times: 1})

	if h.Len() != 2 {
		t.Fatalf("Len = %d, want 2 (insert should update, not duplicate)", h.Len())
	}
	ec, _ := h.Top()
	if ec.Idx != 0 || ec.Times != 1 {
		t.Fatalf("Top = {%d, %d}, want {0, 1}", ec.Idx, ec.Times)
	}
}
