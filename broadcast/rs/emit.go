package rs

import "container/heap"

// emitEntry tracks allocation and send state for a single chunk.
// Times counts allocations (used for fair heap ordering: least-allocated
// first). Sent counts confirmed successful sends (used for budget
// enforcement against ForwardMultiplier). Priority breaks ties between
// chunks with equal Times via a per-relay random seed.
type emitEntry struct {
	Idx      int
	Times    int
	Sent     int
	Priority uint32
}

// emitPlanner is a min-heap of emitEntry ordered by Count. It provides
// O(log n) access to the least-emitted chunk so the strategy can
// distribute chunks fairly: chunks sent fewer times are allocated first.
type emitPlanner struct {
	idToIndex map[int]int
	entries   []emitEntry
}

func newEmitPlanner() *emitPlanner {
	return &emitPlanner{
		idToIndex: make(map[int]int),
	}
}

func (h *emitPlanner) Len() int { return len(h.entries) }
func (h *emitPlanner) Less(i, j int) bool {
	a, b := h.entries[i], h.entries[j]
	if a.Times != b.Times {
		return a.Times < b.Times
	}
	return a.Priority < b.Priority
}

func (h *emitPlanner) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
	h.idToIndex[h.entries[i].Idx] = i
	h.idToIndex[h.entries[j].Idx] = j
}

func (h *emitPlanner) Push(x any) {
	ec := x.(emitEntry)
	h.idToIndex[ec.Idx] = len(h.entries)
	h.entries = append(h.entries, ec)
}

func (h *emitPlanner) Pop() any {
	n := len(h.entries)
	ec := h.entries[n-1]
	h.entries[n-1] = emitEntry{}
	h.entries = h.entries[:n-1]
	delete(h.idToIndex, ec.Idx)
	return ec
}

// Insert adds a new entry or updates an existing one.
func (h *emitPlanner) Insert(ec emitEntry) {
	if idx, ok := h.idToIndex[ec.Idx]; ok {
		h.entries[idx] = ec
		heap.Fix(h, idx)
		return
	}
	heap.Push(h, ec)
}

// Top returns the entry with the lowest Count without removing it.
// Returns zero value and false if the heap is empty.
func (h *emitPlanner) Top() (emitEntry, bool) {
	if len(h.entries) == 0 {
		return emitEntry{}, false
	}
	return h.entries[0], true
}

// PopFront removes and returns the entry with the lowest Count.
func (h *emitPlanner) PopFront() emitEntry {
	return heap.Pop(h).(emitEntry)
}

// Increment increases the Count of the entry with the given ID by one.
func (h *emitPlanner) Increment(id int) {
	idx, ok := h.idToIndex[id]
	if !ok {
		return
	}
	h.entries[idx].Times++
	heap.Fix(h, idx)
}

// Contains reports whether the heap has an entry with the given ID.
func (h *emitPlanner) Contains(id int) bool {
	_, ok := h.idToIndex[id]
	return ok
}

// Delete removes the entry with the given ID.
func (h *emitPlanner) Delete(id int) {
	idx, ok := h.idToIndex[id]
	if !ok {
		return
	}
	heap.Remove(h, idx)
}

// GetSent returns the Sent count for the given chunk ID.
// Returns 0, false if the entry does not exist.
func (h *emitPlanner) GetSent(id int) (int, bool) {
	idx, ok := h.idToIndex[id]
	if !ok {
		return 0, false
	}
	return h.entries[idx].Sent, true
}

// AddSent adjusts the Sent count for the given chunk ID by delta.
// Sent does not affect heap ordering, so no reordering is performed.
func (h *emitPlanner) AddSent(id int, delta int) {
	idx, ok := h.idToIndex[id]
	if !ok {
		return
	}
	h.entries[idx].Sent += delta
}
