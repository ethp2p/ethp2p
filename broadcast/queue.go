package broadcast

import (
	"container/list"
	"sync"
)

// fifo transfers ownership without blocking its producer. The owner supplies
// the domain bound: unread QUIC streams, or lifecycle entries for live sessions.
// Closing atomically rejects later pushes and returns everything still owned.
type fifo[T any] struct {
	mu     sync.Mutex
	items  list.List
	ready  chan struct{}
	closed bool
}

func newFIFO[T any]() *fifo[T] { return &fifo[T]{ready: make(chan struct{}, 1)} }

func (q *fifo[T]) push(v T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.items.PushBack(v)
	q.signal()
	return true
}

func (q *fifo[T]) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *fifo[T]) pop() (v T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	front := q.items.Front()
	if front == nil {
		return v, false
	}
	v = q.items.Remove(front).(T)
	if q.items.Len() > 0 {
		q.signal()
	}
	return v, true
}

func (q *fifo[T]) close() []T {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	var pending []T
	for q.items.Len() > 0 {
		pending = append(pending, q.items.Remove(q.items.Front()).(T))
	}
	return pending
}

// enqueueLifecycle preserves order and elides an unopened session when its
// final close overtakes it in the queue. A close-stream alone must keep the
// open: the session's outbound chunk slot is still needed.
func (p *PeerConn) enqueueLifecycle(event peerCtrlEvent) {
	q := p.lifecycle
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	if end, ok := event.(peerCloseSession); ok {
		for item := q.items.Back(); item != nil; item = item.Prev() {
			if open, ok := item.Value.(peerOpenSession); ok && open.channelID == end.channelID && open.messageID == end.messageID {
				for next := item.Next(); next != nil; {
					after := next.Next()
					if batch, ok := next.Value.(*peerClosures); ok {
						kept := batch.events[:0]
						for _, ev := range batch.events {
							if c, ok := ev.(peerCloseStream); ok && c.channelID == end.channelID && c.messageID == end.messageID {
								continue
							}
							kept = append(kept, ev)
						}
						clear(batch.events[len(kept):])
						batch.events = kept
						if len(kept) == 0 {
							q.items.Remove(next)
						}
					}
					next = after
				}
				q.items.Remove(item)
				// Elision can make close batches adjacent; join without reordering.
				for n := q.items.Front(); n != nil; {
					next := n.Next()
					a, ok := n.Value.(*peerClosures)
					if ok && next != nil {
						if b, ok := next.Value.(*peerClosures); ok {
							a.events = append(a.events, b.events...)
							q.items.Remove(next)
							continue
						}
					}
					n = next
				}
				return
			}
		}
	}
	switch event.(type) {
	case peerCloseSession, peerCloseStream:
		if last := q.items.Back(); last != nil {
			if batch, ok := last.Value.(*peerClosures); ok {
				batch.events = append(batch.events, event)
				q.signal()
				return
			}
		}
		event = &peerClosures{events: []peerCtrlEvent{event}}
	}
	q.items.PushBack(event)
	q.signal()
}
