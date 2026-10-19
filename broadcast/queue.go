package broadcast

import (
	"container/list"
	"sync"
)

// fifo transfers ownership without blocking its producer. The owner supplies
// the domain bound, as with unread QUIC streams.
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
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	var pending []T
	for q.items.Len() > 0 {
		pending = append(pending, q.items.Remove(q.items.Front()).(T))
	}
	return pending
}
