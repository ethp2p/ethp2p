package transport

import (
	"sync"
)

// streamQueue retains accepted streams until delivery. Unfinished streams keep
// their QUIC credit, so the transport need not impose another capacity limit.
type streamQueue[T any] struct {
	mu     sync.Mutex
	items  []T
	closed bool
	ready  chan struct{}
}

func newStreamQueue[T any]() *streamQueue[T] {
	return &streamQueue[T]{ready: make(chan struct{}, 1)}
}

func (q *streamQueue[T]) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *streamQueue[T]) push(item T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.items = append(q.items, item)
	q.signal()
	return true
}

// tryPop returns the oldest queued item. It never blocks; it reports false
// when the queue is empty. A non-empty queue stays signaled so a consumer
// waiting on the wake channel observes every item.
func (q *streamQueue[T]) tryPop() (T, bool) {
	var zero T
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return zero, false
	}
	item := q.items[0]
	q.items[0] = zero
	q.items = q.items[1:]
	if len(q.items) != 0 {
		q.signal()
	} else {
		q.items = nil
	}
	return item, true
}

// close transfers ownership of queued streams to the caller for cancellation.
// A concurrent push either joins this batch or observes closed and cancels its
// own stream. Cancellation never runs under the queue mutex.
func (q *streamQueue[T]) close() []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	items := q.items
	q.items = nil
	return items
}
