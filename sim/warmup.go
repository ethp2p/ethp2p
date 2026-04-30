package sim

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func runWarmupInitiator(s io.ReadWriteCloser, nodeNum int, byteCount int) error {
	if byteCount < 0 {
		byteCount = 0
	}
	if err := binary.Write(s, binary.BigEndian, uint64(nodeNum)); err != nil {
		return err
	}
	if err := binary.Write(s, binary.BigEndian, uint64(byteCount)); err != nil {
		return err
	}
	var peerBytes uint64
	if err := binary.Read(s, binary.BigEndian, &peerBytes); err != nil {
		return err
	}
	return exchangeWarmupBytes(s, byteCount, int(peerBytes))
}

func runWarmupResponder(s io.ReadWriteCloser) (int, error) {
	var peerNode uint64
	if err := binary.Read(s, binary.BigEndian, &peerNode); err != nil {
		return 0, err
	}
	var byteCount uint64
	if err := binary.Read(s, binary.BigEndian, &byteCount); err != nil {
		return 0, err
	}
	if err := binary.Write(s, binary.BigEndian, byteCount); err != nil {
		return 0, err
	}
	return int(peerNode), exchangeWarmupBytes(s, int(byteCount), int(byteCount))
}

func exchangeWarmupBytes(s io.ReadWriteCloser, writeBytes, readBytes int) error {
	errCh := make(chan error, 2)
	go func() {
		_, err := io.CopyN(s, zeroReader{}, int64(writeBytes))
		errCh <- err
	}()
	go func() {
		_, err := io.CopyN(io.Discard, s, int64(readBytes))
		errCh <- err
	}()
	return errors.Join(<-errCh, <-errCh)
}

type warmupTracker struct {
	mu   sync.Mutex
	done map[int]struct{}
}

func (t *warmupTracker) mark(peer int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done == nil {
		t.done = make(map[int]struct{})
	}
	t.done[peer] = struct{}{}
}

func (t *warmupTracker) awaitIncoming(ctx context.Context, nodeNum int, peers []int) error {
	expected := make(map[int]struct{})
	for _, p := range peers {
		if p < nodeNum {
			expected[p] = struct{}{}
		}
	}
	if len(expected) == 0 {
		return nil
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if t.hasIncoming(expected) {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (t *warmupTracker) hasIncoming(expected map[int]struct{}) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for p := range expected {
		if _, ok := t.done[p]; !ok {
			return false
		}
	}
	return true
}

func waitContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
