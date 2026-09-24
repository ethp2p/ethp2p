package transporttest

import (
	"io"
	"sync"
	"time"
)

// RawSendStream records numeric transport cancellation codes for wrapper tests.
type RawSendStream struct {
	WriteErr error
	CloseErr error

	mu               sync.Mutex
	writes           [][]byte
	cancelWriteCodes []uint64
}

// Write records a copy of p and returns the configured result.
func (s *RawSendStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.writes = append(s.writes, append([]byte(nil), p...))
	err := s.WriteErr
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close returns the configured close error.
func (s *RawSendStream) Close() error { return s.CloseErr }

// CancelWrite records the numeric transport code.
func (s *RawSendStream) CancelWrite(code uint64) {
	s.mu.Lock()
	s.cancelWriteCodes = append(s.cancelWriteCodes, code)
	s.mu.Unlock()
}

// SetWriteDeadline satisfies the raw transport stream interface.
func (*RawSendStream) SetWriteDeadline(time.Time) error { return nil }

// CancelWriteCodes returns every numeric code recorded so far.
func (s *RawSendStream) CancelWriteCodes() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.cancelWriteCodes...)
}

// RawReceiveStream records numeric transport cancellation codes for wrapper tests.
type RawReceiveStream struct {
	Reader    io.Reader
	ReadErr   error
	ReadStart chan struct{}
	ReadBlock chan struct{}

	mu              sync.Mutex
	cancelReadCodes []uint64
	cancelReadOnce  sync.Once
}

// Read returns ReadErr, waits for a cancellation when ReadBlock is set, or reads
// from Reader. ReadStart is notified before waiting.
func (s *RawReceiveStream) Read(p []byte) (int, error) {
	s.mu.Lock()
	err := s.ReadErr
	started := s.ReadStart
	block := s.ReadBlock
	reader := s.Reader
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if block != nil {
		if started != nil {
			select {
			case started <- struct{}{}:
			default:
			}
		}
		<-block
		return 0, io.ErrClosedPipe
	}
	if reader == nil {
		return 0, io.EOF
	}
	return reader.Read(p)
}

// CancelRead records the numeric transport code and releases a waiting Read.
func (s *RawReceiveStream) CancelRead(code uint64) {
	s.mu.Lock()
	s.cancelReadCodes = append(s.cancelReadCodes, code)
	block := s.ReadBlock
	s.mu.Unlock()
	if block != nil {
		s.cancelReadOnce.Do(func() { close(block) })
	}
}

// SetReadDeadline satisfies the raw transport stream interface.
func (*RawReceiveStream) SetReadDeadline(time.Time) error { return nil }

// CancelReadCodes returns every numeric code recorded so far.
func (s *RawReceiveStream) CancelReadCodes() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.cancelReadCodes...)
}
