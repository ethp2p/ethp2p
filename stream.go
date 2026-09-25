package ethp2p

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/ethp2p/ethp2p/transport"
)

// Protocol streams end with a code, under these rules:
//
//   - Cancelling a side is idempotent. The first code is the one sent; later
//     calls have no effect.
//   - A failed Read, Write, or Close cancels its side before returning, with
//     wire.Timeout if a deadline expired and wire.Unspecified
//     otherwise. A caller's later cancel therefore has no effect, so callers
//     may cancel with their own code after any error: it takes effect only
//     when no I/O failed, for example on a malformed frame.
//   - Deadline expiry is terminal, unlike Go's usual deadlines, which can be
//     extended to resume I/O.
//   - A peer reset ends the read side. Read returns it as a [ResetError], and
//     the stream sends nothing in reply.

// SendStream is the writable side of a protocol's unidirectional QUIC stream.
// See the rules above for how it ends.
type SendStream interface {
	// Write writes protocol data to the stream.
	Write([]byte) (int, error)
	// Close sends FIN after all data written to the stream.
	Close() error
	// CancelWrite aborts the write side with a selector-scoped outcome.
	CancelWrite(wire.Code)
	// SetWriteDeadline sets or clears the write deadline.
	SetWriteDeadline(time.Time) error
}

// ReceiveStream is the readable side of a protocol's unidirectional QUIC
// stream. See the rules above for how it ends.
type ReceiveStream interface {
	// Read reads protocol data. A remote reset is returned as a [ResetError].
	Read([]byte) (int, error)
	// CancelRead stops reading with a selector-scoped outcome.
	CancelRead(wire.Code)
	// SetReadDeadline sets or clears the read deadline.
	SetReadDeadline(time.Time) error
}

// Stream is a bidirectional protocol stream. Close sends FIN and leaves the
// read side open. See the rules above for how each side ends.
type Stream interface {
	// Read reads protocol data. A remote reset is returned as a [ResetError].
	Read([]byte) (int, error)
	// Write writes protocol data to the stream.
	Write([]byte) (int, error)
	// Close sends FIN after all data written to the stream.
	Close() error
	// CancelRead stops reading with a selector-scoped outcome.
	CancelRead(wire.Code)
	// CancelWrite aborts the write side with a selector-scoped outcome.
	CancelWrite(wire.Code)
	// SetDeadline sets or clears both the read and write deadlines.
	SetDeadline(time.Time) error
	// SetReadDeadline sets or clears the read deadline.
	SetReadDeadline(time.Time) error
	// SetWriteDeadline sets or clears the write deadline.
	SetWriteDeadline(time.Time) error
}

// ResetError reports the code received when a peer reset a stream's read side.
type ResetError struct {
	// Code is the decoded stack or protocol outcome.
	Code  wire.Code
	cause error
}

// Error describes the stream reset and its original transport error.
func (e *ResetError) Error() string {
	if e.cause == nil {
		return fmt.Sprintf("ethp2p stream reset with %s", e.Code)
	}
	return fmt.Sprintf("ethp2p stream reset with %s: %v", e.Code, e.cause)
}

// Unwrap returns the transport reset error that produced e.
func (e *ResetError) Unwrap() error { return e.cause }

type sendStream struct {
	selector    wire.Selector
	stream      transport.SendStream
	cancelWrite sync.Once
}

func (s *sendStream) Write(p []byte) (int, error) {
	n, err := s.stream.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.CancelWrite(streamFailureCode(context.Background(), err))
	}
	return n, err
}

func (s *sendStream) Close() error {
	err := s.stream.Close()
	if err != nil {
		s.CancelWrite(streamFailureCode(context.Background(), err))
	}
	return err
}

func (s *sendStream) CancelWrite(code wire.Code) {
	raw := code.WireFor(s.selector)
	s.cancelWrite.Do(func() { s.stream.CancelWrite(raw) })
}

func (s *sendStream) SetWriteDeadline(deadline time.Time) error {
	return s.stream.SetWriteDeadline(deadline)
}

type receiveStream struct {
	selector   wire.Selector
	stream     transport.ReceiveStream
	cancelRead sync.Once
}

func (s *receiveStream) Read(p []byte) (int, error) {
	n, err := s.stream.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		if _, reset := errors.AsType[*transport.StreamResetError](err); !reset {
			s.CancelRead(streamFailureCode(context.Background(), err))
		}
	}
	return n, resetError(err, s.selector)
}

func (s *receiveStream) CancelRead(code wire.Code) {
	raw := code.WireFor(s.selector)
	s.cancelRead.Do(func() { s.stream.CancelRead(raw) })
}

func (s *receiveStream) SetReadDeadline(deadline time.Time) error {
	return s.stream.SetReadDeadline(deadline)
}

type bidirectionalStream struct {
	selector    wire.Selector
	stream      transport.Stream
	cancelRead  sync.Once
	cancelWrite sync.Once
}

func (s *bidirectionalStream) Read(p []byte) (int, error) {
	n, err := s.stream.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		if _, reset := errors.AsType[*transport.StreamResetError](err); !reset {
			s.CancelRead(streamFailureCode(context.Background(), err))
		}
	}
	return n, resetError(err, s.selector)
}

func (s *bidirectionalStream) Write(p []byte) (int, error) {
	n, err := s.stream.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.CancelWrite(streamFailureCode(context.Background(), err))
	}
	return n, err
}

func (s *bidirectionalStream) Close() error {
	err := s.stream.Close()
	if err != nil {
		s.CancelWrite(streamFailureCode(context.Background(), err))
	}
	return err
}

func (s *bidirectionalStream) CancelRead(code wire.Code) {
	raw := code.WireFor(s.selector)
	s.cancelRead.Do(func() { s.stream.CancelRead(raw) })
}

func (s *bidirectionalStream) CancelWrite(code wire.Code) {
	raw := code.WireFor(s.selector)
	s.cancelWrite.Do(func() { s.stream.CancelWrite(raw) })
}

func (s *bidirectionalStream) SetDeadline(deadline time.Time) error {
	return s.stream.SetDeadline(deadline)
}

func (s *bidirectionalStream) SetReadDeadline(deadline time.Time) error {
	return s.stream.SetReadDeadline(deadline)
}

func (s *bidirectionalStream) SetWriteDeadline(deadline time.Time) error {
	return s.stream.SetWriteDeadline(deadline)
}

func wrapSendStream(selector wire.Selector, stream transport.SendStream) SendStream {
	return &sendStream{selector: selector, stream: stream}
}

func wrapBidirectionalStream(selector wire.Selector, stream transport.Stream) Stream {
	return &bidirectionalStream{selector: selector, stream: stream}
}

func wrapReceiveStream(selector wire.Selector, stream transport.ReceiveStream) ReceiveStream {
	if bidirectional, ok := stream.(transport.Stream); ok {
		return wrapBidirectionalStream(selector, bidirectional)
	}
	return &receiveStream{selector: selector, stream: stream}
}

func resetError(err error, selector wire.Selector) error {
	reset, ok := errors.AsType[*transport.StreamResetError](err)
	if !ok {
		return err
	}
	return &ResetError{Code: wire.ParseCode(selector, reset.Code), cause: err}
}

var (
	_ SendStream    = (*sendStream)(nil)
	_ ReceiveStream = (*receiveStream)(nil)
	_ Stream        = (*bidirectionalStream)(nil)
)
