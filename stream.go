package ethp2p

import (
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
//   - Read, Write, Close return their error and never cancel a side. Write
//     still returns io.ErrShortWrite for a short write.
//   - Deadlines pass straight through; a timed-out stream can have its
//     deadline extended and be used again.
//   - CancelRead/CancelWrite send code.Wire() for any code, stack or
//     protocol, without rewriting. First call wins; later calls do nothing.
//   - A peer reset is returned as a *ResetError holding wire.ParseCode(raw)
//     with the transport error as its cause.

// SendStream is the writable side of a protocol's unidirectional QUIC stream.
// See the rules above for how it ends.
type SendStream interface {
	// Write writes protocol data to the stream.
	Write([]byte) (int, error)
	// Close sends FIN after all data written to the stream.
	Close() error
	// CancelWrite aborts the write side with an outcome code.
	CancelWrite(wire.Code)
	// SetWriteDeadline sets or clears the write deadline.
	SetWriteDeadline(time.Time) error
}

// ReceiveStream is the readable side of a protocol's unidirectional QUIC
// stream. See the rules above for how it ends.
type ReceiveStream interface {
	// Read reads protocol data. A remote reset is returned as a [ResetError].
	Read([]byte) (int, error)
	// CancelRead stops reading with an outcome code.
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
	// CancelRead stops reading with an outcome code.
	CancelRead(wire.Code)
	// CancelWrite aborts the write side with an outcome code.
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
	stream      transport.SendStream
	cancelWrite sync.Once
}

func (s *sendStream) Write(p []byte) (int, error) {
	n, err := s.stream.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

func (s *sendStream) Close() error {
	return s.stream.Close()
}

func (s *sendStream) CancelWrite(code wire.Code) {
	s.cancelWrite.Do(func() { s.stream.CancelWrite(code.Wire()) })
}

func (s *sendStream) SetWriteDeadline(deadline time.Time) error {
	return s.stream.SetWriteDeadline(deadline)
}

type receiveStream struct {
	stream     transport.ReceiveStream
	cancelRead sync.Once
}

func (s *receiveStream) Read(p []byte) (int, error) {
	n, err := s.stream.Read(p)
	return n, resetError(err)
}

func (s *receiveStream) CancelRead(code wire.Code) {
	s.cancelRead.Do(func() { s.stream.CancelRead(code.Wire()) })
}

func (s *receiveStream) SetReadDeadline(deadline time.Time) error {
	return s.stream.SetReadDeadline(deadline)
}

type bidirectionalStream struct {
	stream      transport.Stream
	cancelRead  sync.Once
	cancelWrite sync.Once
}

func (s *bidirectionalStream) Read(p []byte) (int, error) {
	n, err := s.stream.Read(p)
	return n, resetError(err)
}

func (s *bidirectionalStream) Write(p []byte) (int, error) {
	n, err := s.stream.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

func (s *bidirectionalStream) Close() error {
	return s.stream.Close()
}

func (s *bidirectionalStream) CancelRead(code wire.Code) {
	s.cancelRead.Do(func() { s.stream.CancelRead(code.Wire()) })
}

func (s *bidirectionalStream) CancelWrite(code wire.Code) {
	s.cancelWrite.Do(func() { s.stream.CancelWrite(code.Wire()) })
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

func wrapSendStream(stream transport.SendStream) SendStream {
	return &sendStream{stream: stream}
}

func wrapBidirectionalStream(stream transport.Stream) Stream {
	return &bidirectionalStream{stream: stream}
}

func wrapReceiveStream(stream transport.ReceiveStream) ReceiveStream {
	if bidirectional, ok := stream.(transport.Stream); ok {
		return wrapBidirectionalStream(bidirectional)
	}
	return &receiveStream{stream: stream}
}

func resetError(err error) error {
	reset, ok := errors.AsType[*transport.StreamResetError](err)
	if !ok {
		return err
	}
	return &ResetError{Code: wire.ParseCode(reset.Code), cause: err}
}

var (
	_ SendStream    = (*sendStream)(nil)
	_ ReceiveStream = (*receiveStream)(nil)
	_ Stream        = (*bidirectionalStream)(nil)
)
