package ethp2p

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethp2p/ethp2p/wire"
	"github.com/quic-go/quic-go"
)

// Protocol streams end with a code, under these rules:
//
//   - Read, Write, Close return their error and never cancel a side.
//   - Deadlines pass straight through; a timed-out stream can have its
//     deadline extended and be used again.
//   - CancelRead/CancelWrite send code.Wire() for any code, stack or
//     protocol, without rewriting. First call wins; later calls do nothing
//     (quic-go ignores repeated cancels).
//   - A peer reset is returned as a *ResetError holding wire.ParseCode(raw)
//     with the original quic-go error as its cause.

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
	return fmt.Sprintf("ethp2p stream reset with %s: %v", e.Code, e.cause)
}

// Unwrap returns the quic-go stream reset error that produced e.
func (e *ResetError) Unwrap() error { return e.cause }

// receivedStream is an incoming QUIC stream as transport hands it over, before
// a protocol owns it: a *quic.ReceiveStream, or a *quic.Stream whose send side
// the stack ends too when it refuses the stream (see cancelReceived).
type receivedStream interface {
	Read([]byte) (int, error)
	CancelRead(quic.StreamErrorCode)
	SetReadDeadline(time.Time) error
}

func streamCode(code wire.Code) quic.StreamErrorCode { return quic.StreamErrorCode(code.Wire()) }

type sendStream struct {
	stream *quic.SendStream
}

func (s *sendStream) Write(p []byte) (int, error) {
	return s.stream.Write(p)
}

func (s *sendStream) Close() error {
	return s.stream.Close()
}

func (s *sendStream) CancelWrite(code wire.Code) {
	s.stream.CancelWrite(streamCode(code))
}

func (s *sendStream) SetWriteDeadline(deadline time.Time) error {
	return s.stream.SetWriteDeadline(deadline)
}

type receiveStream struct {
	stream receivedStream
}

func (s *receiveStream) Read(p []byte) (int, error) {
	n, err := s.stream.Read(p)
	return n, resetError(err)
}

func (s *receiveStream) CancelRead(code wire.Code) {
	s.stream.CancelRead(streamCode(code))
}

func (s *receiveStream) SetReadDeadline(deadline time.Time) error {
	return s.stream.SetReadDeadline(deadline)
}

type bidirectionalStream struct {
	*quic.Stream
}

func (s *bidirectionalStream) Read(p []byte) (int, error) {
	n, err := s.Stream.Read(p)
	return n, resetError(err)
}

func (s *bidirectionalStream) CancelRead(code wire.Code) {
	s.Stream.CancelRead(streamCode(code))
}

func (s *bidirectionalStream) CancelWrite(code wire.Code) {
	s.Stream.CancelWrite(streamCode(code))
}

func (s *bidirectionalStream) SetDeadline(deadline time.Time) error {
	return errors.Join(s.SetReadDeadline(deadline), s.SetWriteDeadline(deadline))
}

func wrapSendStream(stream *quic.SendStream) SendStream {
	return &sendStream{stream: stream}
}

func wrapBidirectionalStream(stream *quic.Stream) Stream {
	return &bidirectionalStream{Stream: stream}
}

func wrapReceiveStream(stream receivedStream) ReceiveStream {
	if bidirectional, ok := stream.(*quic.Stream); ok {
		return wrapBidirectionalStream(bidirectional)
	}
	return &receiveStream{stream: stream}
}

func resetError(err error) error {
	reset, ok := errors.AsType[*quic.StreamError](err)
	if !ok {
		return err
	}
	return &ResetError{Code: wire.ParseCode(uint64(reset.ErrorCode)), cause: err}
}
