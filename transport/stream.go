package transport

import (
	"errors"
	"time"

	"github.com/quic-go/quic-go"
)

var (
	_ Stream        = stream{}
	_ SendStream    = sendStream{}
	_ ReceiveStream = (*receiveStream)(nil)
)

// SendStream is the writable side of a locally opened unidirectional QUIC
// stream.
type SendStream interface {
	// Write implements io.Writer.
	Write([]byte) (int, error)
	// Close sends the stream FIN after all written data.
	Close() error
	// CancelWrite aborts the stream with the given QUIC application error code.
	CancelWrite(uint64)
	// SetWriteDeadline sets the deadline for future writes. A zero value clears
	// the deadline.
	SetWriteDeadline(time.Time) error
}

// ReceiveStream is the readable side of a remotely opened unidirectional QUIC
// stream.
type ReceiveStream interface {
	// Read implements io.Reader. A QUIC stream cancellation returns a
	// *StreamResetError.
	Read([]byte) (int, error)
	// CancelRead stops reading and sends the given QUIC application error code to
	// the peer.
	CancelRead(uint64)
	// SetReadDeadline sets the deadline for future reads. A zero value clears the
	// deadline.
	SetReadDeadline(time.Time) error
}

// Stream is a bidirectional QUIC stream routed to ethp2p.
type Stream interface {
	// Read implements io.Reader. A QUIC stream cancellation returns a
	// *StreamResetError.
	Read([]byte) (int, error)
	// Write implements io.Writer.
	Write([]byte) (int, error)
	// Close sends the stream FIN after all written data. The read side remains
	// open.
	Close() error
	// CancelRead stops reading and sends the given QUIC application error code to
	// the peer.
	CancelRead(uint64)
	// CancelWrite aborts writing with the given QUIC application error code.
	CancelWrite(uint64)
	// SetDeadline sets the deadline for future reads and writes. A zero value
	// clears both deadlines.
	SetDeadline(time.Time) error
	// SetReadDeadline sets the deadline for future reads. A zero value clears the
	// deadline.
	SetReadDeadline(time.Time) error
	// SetWriteDeadline sets the deadline for future writes. A zero value clears
	// the deadline.
	SetWriteDeadline(time.Time) error
}

// stream adapts quic.Stream to Stream without exposing quic.StreamErrorCode in
// the transport API.
type stream struct{ *quic.Stream }

// Read translates QUIC stream cancellations into [StreamResetError].
func (s stream) Read(p []byte) (int, error) {
	n, err := s.Stream.Read(p)
	return n, readError(err)
}

func (s stream) CancelRead(code uint64) { s.Stream.CancelRead(quic.StreamErrorCode(code)) }

func (s stream) CancelWrite(code uint64) { s.Stream.CancelWrite(quic.StreamErrorCode(code)) }

// sendStream adapts quic.SendStream to SendStream.
type sendStream struct{ *quic.SendStream }

func (s sendStream) CancelWrite(code uint64) { s.SendStream.CancelWrite(quic.StreamErrorCode(code)) }

// receiveStream defers consuming the peeked selector until protocol delivery.
// Even a selector-only stream retains credit while queued. skip survives a
// partial read interrupted by a deadline so a later read can resume it.
type receiveStream struct {
	*quic.ReceiveStream
	skip int
}

// Read translates QUIC stream cancellations into [StreamResetError] so callers
// do not depend on quic-go error types.
func (s *receiveStream) Read(p []byte) (int, error) {
	var scratch [11]byte
	for s.skip > 0 {
		n, err := s.ReceiveStream.Read(scratch[:s.skip])
		s.skip -= n
		if err != nil {
			return 0, readError(err)
		}
	}
	n, err := s.ReceiveStream.Read(p)
	return n, readError(err)
}

func readError(err error) error {
	if streamErr, ok := errors.AsType[*quic.StreamError](err); ok {
		return &StreamResetError{Code: uint64(streamErr.ErrorCode)}
	}
	return err
}

func (s *receiveStream) CancelRead(code uint64) {
	s.ReceiveStream.CancelRead(quic.StreamErrorCode(code))
}
