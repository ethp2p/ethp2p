package transport

import (
	"context"
	"io"
	"time"

	varint "github.com/multiformats/go-varint"
	"github.com/quic-go/quic-go"
)

const (
	classifyTimeout       = 5 * time.Second
	varintContinuationBit = byte(0x80)
)

type peekableStream interface {
	io.Reader
	Peek([]byte) (int, error)
	CancelRead(quic.StreamErrorCode)
	SetReadDeadline(time.Time) error
}

// classify identifies the destination from the first frame's length prefix and
// first payload byte without consuming bytes. '/' routes to libp2p; any other
// byte routes to ethp2p. It does not validate the full frame.
func classify[S peekableStream](ctx context.Context, stream S) (ethp2p bool, err error) {
	stop := context.AfterFunc(ctx, func() { stream.CancelRead(streamReset) })
	defer stop()

	if err := stream.SetReadDeadline(time.Now().Add(classifyTimeout)); err != nil {
		return false, err
	}
	defer func() {
		if clearErr := stream.SetReadDeadline(time.Time{}); err == nil {
			err = clearErr
		}
	}()

	peekExact := func(dst []byte) error {
		n, err := stream.Peek(dst)
		if n == len(dst) {
			return nil
		}
		if err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}

	var buf [varint.MaxLenUvarint63 + 1]byte
	for prefixLen := 1; prefixLen <= varint.MaxLenUvarint63; prefixLen++ {
		if err := peekExact(buf[:prefixLen]); err != nil {
			return false, err
		}
		if buf[prefixLen-1]&varintContinuationBit != 0 {
			continue
		}

		payloadLen, _, err := varint.FromUvarint(buf[:prefixLen])
		if err != nil {
			return false, err
		}
		if payloadLen == 0 {
			return false, errEmptyFirstFrame
		}
		if err := peekExact(buf[:prefixLen+1]); err != nil {
			return false, err
		}
		return buf[prefixLen] != '/', nil
	}
	return false, errInvalidFrame
}
