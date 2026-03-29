package protocol

import (
	"fmt"
	"io"

	pb "github.com/ethp2p/ethp2p/protocol/pb"
)

var ErrUnspecified = fmt.Errorf("unspecified protocol")

// WriteSelector writes the protocol selector as a single byte.
func WriteSelector(w io.Writer, p pb.Protocol) error {
	var data [1]byte
	data[0] = byte(p)
	_, err := w.Write(data[:])
	return err
}

// ReadSelector reads the protocol selector from a single byte. Returns an
// error if the protocol is PROTOCOL_UNSPECIFIED.
func ReadSelector(r io.Reader) (pb.Protocol, error) {
	var data [1]byte
	if _, err := io.ReadFull(r, data[:]); err != nil {
		return 0, err
	}
	p := pb.Protocol(data[0])
	if p == pb.Protocol_PROTOCOL_UNSPECIFIED {
		return 0, ErrUnspecified
	}
	return p, nil
}
