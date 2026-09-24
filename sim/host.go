// package sim is a network simulation harness for broadcast strategies. It
// drives the production ethp2p stack rather than a private transport, so
// measured behaviour reflects the real protocol.
package sim

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"

	"github.com/ethp2p/ethp2p/transport"
)

// nodeIdentity returns the transport identity for a simulation node. Deriving it
// from the node number keeps experiments reproducible and lets a run predict
// every peer ID before any node starts, which the trace header needs.
//
// Outside simulation an identity is generated randomly and kept secret; this
// determinism is a property of the harness, not of ethp2p.
func nodeIdentity(nodeNum int) (*transport.PrivKey, transport.PeerID, error) {
	var seed [32]byte
	binary.BigEndian.PutUint64(seed[:], uint64(nodeNum))
	rng := rand.NewChaCha8(seed)
	secret := make([]byte, 32)
	for i := 0; i < len(secret); i += 8 {
		binary.BigEndian.PutUint64(secret[i:], rng.Uint64())
	}
	key, err := transport.PrivKeyFromBytes(secret)
	if err != nil {
		return nil, "", fmt.Errorf("derive identity for node %d: %w", nodeNum, err)
	}
	return key, key.Public().PeerID(), nil
}

// newNodeEndpoint builds the shared QUIC endpoint for a simulation node over the
// packet connection supplied by the driver. The profile disables path MTU
// discovery, which Shadow requires, and raises the stream limits for large
// topologies.
//
// The caller owns the returned packet connection and the endpoint's shutdown.
func newNodeEndpoint(nodeNum int, packetConn net.PacketConn) (
	eth *transport.Ethp2pTransport,
	shared *transport.SharedTransport,
	peerID transport.PeerID,
	err error,
) {
	key, peerID, err := nodeIdentity(nodeNum)
	if err != nil {
		return nil, nil, "", err
	}
	shared, err = transport.NewShared(key, packetConn, transport.Shadow())
	if err != nil {
		return nil, nil, "", fmt.Errorf("create shared transport: %w", err)
	}
	return shared.Ethp2p(), shared, peerID, nil
}
