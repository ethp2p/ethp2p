// package sim is a network simulation harness for broadcast strategies. It
// drives the production ethp2p stack rather than a private transport, so
// measured behaviour reflects the real protocol.
package sim

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"

	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/transport"
)

// nodeRecord derives the simulation identity and advertises its bound endpoint.
// Shadow processes can reconstruct a peer's record from its node number.
func nodeRecord(nodeNum int, addr net.Addr) (*enr.Record, error) {
	key, _, err := nodeIdentity(nodeNum)
	if err != nil {
		return nil, err
	}
	endpoint, err := netip.ParseAddrPort(addr.String())
	if err != nil {
		return nil, err
	}
	return enr.Sign(key, 1, enr.EndpointEntries(endpoint)...)
}

// nodeIdentity returns the transport identity for a simulation node. Deriving it
// from the node number keeps experiments reproducible and lets a run predict
// every peer ID before any node starts, which the trace header needs.
//
// Outside simulation an identity is generated randomly and kept secret; this
// determinism is a property of the harness, not of ethp2p.
func nodeIdentity(nodeNum int) (*identity.PrivKey, identity.PeerID, error) {
	var seed [32]byte
	binary.BigEndian.PutUint64(seed[:], uint64(nodeNum))
	secret := make([]byte, 32)
	_, _ = rand.NewChaCha8(seed).Read(secret) // ChaCha8.Read never fails.
	key, err := identity.ParsePrivKey(secret)
	if err != nil {
		return nil, "", fmt.Errorf("derive identity for node %d: %w", nodeNum, err)
	}
	return key, key.Public().PeerID(), nil
}

// newNodeEndpoint builds the shared QUIC endpoint for a simulation node over the
// packet connection supplied by the driver. The config disables path MTU
// discovery, which Shadow requires, and raises the stream limits for large
// topologies.
//
// The caller owns the packet connection and the endpoint's shutdown.
func newNodeEndpoint(nodeNum int, packetConn net.PacketConn) (*transport.Endpoint, error) {
	key, _, err := nodeIdentity(nodeNum)
	if err != nil {
		return nil, err
	}
	endpoint, err := transport.NewEndpoint(key, packetConn, transport.Shadow())
	if err != nil {
		return nil, fmt.Errorf("create endpoint: %w", err)
	}
	return endpoint, nil
}
