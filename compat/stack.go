package compat

import (
	"errors"
	"fmt"

	"github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// NewStack adapts a libp2p private key into an ethp2p stack configuration,
// so both stacks authenticate as the same peer. Call Init before use.
func NewStack(key crypto.PrivKey) (*ethp2p.Stack, error) {
	// peer.IDFromPrivateKey panics on nil, so reject it here.
	if key == nil {
		return nil, errors.New("key is required")
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("derive peer ID from key: %w", err)
	}
	return &ethp2p.Stack{
		PeerID: transport.PeerID(id),
		Key:    key,
	}, nil
}
