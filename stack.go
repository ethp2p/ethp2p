// Package ethp2p assembles the ethp2p stack. It holds the node's
// authenticated identity and the key material shared with libp2p, without
// depending on libp2p itself.
package ethp2p

import (
	"errors"
	"sync"

	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/transport"
)

// PrivKey is the private-key capability the stack requires. libp2p's
// crypto.PrivKey satisfies it; adapters live outside this package.
type PrivKey interface {
	// Sign signs data with the private key.
	Sign(data []byte) ([]byte, error)
}

// Stack is the top-level ethp2p assembly. Set the fields, then call Init.
type Stack struct {
	// PeerID is the node's authenticated peer ID, shared with libp2p.
	PeerID transport.PeerID
	// Key authenticates connections ethp2p initiates.
	Key PrivKey
	// BroadcastConfig configures the broadcast engine.
	BroadcastConfig broadcast.EngineConfig

	initialized bool

	engineMu sync.Mutex
	engine   *broadcast.Engine
}

// Init validates the configuration.
func (s *Stack) Init() error {
	if s.PeerID == "" {
		return errors.New("peer ID is required")
	}
	if s.Key == nil {
		return errors.New("key is required")
	}
	s.initialized = true
	return nil
}

// ID returns the authenticated peer ID of this node. Valid after Init.
func (s *Stack) ID() string { return string(s.PeerID) }

// BroadcastEngine returns the stack's singleton broadcast engine, creating it
// on first use from BroadcastConfig. Later calls return the same engine.
// Inbound connections derive peer identity from transport authentication, so
// the stack key is only consumed when ethp2p initiates a connection.
func (s *Stack) BroadcastEngine() (*broadcast.Engine, error) {
	if !s.initialized {
		return nil, errors.New("stack not initialized: call Init")
	}
	s.engineMu.Lock()
	defer s.engineMu.Unlock()
	if s.engine == nil {
		s.engine = broadcast.NewEngine(s.BroadcastConfig)
	}
	return s.engine, nil
}
