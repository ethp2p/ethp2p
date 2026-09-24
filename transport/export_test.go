package transport

// PendingEthp2p reports queued ethp2p connection views for external tests.
func PendingEthp2p(t *SharedTransport) int { return len(t.ethQ) }

// PendingLibp2p reports queued libp2p connection views for external tests.
func PendingLibp2p(t *SharedTransport) int { return len(t.libQ) }
