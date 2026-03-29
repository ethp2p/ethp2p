package sim

import (
	"log/slog"
	"net"
)

// Driver abstracts node creation and network management across
// execution backends (Shadow, simnet).
type Driver interface {
	NewNode(nodeNum int, logger *slog.Logger) (Node, error)
	NodeAddr(nodeNum int) net.Addr
	Start()
	Close() error
}
