package sim

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/ethp2p/ethp2p/broadcast"
)

// ShadowDriver implements Driver for Shadow simulation.
type ShadowDriver struct {
	Strategy    StrategyFunc
	TraceWriter *TraceWriter

	observer *Observer
}

var _ Driver = &ShadowDriver{}

// Close is a no-op for Shadow.
func (s *ShadowDriver) Close() error {
	return nil
}

// NewNode creates a node for Shadow simulation.
func (s *ShadowDriver) NewNode(nodeNum int, logger *slog.Logger) (Node, error) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: DefaultListenPort})
	if err != nil {
		return nil, err
	}
	sconn := &shadowUDPConn{PacketConn: conn, ch: make(chan struct{}, 1)}

	var obs broadcast.Observer
	if s.TraceWriter != nil {
		to := NewTracingObserver(nodeNum, s.TraceWriter)
		s.observer = to.Observer
		obs = to
	} else {
		o := NewObserver()
		s.observer = o
		obs = o
	}
	return s.Strategy(nodeNum, sconn, logger, obs, s.TraceWriter)
}

// Observer returns the observer for the node created by this driver.
func (s *ShadowDriver) Observer() *Observer {
	return s.observer
}

// NodeAddr resolves the address for a node in Shadow via DNS.
func (s *ShadowDriver) NodeAddr(nodeNum int) net.Addr {
	hostname := fmt.Sprintf("node%d", nodeNum)
	addrs, err := net.LookupHost(hostname)
	if err != nil {
		panic(err)
	}
	if len(addrs) == 0 {
		panic("no addrs for host")
	}
	ipAddr := net.ParseIP(addrs[0])
	addr := &net.UDPAddr{IP: ipAddr, Port: DefaultListenPort}
	return addr
}

// Start is a no-op for Shadow.
func (s *ShadowDriver) Start() {}

// shadowUDPConn serializes writes via a channel semaphore (buffer=1).
// Shadow doesn't support concurrent UDP writes from a single socket.
type shadowUDPConn struct {
	net.PacketConn
	ch chan struct{}
}

func (s *shadowUDPConn) Close() error {
	return s.PacketConn.Close()
}

func (s *shadowUDPConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	s.ch <- struct{}{}
	defer func() { <-s.ch }()
	return s.PacketConn.WriteTo(p, addr)
}
