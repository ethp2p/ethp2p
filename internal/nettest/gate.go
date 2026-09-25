package nettest

import (
	"context"
	"sync"

	"github.com/ethp2p/ethp2p/internal/devhook"
	"github.com/ethp2p/ethp2p/internal/trace"
)

// Gate pauses selected component crossings until Release is called.
type Gate struct {
	site     devhook.Site
	release  chan struct{}
	mu       sync.Mutex
	released bool
	held     int
}

// Release resumes every crossing held by g. Repeated calls are safe.
func (g *Gate) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.released {
		g.released = true
		close(g.release)
	}
}

// Held counts crossings currently blocked at this gate.
func (g *Gate) Held() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.held
}

func (g *Gate) matches(site devhook.Site) bool {
	return g.site.Point == site.Point &&
		(g.site.Peer == "" || g.site.Peer == site.Peer) &&
		(g.site.Channel == "" || g.site.Channel == site.Channel) &&
		(g.site.Message == "" || g.site.Message == site.Message)
}

// Hold pauses crossings matching site. Point is required; empty identity
// fields match any value known to the component at that point.
func (nd *Node) Hold(site devhook.Site) *Gate {
	if site.Point < devhook.PointHandshake || site.Point > devhook.PointChunkWrite {
		panic("nettest: Hold requires a valid Point")
	}
	g := &Gate{site: site, release: make(chan struct{})}
	nd.mu.Lock()
	nd.gates = append(nd.gates, g)
	nd.mu.Unlock()
	return g
}

// wait is the node's devhook gate. A crossing counts as held only once it is
// registered on an unreleased matching gate, under the lock Release takes.
func (nd *Node) wait(ctx context.Context, site devhook.Site) bool {
	g := nd.enter(site)
	if g == nil {
		return true
	}
	if ctx.Err() != nil {
		g.leave()
		return false
	}
	nd.net.recorder.emit(nd.Name, trace.GateHeld{Point: site.Point.String(), Peer: site.Peer, Channel: site.Channel, Message: site.Message})
	select {
	case <-g.release:
	case <-ctx.Done():
	}
	g.leave()
	nd.net.recorder.emit(nd.Name, trace.GateReleased{Point: site.Point.String(), Peer: site.Peer, Channel: site.Channel, Message: site.Message})
	return ctx.Err() == nil
}

// enter registers a crossing on the first unreleased gate matching site.
func (nd *Node) enter(site devhook.Site) *Gate {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	for _, g := range nd.gates {
		if !g.matches(site) {
			continue
		}
		g.mu.Lock()
		if !g.released {
			g.held++
			g.mu.Unlock()
			return g
		}
		g.mu.Unlock()
	}
	return nil
}

func (g *Gate) leave() {
	g.mu.Lock()
	g.held--
	g.mu.Unlock()
}

func (nd *Node) releaseGates() {
	nd.mu.Lock()
	gates := append([]*Gate(nil), nd.gates...)
	nd.mu.Unlock()
	for _, g := range gates {
		g.Release()
	}
}

type nodeSink struct{ node *Node }

func (s nodeSink) Emit(event trace.Event) { s.node.net.recorder.emit(s.node.Name, event) }
