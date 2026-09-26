// Package nettest builds real ethp2p nodes on a simulated packet network.
// Every Run owns a synctest bubble and closes its nodes before leaving it.
package nettest

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	ethp2p "github.com/ethp2p/ethp2p"
	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/enr"
	"github.com/ethp2p/ethp2p/identity"
	"github.com/ethp2p/ethp2p/internal/devhook"
	"github.com/ethp2p/ethp2p/internal/trace"
	"github.com/ethp2p/ethp2p/transport"
	"github.com/marcopolo/simnet"
)

type pair struct{ a, b string }

func makePair(a, b string) pair {
	if a > b {
		a, b = b, a
	}
	return pair{a, b}
}

// DefaultTimeout bounds virtual-time waits in the harness.
const DefaultTimeout = 10 * time.Second

// Net owns the nodes, links, and trace recorder for one synctest bubble.
type Net struct {
	t         *testing.T
	sim       *simnet.Simnet
	mu        sync.RWMutex
	nodes     []*Node
	names     map[string]bool
	peerNodes map[string]*Node
	latencies map[pair]time.Duration
	links     []*connection
	recorder  *Recorder
}

// Run executes fn on simnet inside a synctest bubble and reports shutdown errors.
func Run(t *testing.T, fn func(t *testing.T, n *Net)) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		n := &Net{t: t, names: make(map[string]bool), peerNodes: make(map[string]*Node), latencies: make(map[pair]time.Duration), recorder: newRecorder()}
		n.sim = &simnet.Simnet{LatencyFunc: n.latencyFor}
		defer func() {
			for _, node := range n.nodes {
				node.releaseGates()
			}
			for _, v := range slices.Backward(n.nodes) {
				v.Close()
				if err := v.closeError(); err != nil {
					t.Errorf("close node %s: %v", v.Name, err)
				}
			}
			n.sim.Close()
			n.recorder.dump(t)
		}()
		// simnet supports adding endpoints after Start; this also permits Node
		// to be called at any point in a scenario.
		n.sim.Start()
		fn(t, n)
	})
}

func (n *Net) latencyFor(p *simnet.Packet) time.Duration {
	a, b := p.From.(*net.UDPAddr).IP.String(), p.To.(*net.UDPAddr).IP.String()
	n.mu.RLock()
	d, ok := n.latencies[makePair(a, b)]
	n.mu.RUnlock()
	if !ok {
		panic(fmt.Sprintf("packet on unconnected link %s/%s", a, b))
	}
	return d
}

type nodeOptions struct {
	bandwidth   int
	engine      broadcast.EngineConfig
	beforeStart []func(*ethp2p.Stack)
	hookFns     []func(*devhook.Hooks)
	chunkFaults func(peer *Node, channel broadcast.ChannelID, msg broadcast.MessageID, chunk []byte) devhook.ChunkFault
}

// NodeOption configures a node before its engine starts.
type NodeOption func(*nodeOptions)

// Bandwidth sets symmetric simulated upload and download bandwidth in bits/s.
func Bandwidth(bitsPerSec int) NodeOption { return func(o *nodeOptions) { o.bandwidth = bitsPerSec } }

// EngineConfig sets engine options; the harness supplies its trace observer.
func EngineConfig(config broadcast.EngineConfig) NodeOption {
	return func(o *nodeOptions) { o.engine = config }
}

// BeforeStart runs after broadcast registration and before the stack starts.
// Multiple callbacks run in option order, while registration is still open.
func BeforeStart(fn func(*ethp2p.Stack)) NodeOption {
	return func(o *nodeOptions) { o.beforeStart = append(o.beforeStart, fn) }
}

// Hooks configures test instrumentation before the engine starts.
func Hooks(configure func(*devhook.Hooks)) NodeOption {
	return func(o *nodeOptions) { o.hookFns = append(o.hookFns, configure) }
}

// MaxConcurrentReads overrides the per-session read limit for a node.
func MaxConcurrentReads(limit int) NodeOption {
	return Hooks(func(h *devhook.Hooks) { h.MaxConcurrentReads = limit })
}

// ChunkFaults injects outbound chunk faults, resolving authenticated peer IDs
// to harness nodes before invoking fn.
func ChunkFaults(fn func(peer *Node, channel broadcast.ChannelID, msg broadcast.MessageID, chunk []byte) devhook.ChunkFault) NodeOption {
	return func(o *nodeOptions) { o.chunkFaults = fn }
}

// Node builds a real Stack and broadcast Engine on a deterministic simnet endpoint.
func (n *Net) Node(name string, opts ...NodeOption) *Node {
	o := nodeOptions{bandwidth: 50 * simnet.Mibps}
	for _, opt := range opts {
		opt(&o)
	}
	if n.names[name] {
		panic("duplicate nettest node: " + name)
	}
	n.names[name] = true
	var key *identity.PrivKey
	var err error
	for counter := 0; ; counter++ {
		seed := name
		if counter != 0 {
			seed += ":" + strconv.Itoa(counter)
		}
		hash := sha256.Sum256([]byte(seed))
		key, err = identity.ParsePrivKey(hash[:])
		if err == nil {
			break
		}
	}
	ip := simnet.IntToPublicIPv4(len(n.nodes) + 1).To4()
	addr := &net.UDPAddr{IP: ip, Port: 9000}
	settings := simnet.NodeBiDiLinkSettings{Uplink: simnet.LinkSettings{BitsPerSecond: o.bandwidth}, Downlink: simnet.LinkSettings{BitsPerSecond: o.bandwidth}}
	packet := n.sim.NewEndpoint(addr, settings)
	shared, err := transport.NewShared(key, packet, transport.Shadow())
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(n.t.Context())
	nd := &Node{Name: name, ID: shared.PeerID(), net: n, packet: packet, shared: shared, ctx: ctx, cancel: cancel, channels: make(map[broadcast.ChannelID]*Channel)}
	nd.Record, err = enr.Sign(key, 1, enr.IP.Set(netip.AddrFrom4([4]byte(ip))), enr.QUIC.Set(9000))
	if err != nil {
		panic(err)
	}
	hooks := &devhook.Hooks{}
	for _, configure := range o.hookFns {
		configure(hooks)
	}
	hooks.Trace = nodeSink{node: nd}
	hooks.Gate = nd.wait
	n.recorder.registerPeer(string(nd.ID), name)
	n.mu.Lock()
	n.peerNodes[string(nd.ID)] = nd
	n.mu.Unlock()
	if o.chunkFaults != nil {
		hooks.ChunkWrite = func(peer, channel, message string, chunk []byte) devhook.ChunkFault {
			n.mu.RLock()
			peerNode := n.peerNodes[peer]
			n.mu.RUnlock()
			return o.chunkFaults(peerNode, broadcast.ChannelID(channel), broadcast.MessageID(message), chunk)
		}
	}
	devhook.Attach(&o.engine, hooks)
	o.engine.Observer = observer{name: name, recorder: n.recorder}
	nd.Engine = broadcast.NewEngine(o.engine)
	nd.Stack, err = ethp2p.NewStack(shared.Ethp2p(), ethp2p.Config{Record: nd.Record})
	if err != nil {
		panic(err)
	}
	if err := nd.Engine.Register(nd.Stack); err != nil {
		panic(err)
	}
	for _, fn := range o.beforeStart {
		fn(nd.Stack)
	}
	if err := nd.Stack.Start(); err != nil {
		panic(err)
	}
	n.nodes = append(n.nodes, nd)
	return nd
}

type linkOptions struct{ latency time.Duration }

// LinkOption configures a simulated connection before dialing.
type LinkOption func(*linkOptions)

// Latency sets symmetric one-way packet latency on a link.
func Latency(d time.Duration) LinkOption { return func(o *linkOptions) { o.latency = d } }

type connection struct{ a, b *Node }

// Connect records the packet route before dialing through the owning stack.
// Duplicate connections are test bugs; reconnection requires Disconnect first.
func (n *Net) Connect(a, b *Node, opts ...LinkOption) {
	for _, link := range n.links {
		if (link.a == a && link.b == b) || (link.a == b && link.b == a) {
			panic("nodes are already connected")
		}
	}
	o := linkOptions{latency: time.Millisecond}
	for _, opt := range opts {
		opt(&o)
	}
	n.mu.Lock()
	n.latencies[makePair(a.packet.LocalAddr().(*net.UDPAddr).IP.String(), b.packet.LocalAddr().(*net.UDPAddr).IP.String())] = o.latency
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(a.ctx, DefaultTimeout)
	defer cancel()
	if err := a.Stack.Connect(ctx, b.Record); err != nil {
		n.t.Fatalf("connect %s to %s: %v\n%s", a.Name, b.Name, err, n.recorder.timeline())
	}
	n.links = append(n.links, &connection{a: a, b: b})
}

// Disconnect releases the stack view and both nodes' accepted libp2p views.
// The latency entry remains so packets already in flight still have a route.
func (n *Net) Disconnect(a, b *Node) {
	kept := n.links[:0]
	found := false
	for _, c := range n.links {
		if (c.a == a && c.b == b) || (c.a == b && c.b == a) {
			found = true
			n.reportLinkClose(a.Stack.Disconnect(b.ID))
			continue
		}
		kept = append(kept, c)
	}
	n.links = kept
	if !found {
		panic("nodes are not connected")
	}
	a.closeLibViewsFor(b)
	b.closeLibViewsFor(a)
}

func (n *Net) removeNodeLinks(node *Node) {
	for {
		var current *connection
		for _, link := range n.links {
			if link.a == node || link.b == node {
				current = link
				break
			}
		}
		if current == nil {
			return
		}
		n.Disconnect(current.a, current.b)
	}
}

func (n *Net) reportLinkClose(err error) {
	if err = unexpectedClose(err); err != nil {
		n.t.Errorf("close link: %v", err)
	}
}

// ConnInfo returns a's current stack entry for b, with a timeline on failure.
func (n *Net) ConnInfo(t *testing.T, a, b *Node) ethp2p.ConnInfo {
	t.Helper()
	for _, info := range a.Stack.Connections() {
		if info.Peer == b.ID {
			return info
		}
	}
	t.Fatalf("no connection from %s to %s\n%s", a.Name, b.Name, n.recorder.timeline())
	return ethp2p.ConnInfo{}
}

// Settle runs all currently runnable bubble goroutines to a durable block.
func (n *Net) Settle() { synctest.Wait() }

// Trace returns the shared event recorder.
func (n *Net) Trace() *Recorder { return n.recorder }

// Await waits in virtual time for a matching trace record.
func (n *Net) Await(t *testing.T, m Matcher, timeout time.Duration) Record {
	t.Helper()
	rec, ok := n.await(m, timeout)
	if !ok {
		t.Fatalf("timeout awaiting %s\n%s", m, n.recorder.timeline())
	}
	return rec
}

// AwaitDecoded waits for a decode event and settles its delivery work.
func (n *Net) AwaitDecoded(t *testing.T, node *Node, channel broadcast.ChannelID, id broadcast.MessageID) {
	t.Helper()
	n.Await(t, Match(node, trace.SessionDecoded{Channel: string(channel), Message: string(id)}), DefaultTimeout)
	n.Settle()
}

// AwaitPeers waits until the current subscribed peer set exactly equals peers.
// Unsubscribe and gone events remove peers; repeated subscriptions do not count twice.
func (n *Net) AwaitPeers(t *testing.T, node *Node, channel broadcast.ChannelID, peers ...*Node) {
	t.Helper()
	want := make(map[string]bool, len(peers))
	for _, peer := range peers {
		want[string(peer.ID)] = true
	}
	timer := time.NewTimer(DefaultTimeout)
	defer timer.Stop()
	for {
		records, changed := n.recorder.snapshot()
		current := make(map[string]bool)
		for _, rec := range records {
			if rec.Node != node.Name {
				continue
			}
			switch ev := rec.Event.(type) {
			case trace.PeerSubscribed:
				if ev.Channel == string(channel) {
					current[ev.Peer] = true
				}
			case trace.PeerUnsubscribed:
				if ev.Channel == string(channel) {
					delete(current, ev.Peer)
				}
			case trace.PeerGone:
				delete(current, ev.Peer)
			}
		}
		if len(current) == len(want) {
			matched := true
			for id := range want {
				if !current[id] {
					matched = false
					break
				}
			}
			if matched {
				return
			}
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("timeout awaiting peers on %s/%s: have %d want %d\n%s", node.Name, channel, len(current), len(want), n.recorder.timeline())
		}
	}
}

// Within runs a context-aware fn in the bubble and requires it to return
// within d of virtual time. On timeout it logs the timeline, cancels ctx,
// and joins fn before failing. Callers must make every blocking operation
// in fn observe ctx; an uncooperative fn causes a synctest deadlock panic.
func (n *Net) Within(t *testing.T, d time.Duration, fn func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("operation failed: %v\n%s", err, n.recorder.timeline())
		}
	case <-timer.C:
		t.Errorf("operation exceeded %s virtual time\n%s", d, n.recorder.timeline())
		cancel()
		<-done
		t.FailNow()
	}
}

func (n *Net) await(m Matcher, timeout time.Duration) (Record, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		recs, changed := n.recorder.snapshot()
		for _, rec := range recs {
			if m.Match(rec) {
				return rec, true
			}
		}
		select {
		case <-changed:
		case <-timer.C:
			return Record{}, false
		}
	}
}
