package sim

import (
	"sync"

	"github.com/ethp2p/ethp2p/broadcast"
)

type sessionKey struct {
	channelID broadcast.ChannelID
	messageID broadcast.MessageID
}

// ChunkStats holds per-node chunk reception verdict counts.
type ChunkStats struct {
	Accepted  int
	Redundant int
	Decoding  int
	Surplus   int
}

// Observer tracks bytes sent bucketed by session role (origin vs relay) and
// chunk reception verdicts. Embed NoOpObserver for methods we don't override.
type Observer struct {
	broadcast.NoOpObserver

	mu       sync.RWMutex
	roles    map[sessionKey]broadcast.SessionRole
	sent     map[broadcast.SessionRole]int
	verdicts [5]int // indexed by Verdict iota
}

func NewObserver() *Observer {
	return &Observer{
		roles: make(map[sessionKey]broadcast.SessionRole),
		sent:  make(map[broadcast.SessionRole]int),
	}
}

func (o *Observer) OnSessionStarted(channelID broadcast.ChannelID, messageID broadcast.MessageID, role broadcast.SessionRole) {
	o.mu.Lock()
	o.roles[sessionKey{channelID, messageID}] = role
	o.mu.Unlock()
}

func (o *Observer) OnChunkSent(_ broadcast.PeerID, channelID broadcast.ChannelID, messageID broadcast.MessageID, bytesSent int) {
	o.mu.Lock()
	role := o.roles[sessionKey{channelID, messageID}]
	o.sent[role] += bytesSent
	o.mu.Unlock()
}

func (o *Observer) OnChunkRcvd(_ broadcast.PeerID, _ broadcast.ChannelID, _ broadcast.MessageID, verdict broadcast.Verdict) {
	o.mu.Lock()
	if int(verdict) < len(o.verdicts) {
		o.verdicts[verdict]++
	}
	o.mu.Unlock()
}

// Stats returns total bytes sent as origin and as relay.
func (o *Observer) Stats() (originSent, relaySent int) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.sent[broadcast.SessionRoleOrigin], o.sent[broadcast.SessionRoleRelay]
}

// Chunks returns chunk reception verdict counts.
func (o *Observer) Chunks() ChunkStats {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return ChunkStats{
		Accepted:  o.verdicts[broadcast.VerdictAccepted],
		Redundant: o.verdicts[broadcast.VerdictRedundant],
		Decoding:  o.verdicts[broadcast.VerdictDecoding],
		Surplus:   o.verdicts[broadcast.VerdictSurplus],
	}
}

// ObserverSnapshot holds a point-in-time snapshot of all observer counters.
type ObserverSnapshot struct {
	Chunks     ChunkStats
	OriginSent int
	RelaySent  int
}

// Reset snapshots all counters and zeros them.
func (o *Observer) Reset() ObserverSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	snap := ObserverSnapshot{
		Chunks: ChunkStats{
			Accepted:  o.verdicts[broadcast.VerdictAccepted],
			Redundant: o.verdicts[broadcast.VerdictRedundant],
			Decoding:  o.verdicts[broadcast.VerdictDecoding],
			Surplus:   o.verdicts[broadcast.VerdictSurplus],
		},
		OriginSent: o.sent[broadcast.SessionRoleOrigin],
		RelaySent:  o.sent[broadcast.SessionRoleRelay],
	}
	o.verdicts = [5]int{}
	o.sent[broadcast.SessionRoleOrigin] = 0
	o.sent[broadcast.SessionRoleRelay] = 0
	return snap
}
