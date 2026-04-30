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

	mu                sync.RWMutex
	roles             map[sessionKey]broadcast.SessionRole
	sent              map[broadcast.SessionRole]int
	sentByMessage     map[broadcast.MessageID]map[broadcast.SessionRole]int
	verdictsByMessage map[broadcast.MessageID][5]int // indexed by Verdict iota
}

func NewObserver() *Observer {
	return &Observer{
		roles:             make(map[sessionKey]broadcast.SessionRole),
		sent:              make(map[broadcast.SessionRole]int),
		sentByMessage:     make(map[broadcast.MessageID]map[broadcast.SessionRole]int),
		verdictsByMessage: make(map[broadcast.MessageID][5]int),
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
	byRole := o.sentByMessage[messageID]
	if byRole == nil {
		byRole = make(map[broadcast.SessionRole]int)
		o.sentByMessage[messageID] = byRole
	}
	byRole[role] += bytesSent
	o.mu.Unlock()
}

func (o *Observer) OnChunkRcvd(_ broadcast.PeerID, _ broadcast.ChannelID, messageID broadcast.MessageID, verdict broadcast.Verdict) {
	o.mu.Lock()
	if int(verdict) < len([5]int{}) {
		verdicts := o.verdictsByMessage[messageID]
		verdicts[verdict]++
		o.verdictsByMessage[messageID] = verdicts
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
	var verdicts [5]int
	for _, msgVerdicts := range o.verdictsByMessage {
		for i, count := range msgVerdicts {
			verdicts[i] += count
		}
	}
	return chunkStatsFromVerdicts(verdicts)
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
	var verdicts [5]int
	for _, msgVerdicts := range o.verdictsByMessage {
		for i, count := range msgVerdicts {
			verdicts[i] += count
		}
	}
	snap := ObserverSnapshot{
		Chunks:     chunkStatsFromVerdicts(verdicts),
		OriginSent: o.sent[broadcast.SessionRoleOrigin],
		RelaySent:  o.sent[broadcast.SessionRoleRelay],
	}
	o.verdictsByMessage = make(map[broadcast.MessageID][5]int)
	o.sentByMessage = make(map[broadcast.MessageID]map[broadcast.SessionRole]int)
	o.sent[broadcast.SessionRoleOrigin] = 0
	o.sent[broadcast.SessionRoleRelay] = 0
	return snap
}

// ResetMessage snapshots and zeros counters for one message.
func (o *Observer) ResetMessage(messageID broadcast.MessageID) ObserverSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	byRole := o.sentByMessage[messageID]
	snap := ObserverSnapshot{
		Chunks:     chunkStatsFromVerdicts(o.verdictsByMessage[messageID]),
		OriginSent: byRole[broadcast.SessionRoleOrigin],
		RelaySent:  byRole[broadcast.SessionRoleRelay],
	}
	delete(o.verdictsByMessage, messageID)
	delete(o.sentByMessage, messageID)
	o.sent[broadcast.SessionRoleOrigin] -= snap.OriginSent
	o.sent[broadcast.SessionRoleRelay] -= snap.RelaySent
	return snap
}

func chunkStatsFromVerdicts(verdicts [5]int) ChunkStats {
	return ChunkStats{
		Accepted:  verdicts[broadcast.VerdictAccepted],
		Redundant: verdicts[broadcast.VerdictRedundant],
		Decoding:  verdicts[broadcast.VerdictDecoding],
		Surplus:   verdicts[broadcast.VerdictSurplus],
	}
}
