package broadcast

import "sync"

type ctrlOutState struct {
	mu        sync.Mutex
	ready     chan struct{}
	closed    bool
	channels  map[ChannelID]*channelOut
	sessions  map[sessionKey]*sessionOut
	nextOrder uint64
}

type channelOut struct{ want, announced, retire bool }

type sessionOut struct {
	pendingOpen *peerOpenSession
	open        bool
	routing     []byte
	closeStream bool
	close       bool
	order       uint64 // Stable selection within a phase preserves session creation order.
}

func newCtrlOutState() *ctrlOutState {
	return &ctrlOutState{
		ready:    make(chan struct{}, 1),
		channels: make(map[ChannelID]*channelOut),
		sessions: make(map[sessionKey]*sessionOut),
	}
}

// announceInitial records the channel set already carried by Hello.
func (s *ctrlOutState) announceInitial(ids []ChannelID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	for _, id := range ids {
		if s.channels[id] == nil {
			s.channels[id] = &channelOut{want: true, announced: true}
		}
	}
}

func (s *ctrlOutState) announce(id ChannelID, present bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	c := s.channels[id]
	if c == nil {
		c = &channelOut{}
		s.channels[id] = c
	}
	if present {
		if c.announced && !c.want {
			c.retire = true
		}
		c.want = true
	} else {
		c.want = false
		for key, session := range s.sessions {
			if key.channelID == id {
				session.routing = nil
			}
		}
	}
	s.signal()
}

func (s *ctrlOutState) openSession(event peerOpenSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	key := sessionKey{channelID: event.channelID, messageID: event.messageID}
	out := s.session(key)
	if out.close {
		// A fresh open replaces the closed generation for this key. If the
		// previous generation reached the writer, handleSessionOpen closes its
		// existing stream before installing the replacement.
		out.closeStream = false
		out.routing = nil
		out.close = false
	}
	pending := event
	out.pendingOpen = &pending
	s.signal()
}

func (s *ctrlOutState) closeSession(key sessionKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.session(key).close = true
	s.signal()
}

func (s *ctrlOutState) closeSessionStream(key sessionKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.session(key).closeStream = true
	s.signal()
}

func (s *ctrlOutState) setRouting(event peerSendRouting) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	key := sessionKey{channelID: event.channelID, messageID: event.messageID}
	out := s.sessions[key]
	if out == nil {
		return false
	}
	if event.update == nil {
		event.update = []byte{}
	}
	out.routing = event.update
	s.signal()
	return true
}

func (s *ctrlOutState) session(key sessionKey) *sessionOut {
	out := s.sessions[key]
	if out == nil {
		s.nextOrder++
		out = &sessionOut{order: s.nextOrder}
		s.sessions[key] = out
	}
	return out
}

func (s *ctrlOutState) signal() {
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// next returns one transition in phase order, marking it consumed before the
// caller performs its write.
func (s *ctrlOutState) next() (peerCtrlEvent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false
	}

	for {
		key, out := s.firstSession(func(out *sessionOut) bool { return out.close })
		if out == nil {
			break
		}
		if out.pendingOpen != nil && !out.open {
			delete(s.sessions, key)
			continue
		}
		if !out.open {
			delete(s.sessions, key)
			continue
		}
		out.close = false
		out.open = false
		if out.pendingOpen == nil {
			delete(s.sessions, key)
		}
		return peerCloseSession(key), true
	}

	for id, out := range s.channels {
		if !out.want && !out.announced && !out.retire {
			delete(s.channels, id)
			continue
		}
		if out.retire || (!out.want && out.announced) {
			out.announced = false
			out.retire = false
			if !out.want {
				delete(s.channels, id)
			}
			return peerUnsubscribe{channelID: id}, true
		}
	}

	for id, out := range s.channels {
		if out.want && !out.announced {
			out.announced = true
			return peerSubscribe{channelID: id}, true
		}
	}

	if _, out := s.firstSession(func(out *sessionOut) bool { return out.pendingOpen != nil }); out != nil {
		event := *out.pendingOpen
		out.pendingOpen = nil
		out.open = true
		return event, true
	}

	if key, out := s.firstSession(func(out *sessionOut) bool { return out.closeStream && out.open }); out != nil {
		out.closeStream = false
		return peerCloseStream(key), true
	}

	if key, out := s.firstSession(func(out *sessionOut) bool { return out.routing != nil && out.open }); out != nil {
		event := peerSendRouting{channelID: key.channelID, messageID: key.messageID, update: out.routing}
		out.routing = nil
		return event, true
	}

	return nil, false
}

func (s *ctrlOutState) firstSession(match func(*sessionOut) bool) (sessionKey, *sessionOut) {
	var key sessionKey
	var first *sessionOut
	for candidate, out := range s.sessions {
		if match(out) && (first == nil || out.order < first.order) {
			key, first = candidate, out
		}
	}
	return key, first
}

func (s *ctrlOutState) close() {
	s.mu.Lock()
	s.closed = true
	clear(s.channels)
	clear(s.sessions)
	s.mu.Unlock()
}
