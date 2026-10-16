package harness

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethp2p/ethp2p/internal/trace"
)

// Record is one observation from a node in emission order.
type Record struct {
	// Seq is monotonically increasing within a Run.
	Seq uint64
	// At is virtual time since Run began.
	At time.Duration
	// Node is the emitting node's name.
	Node string
	// Event is a value from the closed trace event set.
	Event trace.Event
}

// Recorder orders events at emission, including events from concurrent nodes.
type Recorder struct {
	mu        sync.Mutex
	start     time.Time
	records   []Record
	changed   chan struct{}
	peerNames map[string]string
}

func newRecorder() *Recorder {
	return &Recorder{start: time.Now(), changed: make(chan struct{}), peerNames: make(map[string]string)}
}

func (r *Recorder) registerPeer(id, name string) {
	r.mu.Lock()
	r.peerNames[id] = name
	r.mu.Unlock()
}

func (r *Recorder) emit(node string, ev trace.Event) {
	r.mu.Lock()
	r.records = append(r.records, Record{Seq: uint64(len(r.records) + 1), At: time.Since(r.start), Node: node, Event: ev})
	close(r.changed)
	r.changed = make(chan struct{})
	r.mu.Unlock()
}

// Records returns an ordered snapshot. Callers must not mutate event fields.
func (r *Recorder) Records() []Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.records)
}

func (r *Recorder) snapshot() ([]Record, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.records), r.changed
}

func (r *Recorder) timeline() string {
	var b strings.Builder
	r.mu.Lock()
	names := make(map[string]string, len(r.peerNames))
	for id, name := range r.peerNames {
		names[id] = name
	}
	r.mu.Unlock()
	for _, rec := range r.Records() {
		fmt.Fprintf(&b, "+%10s  %s  %s\n", rec.At, rec.Node, renderEvent(rec.Event, names))
	}
	return b.String()
}

func renderEvent(ev trace.Event, names map[string]string) string {
	out := fmt.Sprint(ev)
	if peer := reflect.ValueOf(ev).FieldByName("Peer"); peer.IsValid() {
		if name, ok := names[peer.String()]; ok {
			out = strings.Replace(out, fmt.Sprintf("peer=%.4x", peer.String()), "peer="+name, 1)
		}
	}
	return out
}

// Matcher tests a record and describes the expected observation on failure.
type Matcher interface {
	// Match reports whether a record satisfies the expectation.
	Match(Record) bool
	// String describes the expectation in failure messages.
	String() string
}

type eventMatcher struct {
	node  string
	event trace.Event
}

// Match compares the concrete event type and each nonzero field of ev.
// Nested struct and nonnil slice fields compare exactly; error fields use
// errors.Is so wrapped sentinel errors match.
func Match(node *Node, ev trace.Event) Matcher { return eventMatcher{node: node.Name, event: ev} }
func (m eventMatcher) Match(rec Record) bool {
	if rec.Node != m.node {
		return false
	}
	want, got := reflect.ValueOf(m.event), reflect.ValueOf(rec.Event)
	if want.Type() != got.Type() {
		return false
	}
	for i := range want.NumField() {
		field := want.Field(i)
		if field.IsZero() {
			continue
		}
		if field.Type().Implements(reflect.TypeFor[error]()) {
			gotErr, _ := got.Field(i).Interface().(error)
			if !errors.Is(gotErr, field.Interface().(error)) {
				return false
			}
			continue
		}
		if !reflect.DeepEqual(field.Interface(), got.Field(i).Interface()) {
			return false
		}
	}
	return true
}
func (m eventMatcher) String() string { return fmt.Sprintf("node=%s event=%v", m.node, m.event) }

type predicateMatcher[E trace.Event] struct {
	node string
	pred func(E) bool
}

// Where matches events of type E for which pred returns true.
func Where[E trace.Event](node *Node, pred func(E) bool) Matcher {
	return predicateMatcher[E]{node: node.Name, pred: pred}
}
func (m predicateMatcher[E]) Match(rec Record) bool {
	if rec.Node != m.node {
		return false
	}
	ev, ok := rec.Event.(E)
	return ok && m.pred(ev)
}
func (m predicateMatcher[E]) String() string {
	return fmt.Sprintf("node=%s event=%T predicate", m.node, *new(E))
}

// Require asserts that matchers occur as an in-order subsequence.
func (r *Recorder) Require(t *testing.T, ms ...Matcher) {
	t.Helper()
	i := 0
	for _, rec := range r.Records() {
		if i < len(ms) && ms[i].Match(rec) {
			i++
		}
	}
	if i != len(ms) {
		t.Fatalf("missing ordered matcher %d/%d: %s\n%s", i+1, len(ms), ms[i], r.timeline())
	}
}

// Count returns the number of recorded events matching m.
func (r *Recorder) Count(m Matcher) int {
	n := 0
	for _, rec := range r.Records() {
		if m.Match(rec) {
			n++
		}
	}
	return n
}

func (r *Recorder) dump(t *testing.T) {
	if t.Failed() || os.Getenv("NETTEST_TRACE") == "1" {
		t.Logf("network timeline:\n%s", r.timeline())
	}
}
