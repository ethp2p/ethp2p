package nettest

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strconv"
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

func requireValueEvent(ev trace.Event) reflect.Value {
	v := reflect.ValueOf(ev)
	if !v.IsValid() || v.Kind() != reflect.Struct {
		panic(fmt.Sprintf("nettest: trace events must be values, got %T", ev))
	}
	return v
}

func (r *Recorder) emit(node string, ev trace.Event) {
	requireValueEvent(ev)
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
	maps.Copy(names, r.peerNames)
	r.mu.Unlock()
	for _, rec := range r.Records() {
		fmt.Fprintf(&b, "+%10s  %s  %s\n", rec.At, rec.Node, renderEvent(rec.Event, names))
	}
	return b.String()
}

func renderEvent(ev trace.Event, names map[string]string) string {
	out := fmt.Sprint(ev)
	v := requireValueEvent(ev)
	t := v.Type()
	for i := range v.NumField() {
		field := v.Field(i)
		if field.Kind() == reflect.String && t.Field(i).Name == "Peer" {
			id := field.String()
			if name, ok := names[id]; ok {
				hexID := fmt.Sprintf("%x", []byte(id))
				hexID = hexID[:8]
				out = strings.Replace(out, "peer="+hexID, "peer="+name, 1)
			}
		}
		if field.Kind() == reflect.Slice && field.Type().Elem().Kind() == reflect.String {
			for j := range field.Len() {
				id := field.Index(j).String()
				if name, ok := names[id]; ok {
					out = strings.ReplaceAll(out, strconv.Quote(id), strconv.Quote(name))
				}
			}
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
	node    string
	anyNode bool
	event   trace.Event
}

// Match compares the concrete event type and each nonzero field of ev.
// Nested struct and nonnil slice fields compare exactly; error fields use
// errors.Is so wrapped sentinel errors match. Pointer events panic.
func Match(node *Node, ev trace.Event) Matcher {
	requireValueEvent(ev)
	m := eventMatcher{event: ev, anyNode: node == nil}
	if node != nil {
		m.node = node.Name
	}
	return m
}
func (m eventMatcher) Match(rec Record) bool {
	if !m.anyNode && rec.Node != m.node {
		return false
	}
	want, got := requireValueEvent(m.event), requireValueEvent(rec.Event)
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
	node    string
	anyNode bool
	pred    func(E) bool
}

// Where matches value events of type E for which pred returns true.
// E must be a value event type; pointer event types panic.
func Where[E trace.Event](node *Node, pred func(E) bool) Matcher {
	if reflect.TypeFor[E]().Kind() != reflect.Struct {
		panic(fmt.Sprintf("nettest: trace events must be values, got %s", reflect.TypeFor[E]()))
	}
	m := predicateMatcher[E]{anyNode: node == nil, pred: pred}
	if node != nil {
		m.node = node.Name
	}
	return m
}
func (m predicateMatcher[E]) Match(rec Record) bool {
	if !m.anyNode && rec.Node != m.node {
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

// RequireNone asserts that no recorded event matches m.
func (r *Recorder) RequireNone(t *testing.T, m Matcher) {
	t.Helper()
	for _, rec := range r.Records() {
		if m.Match(rec) {
			t.Fatalf("unexpected matcher %s at seq %d\n%s", m, rec.Seq, r.timeline())
		}
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
