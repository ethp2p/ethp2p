package sim

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

// traceHeader is the first line of a .bctrace file.
type traceHeader struct {
	V           int             `json:"v"`
	T0          time.Time       `json:"t0"`
	Nodes       []string        `json:"nodes"`
	Topology    Topology        `json:"topology"`
	Config      json.RawMessage `json:"config"`
	DecoderName string          `json:"decoderName,omitempty"`
	PeerIDs     []string        `json:"peer_ids,omitempty"`
}

// traceFooter is the last line of a .bctrace file.
type traceFooter struct {
	End      bool     `json:"end"`
	Duration int64    `json:"duration"`
	Index    [][2]int `json:"index"`
}

// TraceWriter writes broadcast trace events as compact NDJSON.
// Safe for concurrent use from multiple goroutines.
type TraceWriter struct {
	mu     sync.Mutex
	w      *bufio.Writer
	t0     time.Time
	offset int

	indexIntervalUs int64
	lastIndexTs     int64
	index           [][2]int
}

type TraceHeaderOptions struct {
	DecoderName string
	PeerIDs     []string
}

const defaultIndexInterval = 100_000 // 100ms in microseconds

// NewTraceWriter creates a TraceWriter and writes the header line.
func NewTraceWriter(w io.Writer, t0 time.Time, nodes []string, topology Topology, config json.RawMessage) (*TraceWriter, error) {
	return NewTraceWriterWithOptions(w, t0, nodes, topology, config, TraceHeaderOptions{})
}

// NewTraceWriterWithOptions creates a TraceWriter and writes the header line.
func NewTraceWriterWithOptions(w io.Writer, t0 time.Time, nodes []string, topology Topology, config json.RawMessage, opts TraceHeaderOptions) (*TraceWriter, error) {
	bw := bufio.NewWriterSize(w, 256*1024)
	hdr := traceHeader{
		V:           1,
		T0:          t0,
		Nodes:       nodes,
		Topology:    topology,
		Config:      config,
		DecoderName: opts.DecoderName,
		PeerIDs:     opts.PeerIDs,
	}
	data, err := json.Marshal(hdr)
	if err != nil {
		return nil, fmt.Errorf("marshal trace header: %w", err)
	}
	data = append(data, '\n')
	if _, err := bw.Write(data); err != nil {
		return nil, fmt.Errorf("write trace header: %w", err)
	}
	return &TraceWriter{
		w:               bw,
		t0:              t0,
		offset:          len(data),
		indexIntervalUs: defaultIndexInterval,
		index:           [][2]int{{0, len(data)}},
	}, nil
}

// WriteEvent writes a single event as a JSON array tuple.
func (tw *TraceWriter) WriteEvent(ts time.Time, node int, ev string, args ...any) {
	usec := ts.Sub(tw.t0).Microseconds()
	row := make([]any, 0, 3+len(args))
	row = append(row, usec, node, ev)
	row = append(row, args...)
	data, err := json.Marshal(row)
	if err != nil {
		return
	}
	data = append(data, '\n')

	tw.mu.Lock()
	defer tw.mu.Unlock()

	if usec-tw.lastIndexTs >= tw.indexIntervalUs {
		tw.index = append(tw.index, [2]int{int(usec), tw.offset})
		tw.lastIndexTs = usec
	}
	tw.w.Write(data)
	tw.offset += len(data)
}

// Close writes the footer and flushes.
func (tw *TraceWriter) Close() error {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	duration := time.Since(tw.t0).Microseconds()
	footer := traceFooter{End: true, Duration: duration, Index: tw.index}
	data, err := json.Marshal(footer)
	if err != nil {
		return fmt.Errorf("marshal trace footer: %w", err)
	}
	data = append(data, '\n')
	if _, err := tw.w.Write(data); err != nil {
		return fmt.Errorf("write trace footer: %w", err)
	}
	return tw.w.Flush()
}
