package sim

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestTraceWriterWithOptions_WritesDecoderMetadata(t *testing.T) {
	var buf bytes.Buffer
	tw, err := NewTraceWriterWithOptions(
		&buf,
		time.Unix(0, 0),
		[]string{"n0", "n1"},
		Topology{},
		json.RawMessage(`{"name":"gossipsub"}`),
		TraceHeaderOptions{
			DecoderName: "gossipsub",
			PeerIDs:     []string{"peer-a", "peer-b"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	var hdr traceHeader
	line := strings.Split(strings.TrimSpace(buf.String()), "\n")[0]
	if err := json.Unmarshal([]byte(line), &hdr); err != nil {
		t.Fatal(err)
	}
	if hdr.DecoderName != "gossipsub" {
		t.Fatalf("decoderName = %q, want gossipsub", hdr.DecoderName)
	}
	if len(hdr.PeerIDs) != 2 || hdr.PeerIDs[1] != "peer-b" {
		t.Fatalf("peer_ids = %v", hdr.PeerIDs)
	}
}

func TestGossipsubTrace_ProducesExpectedEvents(t *testing.T) {
	var buf bytes.Buffer
	t0 := time.Now()
	tw, err := NewTraceWriterWithOptions(
		&buf,
		t0,
		[]string{"n0", "n1"},
		Topology{},
		json.RawMessage(`{"name":"gossipsub"}`),
		TraceHeaderOptions{DecoderName: "gossipsub", PeerIDs: []string{"peer-a", "peer-b"}},
	)
	if err != nil {
		t.Fatal(err)
	}

	trace := newGossipsubTrace(0, tw)
	channel := gossipsubChannelID
	topicPtr := func(s string) *string { return &s }

	trace.Trace(&pubsubpb.TraceEvent{
		Type: pubsubpb.TraceEvent_ADD_PEER.Enum(),
		AddPeer: &pubsubpb.TraceEvent_AddPeer{
			PeerID: []byte("peer-b"),
			Proto:  topicPtr("/meshsub/1.1.0"),
		},
	})
	trace.Trace(&pubsubpb.TraceEvent{
		Type: pubsubpb.TraceEvent_JOIN.Enum(),
		Join: &pubsubpb.TraceEvent_Join{
			Topic: topicPtr(channel),
		},
	})

	trace.SendRPC(&pubsub.RPC{
		RPC: pubsubpb.RPC{
			Publish: []*pubsubpb.Message{{
				Data:  encodeGossipsubMessage("msg-0", []byte("payload")),
				Topic: topicPtr(channel),
			}},
			Control: &pubsubpb.ControlMessage{
				Ihave:     []*pubsubpb.ControlIHave{{TopicID: topicPtr(channel), MessageIDs: []string{"msg-0"}}},
				Iwant:     []*pubsubpb.ControlIWant{{MessageIDs: []string{"msg-0"}}},
				Idontwant: []*pubsubpb.ControlIDontWant{{MessageIDs: []string{"msg-0"}}},
			},
		},
	}, peer.ID("peer-b"))

	trace.ValidateMessage(&pubsub.Message{
		Message: &pubsubpb.Message{
			Data:  encodeGossipsubMessage("msg-0", []byte("payload")),
			Topic: topicPtr(channel),
		},
		ReceivedFrom: peer.ID("peer-b"),
	})

	trace.Trace(&pubsubpb.TraceEvent{
		Type: pubsubpb.TraceEvent_RECV_RPC.Enum(),
		RecvRPC: &pubsubpb.TraceEvent_RecvRPC{
			ReceivedFrom: []byte("peer-b"),
			Meta: &pubsubpb.TraceEvent_RPCMeta{
				Control: &pubsubpb.TraceEvent_ControlMeta{
					Ihave:     []*pubsubpb.TraceEvent_ControlIHaveMeta{{Topic: topicPtr(channel), MessageIDs: [][]byte{[]byte("msg-0")}}},
					Iwant:     []*pubsubpb.TraceEvent_ControlIWantMeta{{MessageIDs: [][]byte{[]byte("msg-0")}}},
					Idontwant: []*pubsubpb.TraceEvent_ControlIDontWantMeta{{MessageIDs: [][]byte{[]byte("msg-0")}}},
				},
			},
		},
	})
	trace.Trace(&pubsubpb.TraceEvent{
		Type: pubsubpb.TraceEvent_DELIVER_MESSAGE.Enum(),
		DeliverMessage: &pubsubpb.TraceEvent_DeliverMessage{
			ReceivedFrom: []byte("peer-b"),
			Topic:        topicPtr(channel),
			MessageID:    []byte("msg-0"),
		},
	})

	trace.UndeliverableMessage(&pubsub.Message{
		Message: &pubsubpb.Message{
			Data:  encodeGossipsubMessage("msg-0", []byte("payload")),
			Topic: topicPtr(channel),
		},
	})

	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	codes := map[string]int{}
	for _, line := range lines[1 : len(lines)-1] {
		var row []any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("parse row %q: %v", line, err)
		}
		codes[row[2].(string)]++
	}

	for _, code := range []string{"pa", "tj", "ms", "hs", "ws", "ns", "va", "hr", "wr", "nr", "dl", "ud"} {
		if codes[code] == 0 {
			t.Fatalf("expected %s event, got none (codes=%v)", code, codes)
		}
	}
}
