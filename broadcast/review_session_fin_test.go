package broadcast

import (
	"bytes"
	"context"
	"fmt"
	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
	"github.com/ethp2p/ethp2p/transport"
	"testing"
	"time"
)

func TestCapacityReviewFinishedSessionsDoNotBoundLiveState(t *testing.T) {
	ctx := context.Background()
	p := newPeerConn(&Engine{ctx: ctx, config: EngineConfig{Observer: NoOpObserver{}, MaxLiveSessionsPerPeer: 16}}, ctx, "peer", nil)
	defer p.Close()
	p.ready = true
	p.finishHandshake(true, nil)
	inbox := make(chan channelEvent, 4)
	tr := newTestChannel(inbox)
	tr.id = "channel"
	p.BindChannel("channel", tr.delivery)
	tr.sessions = make(map[MessageID]*session[*testChunk, *testRouting])
	tr.members = map[transport.PeerID]*PeerConn{p.id: p}
	tr.scheme.NewP = func() *testPreamble { return new(testPreamble) }
	tr.scheme.NewRelay = func(MessageID, *testPreamble) (Strategy[*testChunk, *testRouting], error) {
		return newMockStrategy(), nil
	}
	f := newOutcomeFixture(t)
	for i := range 150 {
		var wire bytes.Buffer
		_ = WriteFrame(&wire, &bcastpb.Sess{Frame: &bcastpb.Sess_SessionOpen{SessionOpen: &bcastpb.Sess_Open{Channel: "channel", MessageId: fmt.Sprint(i)}}})
		raw, in := f.incoming(t, SESS, &wire)
		_ = raw.out.Close()
		p.acceptSession(in)
		tr.handle(capacityTake(t, inbox))
		p.wg.Wait()
	}
	t.Logf("live sessions=%d SESS readers=%d lifecycle=%d", len(tr.sessions), len(p.sessionSem), p.lifecycle.items.Len())
	if len(tr.sessions) != 16 || len(p.sessionSem) != 0 || len(p.liveSessions) != 16 {
		t.Fatal("completed SESS exceeded live-session cap")
	}
	// Unlike completed SESS, an unfinished excess SESS observes wire Unspecified.
	var wire bytes.Buffer
	_ = WriteFrame(&wire, &bcastpb.Sess{Frame: &bcastpb.Sess_SessionOpen{SessionOpen: &bcastpb.Sess_Open{Channel: "channel", MessageId: "excess"}}})
	raw, in := f.incoming(t, SESS, &wire)
	p.acceptSession(in)
	tr.handle(capacityTake(t, inbox))
	requireWireCancelCode(t, raw, 0)
	p.wg.Wait()
	for _, s := range tr.sessions {
		s.createdAt = time.Now().Add(-activeSessionTTL - time.Second)
	}
	tr.cleanup()
	if len(p.liveSessions) != 0 {
		t.Fatal("TTL retained leases")
	}
	for _, id := range []string{"disposed", "replaced", "peer-close"} {
		wire.Reset()
		_ = WriteFrame(&wire, &bcastpb.Sess{Frame: &bcastpb.Sess_SessionOpen{SessionOpen: &bcastpb.Sess_Open{Channel: "channel", MessageId: id}}})
		raw, in := f.incoming(t, SESS, &wire)
		_ = raw.out.Close()
		p.acceptSession(in)
		tr.handle(capacityTake(t, inbox))
		p.wg.Wait()
		if len(p.liveSessions) != 1 {
			t.Fatal("released capacity was not reusable")
		}
		if id == "replaced" {
			errCh := make(chan error, 1)
			tr.handlePublish(channelPublish{messageID: MessageID(id), strategy: newMockStrategy(), errCh: errCh})
			if err := <-errCh; err != nil {
				t.Fatal(err)
			}
			if len(p.liveSessions) != 0 {
				t.Fatal("replacement retained creator lease")
			}
			tr.disposeSession(MessageID(id), "test")
		} else if id == "disposed" {
			tr.disposeSession(MessageID(id), "test")
		} else {
			p.Close()
			for len(inbox) > 0 {
				tr.handle(<-inbox)
			}
		}
		if len(p.liveSessions) != 0 || len(tr.sessions) != 0 {
			t.Fatal("disposal retained creator session")
		}
	}
	tr.shutdown()
}
