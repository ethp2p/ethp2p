package broadcast

import (
	"context"
	"testing"

	bcastpb "github.com/ethp2p/ethp2p/broadcast/pb"
)

func TestAcceptSessionRejectsFullHandlerBudgetAsUnspecified(t *testing.T) {
	peer := &PeerConn{
		ctx:        context.Background(),
		sessionSem: make(chan struct{}, 1),
	}
	peer.sessionSem <- struct{}{}

	raw, stream := newOutcomeFixture(t).incoming(t, nil)
	peer.acceptSession(stream, &bcastpb.Sess_Open{})
	requireCode(t, raw.CancelReadCodes(), 0)
}
