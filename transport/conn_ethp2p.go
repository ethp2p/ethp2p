package transport

import "context"

var _ Conn = (*connEth)(nil)

// connEth is the authenticated ethp2p view of one routed QUIC connection. It
// receives classified streams from the ethp2p queues, shares the raw
// connection with the libp2p view, and implements the Conn interface.
type connEth struct {
	sc   *sharedConn
	dir  ConnDir
	auth AuthInfo
}

func (c *connEth) AuthInfo() AuthInfo { return c.auth }
func (c *connEth) Direction() ConnDir { return c.dir }

func (c *connEth) OpenStream(ctx context.Context) (Stream, error) {
	s, err := c.sc.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return stream{s}, nil
}

func (c *connEth) AcceptBiStream(ctx context.Context) (Stream, error) {
	select {
	case s := <-c.sc.ethp2pBi:
		return stream{s}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.sc.conn.Context().Done():
		return nil, c.sc.conn.Context().Err()
	}
}

func (c *connEth) OpenUniStream(ctx context.Context) (SendStream, error) {
	s, err := c.sc.conn.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return sendStream{s}, nil
}

func (c *connEth) AcceptUniStream(ctx context.Context) (ReceiveStream, error) {
	select {
	case s := <-c.sc.ethp2pUni:
		return receiveStream{s}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.sc.conn.Context().Done():
		return nil, c.sc.conn.Context().Err()
	}
}

func (c *connEth) SendDatagram(_ context.Context, payload []byte) error {
	return c.sc.conn.SendDatagram(payload)
}

func (c *connEth) RecvDatagram(ctx context.Context) ([]byte, error) {
	return c.sc.conn.ReceiveDatagram(ctx)
}

func (c *connEth) Close() error {
	// Mark only; the raw connection closes once the libp2p view closes too.
	c.sc.closeSide(sideEthp2p, appNoError, "closed")
	return nil
}

func (c *connEth) SupportsStreams() bool { return true }

func (c *connEth) SupportsDatagrams() bool {
	support := c.sc.conn.ConnectionState().SupportsDatagrams
	return support.Local && support.Remote
}

func (c *connEth) ConnectionStats() (uint64, uint64) {
	s := c.sc.conn.ConnectionStats()
	return s.BytesSent, s.BytesReceived
}
