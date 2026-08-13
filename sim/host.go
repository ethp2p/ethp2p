// package sim is a simple implementation of a host using
// quic for transport. It's provided here to explain the
// transport capabilities required to drive `broadcast.Broadcaster`
// and must not be used in production.
package sim

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3/qlog"
)

// QUICHost is QUIC transport and listener for ec broadcast. The host is provided
// for illustrating the transport requirements of an application that uses `broadcast.Broadcaster`.
type QUICHost struct {
	Transport *quic.Transport
	Listener  *quic.Listener
	UDPAddr   *net.UDPAddr
	Config    *quic.Config
}

// NewQUICHost creates a new `QUICHost`. The returned host listens for new
// connections on conn.
func NewQUICHost(conn net.PacketConn) (QUICHost, error) {
	transport, ln, err := makeQUICTransportAndListener(conn)
	if err != nil {
		return QUICHost{}, err
	}
	return QUICHost{
		Transport: transport,
		Listener:  ln,
		UDPAddr:   ln.Addr().(*net.UDPAddr),
		Config:    defaultQuicConfig.Clone(),
	}, nil
}

// Dial dials the host at `addr`.
func (q *QUICHost) Dial(ctx context.Context, addr net.Addr, conf *quic.Config) (*quic.Conn, error) {
	tlsConf := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"eth-ec-broadcast"}}
	if conf == nil {
		conf = q.Config.Clone()
	}
	return q.Transport.Dial(ctx, addr, tlsConf, conf)
}

// Accept accepts a new connection.
func (q *QUICHost) Accept(ctx context.Context) (*quic.Conn, error) {
	return q.Listener.Accept(ctx)
}

func (q *QUICHost) Close() error {
	return errors.Join(q.Transport.Close(), q.Listener.Close(), q.Transport.Conn.Close())
}

var defaultQuicConfig = quic.Config{
	MaxIdleTimeout:          365 * 24 * time.Hour, // quic-go rejects math.MaxInt64; 1-year effectively disables
	MaxIncomingStreams:      16384,                // per-chunk streams at scale need headroom
	MaxIncomingUniStreams:   16384,
	DisablePathMTUDiscovery: true, // Required for Shadow simulator (disables DF bit)
	Tracer:                  qlog.DefaultConnectionTracer,
}

func makeQUICTransportAndListener(conn net.PacketConn) (*quic.Transport, *quic.Listener, error) {
	transport := &quic.Transport{Conn: conn}
	tls, err := generateTLSConfig()
	if err != nil {
		return nil, nil, err
	}
	conf := defaultQuicConfig.Clone()
	ln, err := transport.Listen(tls, conf)
	if err != nil {
		return nil, nil, err
	}
	return transport, ln, nil
}

// generateTLSConfig returns a barebones TLS config.
func generateTLSConfig() (*tls.Config, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, priv.Public(), priv)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{certDER},
			PrivateKey:  priv,
		}},
		NextProtos: []string{"eth-ec-broadcast"},
	}, nil
}
