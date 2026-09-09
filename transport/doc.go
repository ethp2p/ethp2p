// Package transport lets ethp2p and libp2p share a QUIC endpoint and connections.
// [Conn] provides streams, datagrams, authenticated identities, and byte counters.
// The API is QUIC-native, not an abstraction over multiple network transports.
// It does not expose 0-RTT, connection resumption, RTT measurements,
// congestion-control feedback, multipath, or congestion-control selection.
//
// # Shared QUIC transport
//
// [NewShared] wraps a packet connection. Lend [TransportShared.Libp2p] to
// libp2p through quicreuse.ConnManager.LendTransport. The application passes
// connections from [TransportEth.Accept] and [TransportEth.Dial] to
// ethp2p.Stack.ServeConn. The first [TransportLib.Listen] or [TransportEth.Accept]
// starts listening.
//
// The application owns endpoint shutdown. [TransportShared.Close] closes all
// connections and stops listening; [TransportLib.Close] and its listener's
// Close are no-ops. Stack.ServeConn only borrows connections and never closes
// the endpoint. The caller must also close the supplied packet connection.
// On a shared connection, closing one view releases only that view; the
// underlying QUIC connection closes when both views are released.
//
// # Connection negotiation
//
// [TransportEth.Dial] and the shared listener prefer ethp2p_0 over libp2p.
// Connections negotiating ethp2p_0 expose both views. Connections negotiating
// libp2p are libp2p-only; TransportEth.Dial returns [ErrDialLegacyPeer] and
// offers the connection to the libp2p listener. [TransportLib.Dial] uses the
// caller's TLS configuration and accepts only libp2p.
//
// # TLS authentication
//
// The shared listener and [TransportEth.Dial] authenticate secp256k1 identities
// using the libp2p TLS certificate format. Dial can require a specific [PeerID].
// [Conn.AuthInfo] exposes the authenticated identities and remote public key.
// [NewQUICConn] instead trusts caller-supplied authentication metadata.
//
// # Connection views
//
// TransportLib presents a shared connection to libp2p with the libp2p ALPN.
// One dispatcher owns inbound stream acceptance on each shared connection,
// so neither stack accepts directly from the underlying QUIC connection.
// Both views can open outbound streams on that connection.
//
// # Stream allocation
//
// Incoming bidirectional streams route to libp2p if the first length-delimited
// frame's payload starts with '/'; other nonempty payloads route to ethp2p.
//
// Classification leaves all bytes available to the receiving stack. It reads
// only the length prefix and first payload byte, not the full frame. Invalid
// prefixes, empty payloads, classification timeouts, and full delivery queues
// reset only the affected stream.
//
// All incoming unidirectional streams route to ethp2p because libp2p does not
// open unidirectional QUIC streams. QUIC datagrams are exposed only through the
// ethp2p Conn and do not require stream classification.
package transport
