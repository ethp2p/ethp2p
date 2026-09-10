// Package transport lets ethp2p and libp2p share a QUIC endpoint and connections.
// [Conn] provides streams, datagrams, authenticated identities, and byte counters.
// The API is QUIC-native, not an abstraction over multiple network transports.
// It does not expose 0-RTT, connection resumption, RTT measurements,
// congestion-control feedback, multipath, or congestion-control selection.
//
// # Shared QUIC transport
//
// [NewShared] wraps a packet connection. Lend [SharedTransport.Libp2p] to
// libp2p through quicreuse.ConnManager.LendTransport. The application passes
// connections from [Ethp2pTransport.Accept] and [Ethp2pTransport.Dial] to
// ethp2p.Stack.ServeConn. The first [Libp2pTransport.Listen] or
// [Ethp2pTransport.Accept] starts listening.
//
// The application owns endpoint shutdown. [SharedTransport.Close] closes all
// connections and stops listening; [Libp2pTransport.Close] is a no-op and its
// listener rejects Close, because libp2p must not stop the shared endpoint
// while ethp2p still uses it. Stack.ServeConn only borrows connections and
// never closes the endpoint. The caller must also close the supplied packet
// connection. On a shared connection, closing one view releases only that view;
// the underlying QUIC connection closes when both views are released.
//
// # Connection negotiation
//
// [Ethp2pTransport.Dial] and the shared listener prefer ethp2p_0 over libp2p.
// Connections negotiating ethp2p_0 expose both views. Connections negotiating
// libp2p are libp2p-only; [Ethp2pTransport.Dial] returns [ErrDialLegacyPeer] and
// offers the connection to the libp2p listener. [Libp2pTransport.Dial] uses the
// caller's TLS configuration and accepts only libp2p.
//
// # TLS authentication
//
// The shared listener and [Ethp2pTransport.Dial] authenticate secp256k1 identities
// using the libp2p TLS certificate format. Dial can require a specific [PeerID].
// The handshake verify callback is the security boundary; [Conn.RemotePeerID]
// reads back the identity it authenticated from the connection's TLS state.
// [NewQUICConn] instead accepts caller-supplied identity for connections that
// carry no ethp2p certificate.
//
// # Connection views
//
// [Libp2pTransport] presents a shared connection to libp2p with the libp2p ALPN.
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
