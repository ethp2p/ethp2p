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
// [SharedTransport.PeerID], [SharedTransport.PublicKey], and
// [SharedTransport.Addr] report endpoint identity and address without
// registering interest in either connection view.
//
// Calling [SharedTransport.Libp2p] or [SharedTransport.Ethp2p] registers
// interest in that side's connection views. Until then, the transport releases
// that side's view of every new connection immediately, so a process that
// never uses a side never accumulates its views. Request both transports
// before connections arrive. Each interested side has a bounded delivery
// queue; a full queue releases only that side's view.
//
// The application owns endpoint shutdown. [SharedTransport.Close] closes all
// connections and stops listening. [Libp2pTransport.Close] is a no-op; closing
// its listener detaches libp2p and releases its queued views while ethp2p
// continues. Close the libp2p host first, then SharedTransport, then the
// supplied packet connection. Stack.ServeConn borrows connections and never
// closes the endpoint. On a shared connection, closing one view releases only
// that view. Its pending and later accept, open, and datagram calls fail, and
// streams arriving for it are reset. Streams already handed out remain usable
// until both views are released and the underlying QUIC connection closes.
// Operations after endpoint shutdown and Accept on a detached libp2p listener
// return [ErrClosed].
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
// reports the identity it authenticated.
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
// Every ethp2p stream begins with a selector frame: an unsigned-varint payload
// length followed by the selector's unsigned-varint encoding. The first
// unidirectional stream from each endpoint is its selector advertisement; it
// begins with the reserved advertisement selector frame, then carries the
// endpoint's selector frames until FIN.
//
// The shared dispatcher classifies only incoming bidirectional streams. It
// routes one to libp2p when the first frame's payload starts with '/', as
// libp2p multistream-select frames do; other nonempty payloads route to ethp2p.
//
// Classification leaves all bytes available to the receiving stack. It reads
// only the length prefix and first payload byte, not the full frame. Invalid
// prefixes, empty payloads, classification timeouts, and full delivery queues
// reset only the affected stream.
//
// Incoming unidirectional streams route directly to ethp2p because libp2p does
// not open unidirectional QUIC streams. QUIC datagrams are exposed only through
// the ethp2p Conn and do not require stream classification.
package transport
