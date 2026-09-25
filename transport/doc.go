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
// the ethp2p endpoint to ethp2p.NewStack, registers protocols, and calls Stack.Start.
// Stack.Start calls [Ethp2pTransport.SetHello] to install the local Hello and
// start listening. Direct users can also call [Ethp2pTransport.Accept] and
// [Ethp2pTransport.Dial]. The first Accept, [Ethp2pTransport.Listen], or
// [Libp2pTransport.Listen] starts listening.
// [SharedTransport.PeerID], [SharedTransport.PublicKey], and
// [SharedTransport.Addr] report endpoint identity and address without
// registering interest in either connection view.
//
// Calling [SharedTransport.Libp2p] registers interest in libp2p views;
// [Ethp2pTransport.SetHello] registers ethp2p interest. Until then,
// the transport releases
// that side's view of every new connection immediately, so a process that
// never uses a side never accumulates its views. Configure Hello before
// connections arrive. Connection delivery has a bounded queue;
// a full queue releases only that side's view with wire.Unspecified.
//
// [Ethp2pTransport.Accept] and [Ethp2pTransport.Dial] return views only after
// the peer's Hello arrived and was validated. [Ethp2pTransport.Close] stops
// ethp2p view delivery: it clears ethp2p interest, releases queued views, and
// wakes blocked Accept calls with [ErrClosed], while views already returned
// and libp2p continue unaffected.
//
// The application owns endpoint shutdown. [SharedTransport.Close] closes all
// connections and stops listening. [Ethp2pTransport.Close] detaches ethp2p and
// releases its queued views while libp2p continues. [Libp2pTransport.Close] is
// a no-op; closing
// its listener detaches libp2p and releases its queued views while ethp2p
// continues. Close Stack first, then protocol workers and the libp2p host,
// then SharedTransport, then the supplied packet connection. Stack never
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
// reports the identity it authenticated. NewShared derives a stable QUIC
// stateless reset key from the private identity key with HKDF-SHA256.
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
// length followed by the selector's unsigned-varint encoding. The dispatcher
// owns selector zero as one outbound and one inbound control stream per view.
// [Ethp2pTransport.SetHello] snapshots the Hello sent first on each new view;
// [Conn.PeerHello] returns a copy of the peer's validated Hello without blocking.
// [Conn.Streams] signals newly classified streams and [Conn.NextStream] pulls
// them in classification completion order; [Conn.Done] closes on release and
// [Conn.CloseCode] then reports the stack code.
// [Conn.CloseWithCode] sends GoAway and FIN before releasing the view. A peer
// GoAway, FIN, or reset also releases the view. View methods then return a
// [ViewClosedError] with the stack code and closure origin; [ErrViewClosed]
// matches these view closures. If ethp2p releases the last physical connection
// view, CONNECTION_CLOSE carries the same stack code as GoAway. A remote
// application close with a recognized even stack code becomes ViewClosedError;
// odd and unknown codes, local closes, and other QUIC errors remain unchanged.
// GoAway stays best-effort and the peer's closure is answered with FIN alone.
// A libp2p-last zero-code close means Unspecified for a still-open ethp2p view;
// it cannot replace a GoAway closure already observed. Dial before SetHello
// returns [ErrNoHello]; Accept waits for a validated view.
//
// For each incoming bidirectional stream, the dispatcher checks the first byte.
// Bytes 1 through 10 begin an ethp2p selector frame. Otherwise a second byte
// of '/' identifies libp2p, and both bytes remain available to libp2p. All
// other heads are reset with wire.BadSelector. Unidirectional streams are
// always ethp2p because libp2p opens none.
//
// The dispatcher validates the complete selector frame under one
// five-second deadline, then returns the selector with the ethp2p stream.
// Independent per-connection bounds limit bidirectional and unidirectional
// classification. Classified non-control streams wait in one per-connection queue,
// in classification completion order across both directions, until pulled with
// [Conn.NextStream]. Each holds stream credit until it completes,
// so QUIC applies backpressure without resetting streams when a queue grows.
// Uni classification only peeks at the selector; the receive adapter skips it on
// its first read, so even a selector-only stream retains credit until read.
// Classifiers never wait for delivery queue space. [Conn.OpenStream] and
// [Conn.OpenUniStream] write the selector frame before returning. QUIC
// datagrams do not require stream classification.
package transport
