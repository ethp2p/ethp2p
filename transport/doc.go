// Package transport lets ethp2p and libp2p share a QUIC endpoint and connections.
// [Conn] hands out quic-go streams, authenticated identities, and byte
// counters. Package ethp2p adapts streams to protocol outcome codes and
// reports resets as its ResetError; transport leaves quic-go stream errors intact.
// The API is QUIC-native, not an abstraction over multiple network transports.
// It does not expose 0-RTT, connection resumption, RTT measurements,
// congestion-control feedback, multipath, or congestion-control selection.
//
// # Specifications
//
// This package implements:
//
//   - QUIC and TLS: [NewEndpoint] with an [Interop] or [Shadow] [Config];
//     the handshake that authenticates [Conn.RemotePeerID].
//   - Streams: the split of bidirectional streams by their first byte, and
//     the stream credit an unread stream holds. Package ethp2p writes and
//     reads every stream header.
//   - Connection lifecycle: the CONNECTION_CLOSE code of a released view
//     ([Conn.CloseWithCode]) and the reading of a peer's close
//     ([ViewClosedError]). Package ethp2p runs the control protocol.
//   - libp2p compatibility: [Endpoint.Libp2p] and the split of each
//     ethp2p_0 connection into two views.
//
// # Shared QUIC endpoint
//
// [NewEndpoint] wraps a packet connection and serves both the libp2p and
// ethp2p views of every ethp2p_0 connection (a legacy libp2p connection
// is libp2p-only: it has no ethp2p view): the startup choreography is start
// once, and shut down in the order below; views are not registered or
// detached dynamically. Lend [Endpoint.Libp2p] to
// libp2p through quicreuse.ConnManager.LendTransport. The application passes
// the ethp2p endpoint to ethp2p.NewStack, registers protocols, and calls Stack.Start.
// Direct users can also call [Ethp2pTransport.Accept] and
// [Ethp2pTransport.Dial]. Listening begins only at [Endpoint.Start],
// which requires both views to have been wired by [Endpoint.Libp2p]
// and [Endpoint.Ethp2p], may be called once, and returns any
// listener error: it is not recoverable, and the application is expected
// to exit. Starting after [Endpoint.Close] fails with [ErrClosed].
// [Libp2pTransport.Listen] succeeds once; later calls fail even after its
// listener is closed.
// [Endpoint.PeerID] and [Endpoint.Addr] report endpoint identity
// and address.
//
// Each side has a delivery
// queue of 16 views; a full queue releases only that side's view, the ethp2p
// view with wire.Unspecified. A failed handshake creates no view and takes no
// queue slot. A view released before delivery sends nothing on its own: while
// the libp2p view keeps the connection open, the peer learns of the release
// only from its own HELLO_TIMEOUT.
//
// [Ethp2pTransport.Accept] and [Ethp2pTransport.Dial] return a view once the
// QUIC handshake completes; the caller runs the control protocol on it.
// Blocked Accept calls end with [ErrClosed] once the endpoint shuts down.
// An in-flight Dial holds the endpoint open:
// [Endpoint.Close] waits for it.
//
// The application owns endpoint shutdown, in a strict sequence:
// Stack.Close first (it releases its views per connection), then protocol
// workers and the libp2p host (closing its listener detaches libp2p and
// releases its queued views as that sequence's terminal libp2p step), then
// [Endpoint.Close], then the supplied packet connection.
// [Libp2pTransport.Close] is a no-op. Stack never closes the endpoint.
// On a shared connection, closing one view releases only
// that view. Its pending and later accept and open calls fail, and
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
// caller's TLS configuration and accepts only libp2p; it ignores the QUIC
// configuration libp2p passes and uses the config's, so a libp2p dial cannot
// differ from an ethp2p dial in QUIC version or transport parameters.
//
// # TLS authentication
//
// The shared listener and [Ethp2pTransport.Dial] authenticate secp256k1 identities
// using the libp2p TLS certificate format. Dial can require a specific [identity.PeerID].
// The handshake verify callback is the security boundary; [Conn.RemotePeerID]
// reports the identity it authenticated. NewEndpoint derives a stable QUIC
// stateless reset key from the private identity key with HKDF-SHA256.
//
// # Connection views
//
// [Libp2pTransport] presents a shared connection to libp2p with the libp2p ALPN,
// whatever was negotiated.
// One dispatcher owns inbound stream acceptance on each shared connection,
// so neither stack accepts directly from the underlying QUIC connection.
// Both views can open outbound streams on that connection.
//
// # Stream allocation
//
// The transport reads no stream header and writes none: every ethp2p stream,
// the control stream included, reaches the caller unread, and
// [Conn.OpenStream] and [Conn.OpenUniStream] return raw streams for the
// caller to write a header on. Unidirectional streams are all ethp2p and are
// queued as QUIC accepts them, in stream ID order, so the peer's first
// unidirectional stream, its control stream, is the first one
// [Conn.NextUniStream] returns. A bidirectional stream goes to libp2p when its
// first byte is 0x13 and to ethp2p otherwise, with that byte left unread; a
// stream whose first byte does not arrive within CLASSIFY_TIMEOUT, or that
// ends first, is reset as the libp2p compatibility spec defines. libp2p receives its
// streams with the multistream-select header intact.
//
// Incoming ethp2p streams have separate queues per direction. [Conn.Streams]
// signals the bidirectional queue and [Conn.NextStream] pulls from it in
// classification completion order. [Conn.UniStreams] signals the
// unidirectional queue and [Conn.NextUniStream] pulls from it in stream ID
// order. A waiting stream holds its stream credit, so a caller that stops
// pulling applies backpressure to the peer instead of growing memory, and the
// transport never resets a stream for lack of queue space. [Conn.Done] closes
// on release and [Conn.CloseCode] then reports the stack code.
//
// [Conn.CloseWithCode] releases the view without writing anything on its
// streams; the caller sends GOAWAY first. Queued streams are reset with
// Closing. View methods then return a [ViewClosedError] with the stack code
// and closure origin; [ErrViewClosed] matches these view closures. If ethp2p
// releases the last physical connection view, CONNECTION_CLOSE carries the
// stack code. A remote application close with a recognized even stack code
// becomes ViewClosedError; odd and unknown codes, local closes, and other QUIC
// errors remain unchanged, and [ConnectionClosed] tells them from stream
// failures. A libp2p-last zero-code close means Unspecified for a still-open
// ethp2p view; it cannot replace a closure already recorded. QUIC datagrams are negotiated but not exposed until the Datagrams spec
// is implemented.
package transport
