// Package ethp2p runs ethp2p protocols over a shared QUIC endpoint: it
// registers protocol families, connects to peers, and delivers each family its
// peers and streams as ordered events.
//
// # Specifications
//
// This package implements:
//
//   - Streams: [Stack.Register] declares a family by its code and protocols,
//     the stack reads every incoming stream header and routes the stream,
//     [Family.Next] delivers the streams routed to a family,
//     [Peer.OpenStream] and [Peer.OpenUniStream] open streams and write their
//     headers, and [Peer.Close] stops serving a family to one peer.
//   - Control protocol: the stack opens each view's control stream and
//     sends the registered family codes and [Config.Record] in HELLO, reads
//     the peer's HELLO and GOAWAY, and sends GOAWAY when it closes a view;
//     [Peer.Record] returns the peer's record.
//   - Connection lifecycle: [Stack.Connect] and the accept loop admit views
//     under the duplicate rule; [Stack.Disconnect] and [Stack.Close] close them.
//
// The connection itself (QUIC, TLS, the split between ethp2p and libp2p
// streams) is package transport's.
//
// # Setup and shutdown
//
// Build the endpoint with transport.NewEndpoint, lend its libp2p transport
// to the libp2p host, pass its ethp2p transport to [NewStack], register
// every family with [Stack.Register], call Endpoint.Start, then
// [Stack.Start]. Registration freezes when Start begins. Start builds the
// HELLO every view sends from the sorted codes of all registered families,
// plus [Config.Record] if set; it may be called once. Listener errors are
// reported by Endpoint.Start, not by Start.
//
// The stack decides nothing about whom to dial, when to retry or how many peers
// to keep. Discovery, retries, limits and scoring belong to the application,
// which layers them on Connect, Disconnect and PeerUp.
//
// [Stack.Close] cancels in-flight dials, stops accepting views, closes every
// view with wire.Closing, and waits for every
// stack goroutine. It returns after every view the stack admitted has been
// released, and never closes the endpoint or the socket. Shut down in this
// order: Stack.Close, protocol workers, the libp2p host, the shared endpoint,
// the packet connection.
//
// # Connecting
//
// [Stack.Connect] dials the record's QUIC endpoints in order, requiring the
// record's identity, until one view is admitted. If the peer already has a
// view, it only updates the peer's record. It returns nil when the peer has a
// view at the end, including one that beat the attempt's own view under the
// duplicate rule. A peer that speaks only libp2p yields
// transport.ErrDialLegacyPeer and stops the remaining endpoints; the stack
// remembers nothing about the peer, so retry policy is the caller's.
// Otherwise Connect returns the joined dial and rejection errors, or
// [ErrDisconnected] if [Stack.Disconnect] abandoned the attempt. Concurrent
// Connect calls for one peer share one dial driven by the first caller; if
// that caller's context ends, a waiter with a live context starts a new one.
//
// Each view writes our HELLO before reading the peer's. Both operations share
// HELLO_TIMEOUT, and caller cancellation ends either one. A HELLO write that
// times out resets the control stream and releases the view with wire.Timeout.
//
// Dialed and accepted views pass the same admission, in order: the HELLO
// exchange, the record check, at least one shared family, an open stack, a dial not
// abandoned by Disconnect, and the duplicate rule. A view that passes gets a
// [ConnID], and each shared family gets a [Peer] handle and a [PeerUp].
//
// [Stack.Disconnect] cancels any dial to the peer, closes its view with
// wire.Closing and returns once the view is released. It is not a ban: the
// peer can dial again. [Stack.Connections] lists connected views, and
// [Stack.Traffic] reports QUIC bytes, including libp2p bytes on shared
// connections.
//
// # Families and events
//
// A family is registered with a wake channel of capacity at least one;
// families may share one. Each [ProtocolSpec] names a protocol number within
// the family and MaxQueued, the number of streams per peer that may wait for
// [Family.Next]. Streams over it are reset with wire.Unspecified. MaxQueued is
// never sent to the peer. A family is shared with a peer when the peer's HELLO
// lists its code; a stream for a protocol the family did not register is reset
// with wire.UnsupportedSelector.
//
// The stack signals the wake channel without blocking after every enqueue, so
// a consumer must call [Family.Next] until it reports none before waiting.
// Next serves peers in turn, one event per turn. For each handle it returns
// [PeerUp] first, then [StreamIn] events in the order their stream headers
// were read, then exactly one [PeerDown] carrying the view's close code or the
// code passed to [Peer.Close]. A handle whose PeerUp was never returned yields
// no events. The handle's context ends before its PeerDown is visible. Streams
// still queued when a handle ends are reset and never returned: with
// wire.Closing when the view closed, and with wire.UnsupportedSelector when the
// handle was closed locally.
//
// [Event.Reject] refuses a StreamIn with wire.Refused, and [Event.Cancel]
// resets it with any code. A consumer that stops should dispose of unhandled
// events with Cancel(wire.Unspecified) and keep Reject for deliberate
// refusals. Admission runs no consumer code: a consumer decides whether it
// wants a peer after PeerUp, and stops serving it with [Peer.Close].
//
// # Peer handles and streams
//
// A [Peer] belongs to one family and one view; a new view yields new handles.
// [Peer.OpenStream] and [Peer.OpenUniStream] wait for stream credit and stop
// at the caller's context or the handle's end. They never return a stream on
// an ended handle, they panic for a protocol the family does not own, and they
// do not wait on incoming delivery. [Peer.Close] resets the handle's queued
// streams, ends its context, queues PeerDown, and closes the view with
// wire.NoSharedProtocols once no family holds it. Streams already returned
// stay the caller's.
//
// Streams never cancel themselves. Read, Write and Close return their error
// and leave every side open, so a timed-out stream can have its deadline
// extended and be used again. The caller must close or cancel each side it
// abandons, including after decode and I/O errors. Read returns a peer's reset
// as a [ResetError]; Write returns the transport's error unchanged.
package ethp2p
