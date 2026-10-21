---
status: implemented
requires:
  - ../00-foundations/05-identity.md
---

# QUIC and TLS

This document fixes the QUIC and TLS 1.3 parameters every ethp2p connection uses, the certificate that authenticates each peer, and the QUIC facts the other transport specs rely on: how streams are limited and returned, and how a connection ends.
Everything else about QUIC is as in [RFC 9000] and [RFC 9001].
Identity keys and peer IDs are defined in [Identity](../00-foundations/05-identity.md); what each application protocol name means for a connection is in [libp2p compatibility](04-libp2p-compat.md).
Notation and the meaning of MUST and SHOULD are in [Conventions](../00-foundations/02-conventions.md); defined terms are in [Terminology](../00-foundations/03-terminology.md).

## Goals

- ethp2p and libp2p share one UDP socket and one QUIC connection per peer.
- A stock libp2p peer authenticates an ethp2p endpoint, and the reverse, with no extra step.
- A peer cannot make an endpoint buffer more than the stream limits and windows allow.

## Wire format

**Handshake.**
Every connection is QUIC version 1 secured by TLS 1.3 with a certificate from each side.
The dialer offers two application protocol names; the one the handshake selects decides what the connection is ([libp2p compatibility](04-libp2p-compat.md)).

```text
dialer                                       listener
  | -- ClientHello -------------------------->|   ALPN ethp2p_0, libp2p
  |<-- ServerHello, CertificateRequest -------|   ALPN picks one
  |<-- Certificate (one) ---------------------|
  | -- Certificate (one) -------------------->|
  |<-- Finished -- Finished ----------------->|
```

**Certificate.**
Each endpoint presents one self-signed X.509 v3 certificate for a fresh P-256 key, the certificate key.
The identity key signs the certificate key once, and the signature travels in a certificate extension; this is the [libp2p TLS] format.

| Field | Value |
| ----- | ----- |
| Key | Fresh P-256 key pair |
| Serial numbers | Random |
| `NotBefore` | One hour before creation |
| `NotAfter` | `CERT_VALIDITY_PERIOD` (100 × 365 days) after creation |
| Chain | Exactly one certificate |
| Extensions | One with OID `EXTENSION_ID` (`1.3.6.1.4.1.53594.1.1`) |

The extension value is the DER encoding of `SignedKey` ([Identity](../00-foundations/05-identity.md#certificate-binding)):

```text
signed_input = CERTIFICATE_PREFIX || spki         # spki: DER SubjectPublicKeyInfo of the certificate key
SignedKey    = SEQUENCE { publicKey, signature }  # publicKey: the identity PublicKey message
                                                  # signature: DER ECDSA over sha256(signed_input)
```

**Transport parameters.**
Shared by every connection, dialed or accepted, ethp2p or libp2p:

| Parameter | Value |
| --------- | ----- |
| QUIC version | 1 only |
| TLS version | 1.3 only |
| Application protocol names, offer order | `ethp2p_0`, then `libp2p` |
| QUIC datagrams ([RFC 9221]) | Enabled |
| Reset with partial delivery ([RESET-PARTIAL]) | Enabled |
| Session tickets, resumption, 0-RTT | None |

Parameters that vary by profile:

| Parameter | `Interop` | `Shadow` |
| --------- | --------- | -------- |
| Incoming bidirectional streams | 256 | 16384 |
| Incoming unidirectional streams | 256 | 16384 |
| Stream receive window, maximum | 10 MiB | 6 MiB |
| Connection receive window, maximum | 15 MiB | 15 MiB |
| Keep-alive period | 15 seconds | none |
| Idle timeout | 30 seconds | 365 days |
| Path MTU discovery | Enabled | Disabled |

Initial windows are 512 KiB and grow by auto-tuning up to the maximum.
`Interop` is for a node that talks to stock libp2p peers; `Shadow` is for the Shadow network simulator.

**Error codes.**
Application error codes in `RESET_STREAM`, `STOP_SENDING` and `CONNECTION_CLOSE` (type `0x1d`) are outcome codes ([Outcome codes](../00-foundations/06-outcome-codes.md)).
A TLS alert that ends the handshake arrives as transport error `0x100 + alert`; a rejected certificate is `bad_certificate` (42), transport error `0x12a`.

A connection ends in one of four ways:

```text
application close   CONNECTION_CLOSE 0x1d with an application error code
transport error     CONNECTION_CLOSE 0x1c with a transport error code (TLS alert, protocol error)
idle timeout        nothing received within the timeout
stateless reset     a packet that ends in a reset token
```

**Constants.**

| Name | Value |
| ---- | ----- |
| `CERT_VALIDITY_PERIOD` | 100 × 365 days |
| `EXTENSION_ID` | `1.3.6.1.4.1.53594.1.1` |
| `CERTIFICATE_PREFIX` | `libp2p-tls-handshake:` |
| `HANDSHAKE_IDLE_TIMEOUT` | 5 seconds |

## Behavior

**Versions.**
An endpoint MUST negotiate QUIC version 1 only and MUST NOT offer or accept another version, so every connection is one libp2p can share.
An endpoint MUST require TLS 1.3 and MUST NOT enable session tickets, resumption or 0-RTT, so every connection runs the full certificate check.

**Application protocol.**
A dialer that connects as ethp2p MUST offer `ethp2p_0` first and `libp2p` second.
A listener MUST prefer `ethp2p_0` and MUST accept `libp2p` from a peer that does not offer `ethp2p_0`, so stock libp2p peers still connect.

**Certificate.**
An endpoint MUST create one certificate when it starts and use it for every connection, dialed and accepted.
A listener MUST request a client certificate; otherwise the dialer is not authenticated.
The identity key MUST sign only the certificate binding, never a TLS handshake transcript.

**Verifying the peer.**
Standard chain verification is replaced by these steps, run once per connection on both sides.
The handshake MUST fail unless all of them pass:

1. The peer presented exactly one certificate, and it parses as X.509.
2. The certificate carries the extension `EXTENSION_ID` (`1.3.6.1.4.1.53594.1.1`).
3. The certificate is valid with itself as the only root, at the current time; the identity extension does not count as an unhandled critical extension.
4. The extension decodes as `SignedKey`, and `publicKey` decodes as a secp256k1 `PublicKey` ([Identity](../00-foundations/05-identity.md)).
5. `signature` verifies under that key over `signed_input`.
6. If the dialer expects a peer ID, the peer ID derived from the authenticated key equals it.

On success the connection's remote identity is the authenticated key, fixed for the life of the connection.
A dialer compares an expected peer ID with the one derived from the authenticated key, never with a value the peer sent.

**Datagrams and partial reset.**
An endpoint MUST advertise both in its transport parameters on every connection.
Advertising obliges neither side to use them; nothing sends datagrams yet ([Datagrams](05-datagrams.md)) and no protocol uses partial reset.

**Streams.**
An endpoint MUST NOT open more streams than its peer has granted credit for; an open waits for credit.
A stream returns its credit only when it has finished at both ends: a unidirectional stream when its receive side is read to the end or reset, a bidirectional stream when both directions are.
A reset returns credit at once, so an endpoint that closes a connection MUST close it before it resets the streams it has not handed to a protocol; otherwise the resets give the peer credit to open more.

**Flow control.**
Stream and connection receive windows are QUIC's own and shared by every protocol on the connection.
The profile's maximum windows cap what an endpoint buffers per stream and per connection.

**Idle timeout and keep-alive.**
The idle timeout is the smaller of the two endpoints' values and applies after the handshake.
An unanswered handshake is abandoned after `HANDSHAKE_IDLE_TIMEOUT` (5 seconds).
An endpoint with a keep-alive period sends a packet at the smaller of that period and half the negotiated idle timeout.

**Ending a connection.**
An endpoint MUST treat an idle timeout and a stateless reset as the end of the connection, not as a violation by the peer.
An endpoint MUST answer a packet for a connection it no longer has with a stateless reset.
Its reset key SHOULD be derived from the identity key ([Identity](../00-foundations/05-identity.md#stateless-reset-key)), so a restarted endpoint resets its old connections within one round trip.

**Changes.**
A change to the QUIC version, the TLS version, the certificate or the application protocol offer ships with a new application protocol name offered before `ethp2p_0`; a listener that still accepts `ethp2p_0` keeps older dialers working.
Profile values are local and need no negotiation.

## Failure modes

**No common application protocol name.**
The handshake fails on both sides, so no connection forms that neither side can use.

**Unauthenticated peer.**
Certificate verification runs inside the handshake, and streams exist only after it completes, so a peer that fails it reaches no stream.

**Bad certificate.**
A dialer that rejects the listener's certificate fails the dial.
A listener that rejects the dialer's certificate sends `bad_certificate`, which the dialer sees as transport error `0x12a`, possibly just after its handshake completed.

**Pinning and disclosure.**
In TLS 1.3 the dialer verifies the listener's certificate before it sends its own, so a dialer that expects a peer ID aborts on a mismatch before it presents a certificate.
A dialer that expects none presents its certificate, and so its identity, to any listener whose certificate verifies; this follows from the message order and is not tested.

**Clock skew.**
The certificate's `NotBefore` is one hour in the past, so a peer whose clock runs more than an hour behind the creator's rejects the certificate.

**Streams that never finish.**
A peer can open at most the profile's incoming stream limit per kind (256 under `Interop`), and the receive windows bound what each can make the endpoint buffer, so open streams cost bounded memory.
Streams that wait to be read are bounded further in [Streams](02-streams.md).

**Off-path reset.**
A reset token depends on the connection ID and the reset key, so an attacker without the key cannot reset a connection.

## Rationale

**QUIC version 1 only.**
To share one connection with libp2p, the version must be one libp2p accepts, and libp2p's QUIC transport rejects every other.
This departs from QUIC's version negotiation; offering another version would only let a connection form that libp2p cannot share.

**The libp2p certificate.**
To let a stock libp2p peer authenticate an ethp2p endpoint with no extra step, ethp2p uses libp2p's certificate unchanged, including its 100-year `NotAfter`.
The certificate key is throwaway and the identity key signs only the binding, so a compromised TLS session never yields an identity signature over a transcript.

**Symmetric stream limits.**
Stock libp2p allows 256 bidirectional and 5 unidirectional streams, because it opens no unidirectional ones.
An ethp2p node opens a unidirectional stream for the control protocol and one per broadcast control channel; at the libp2p limit eight parallel broadcasts delivered nothing, and with the limit raised to 256 they completed in 0.31 seconds.

**Backpressure through credit.**
To bound what a peer can make an endpoint buffer, credit is held until a stream finishes.
A reset is cheap for the receiver but hands credit back at once, which would let the peer open the next stream immediately.

**Datagrams enabled before they are used.**
Datagram support is fixed at the handshake.
Enabling it now lets the datagram protocol start later without a new application protocol name.

**Shadow profile.**
Shadow cannot set the don't-fragment bit, so path MTU discovery is off; the profile also raises stream limits for large topologies and lengthens the idle timeout.

## Reference implementation

Package `transport` ([docs]).
Tests that check this spec: `interop_test.go` (stock libp2p, bad certificates, wrong peer ID, no common name, window growth), `credit_test.go` and `uni_backpressure_test.go` (stream credit), `view_test.go` (stateless reset after a restart).

[RFC 9000]: https://www.rfc-editor.org/rfc/rfc9000
[RFC 9001]: https://www.rfc-editor.org/rfc/rfc9001
[RFC 9221]: https://www.rfc-editor.org/rfc/rfc9221
[RESET-PARTIAL]: https://datatracker.ietf.org/doc/draft-ietf-quic-reliable-stream-reset/
[libp2p TLS]: https://github.com/libp2p/specs/blob/master/tls/tls.md
[docs]: https://pkg.go.dev/github.com/ethp2p/ethp2p/transport
