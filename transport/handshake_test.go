package transport

import (
	"errors"
	"testing"
)

// The handshake verify callback is the single place that authenticates a remote
// certificate, and it records the resulting key into a per-connection identity
// slot so Conn.RemotePeerID derives the peer ID from it rather than
// re-authenticating. These tests pin that contract: the slot is filled on
// success and left empty on failure, which is what keeps authentication to one
// certificate parse and one signature check per connection.

func TestVerifyRecordsRemoteKeyInSlot(t *testing.T) {
	ours, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}

	slot := &remoteIdentitySlot{}
	config := ours.connConfig(slot, theirs.peerID)

	raw := theirs.config.Certificates[0].Certificate[0]
	if err := config.VerifyPeerCertificate([][]byte{raw}, nil); err != nil {
		t.Fatalf("verify rejected a valid certificate: %v", err)
	}
	if slot.key == nil {
		t.Fatal("slot holds no key after a valid handshake")
	}
	// The peer ID must be derivable from the stored key alone.
	if got := PeerIDFromKey(slot.key); got != theirs.peerID {
		t.Fatalf("peer ID derived from slot key = %x, want %x", got, theirs.peerID)
	}
}

func TestVerifyLeavesSlotEmptyOnMismatch(t *testing.T) {
	ours, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}
	unexpected, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}

	slot := &remoteIdentitySlot{}
	// Pin a peer identity that does not match the certificate presented.
	config := ours.connConfig(slot, unexpected.peerID)

	raw := theirs.config.Certificates[0].Certificate[0]
	err = config.VerifyPeerCertificate([][]byte{raw}, nil)
	if _, ok := errors.AsType[ErrPeerMismatch](err); !ok {
		t.Fatalf("verify error = %v, want ErrPeerMismatch", err)
	}
	if slot.key != nil {
		t.Fatalf("slot holds a key after a failed handshake: %x", PeerIDFromKey(slot.key))
	}
}

// TestVerifyWithoutSlotStillAuthenticates covers the guard path: a nil slot must
// not skip authentication, only skip recording it.
func TestVerifyWithoutSlotStillAuthenticates(t *testing.T) {
	ours, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}

	config := ours.connConfig(nil, "")
	raw := theirs.config.Certificates[0].Certificate[0]
	if err := config.VerifyPeerCertificate([][]byte{raw}, nil); err != nil {
		t.Fatalf("verify with a nil slot rejected a valid certificate: %v", err)
	}
	if err := config.VerifyPeerCertificate([][]byte{[]byte("not a certificate")}, nil); err == nil {
		t.Fatal("verify with a nil slot accepted an invalid certificate")
	}
}
