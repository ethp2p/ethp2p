package transport

import (
	"errors"
	"testing"
)

// The handshake verify callback is the single place that authenticates a remote
// certificate, and it records the result into a per-connection identity slot so
// Conn.RemotePeerID does not re-derive it. These tests pin that contract: the
// slot is filled on success and left empty on failure, which is what keeps
// authentication to exactly one certificate parse and one signature check per
// connection.

func TestVerifyRecordsPeerIDInSlot(t *testing.T) {
	ours, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := newHandshaker(testKey(t))
	if err != nil {
		t.Fatal(err)
	}

	slot := &identitySlot{}
	config := ours.dialConfig(slot, theirs.peerID)

	raw := theirs.config.Certificates[0].Certificate[0]
	if err := config.VerifyPeerCertificate([][]byte{raw}, nil); err != nil {
		t.Fatalf("verify rejected a valid certificate: %v", err)
	}
	if slot.id != theirs.peerID {
		t.Fatalf("slot id = %x, want %x", slot.id, theirs.peerID)
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

	slot := &identitySlot{}
	// Pin a peer identity that does not match the certificate presented.
	config := ours.dialConfig(slot, unexpected.peerID)

	raw := theirs.config.Certificates[0].Certificate[0]
	err = config.VerifyPeerCertificate([][]byte{raw}, nil)
	var mismatch ErrPeerMismatch
	if !errors.As(err, &mismatch) {
		t.Fatalf("verify error = %v, want ErrPeerMismatch", err)
	}
	if slot.id != "" {
		t.Fatalf("slot holds %x after a failed handshake, want empty", slot.id)
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

	config := ours.verify(nil, "")
	raw := theirs.config.Certificates[0].Certificate[0]
	if err := config.VerifyPeerCertificate([][]byte{raw}, nil); err != nil {
		t.Fatalf("verify with a nil slot rejected a valid certificate: %v", err)
	}
	if err := config.VerifyPeerCertificate([][]byte{[]byte("not a certificate")}, nil); err == nil {
		t.Fatal("verify with a nil slot accepted an invalid certificate")
	}
}
