package identity

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"google.golang.org/protobuf/encoding/protowire"
)

func testKey(t *testing.T) *PrivKey {
	t.Helper()
	key, err := GenPrivKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestParsePrivKeyRoundTripsAndRejectsNonCanonicalScalars(t *testing.T) {
	key := testKey(t)
	parsed, err := ParsePrivKey(key.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Public().Equal(key.Public()) {
		t.Fatal("parsed key differs from the original")
	}
	order := secp256k1.Params().N.FillBytes(make([]byte, 32))
	for name, secret := range map[string][]byte{
		"zero":        make([]byte, 32),
		"group order": order,
		"short":       key.Bytes()[:31],
		"long":        append(key.Bytes(), 0),
	} {
		if _, err := ParsePrivKey(secret); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("ParsePrivKey(%s) = %v, want ErrInvalidKey", name, err)
		}
	}
}

func TestPubKeyEncodingsRoundTrip(t *testing.T) {
	pub := testKey(t).Public()
	fromBytes, err := ParsePubKey(pub.Bytes())
	if err != nil || !fromBytes.Equal(pub) {
		t.Fatalf("ParsePubKey round trip: equal=%t err=%v", err == nil && fromBytes.Equal(pub), err)
	}
	fromWire, err := UnmarshalPubKey(pub.Marshal())
	if err != nil || !fromWire.Equal(pub) {
		t.Fatalf("UnmarshalPubKey round trip: equal=%t err=%v", err == nil && fromWire.Equal(pub), err)
	}
	uncompressed := pub.key.SerializeUncompressed()
	if _, err := ParsePubKey(uncompressed); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("ParsePubKey(uncompressed) = %v, want ErrInvalidKey", err)
	}
}

func TestUnmarshalPubKeyRejectsOtherKeyTypes(t *testing.T) {
	const ed25519KeyType = 1
	wire := protowire.AppendTag(nil, 1, protowire.VarintType)
	wire = protowire.AppendVarint(wire, ed25519KeyType)
	wire = protowire.AppendTag(wire, 2, protowire.BytesType)
	wire = protowire.AppendBytes(wire, bytes.Repeat([]byte{1}, 32))
	if _, err := UnmarshalPubKey(wire); !errors.Is(err, ErrUnsupportedKey) {
		t.Fatalf("UnmarshalPubKey(ed25519) = %v, want ErrUnsupportedKey", err)
	}
	if _, err := UnmarshalPubKey([]byte{0xff}); err == nil {
		t.Fatal("UnmarshalPubKey accepted a truncated message")
	}
}

func TestSignaturesVerifyOnlyUnderTheirScheme(t *testing.T) {
	key := testKey(t)
	message := []byte("message")
	hash := sha256.Sum256(message)
	der, compact := key.Sign(message), key.SignHash(hash[:])
	if !key.Public().Verify(message, der) || !key.Public().VerifyHash(hash[:], compact) {
		t.Fatal("valid signatures rejected")
	}
	if key.Public().Verify(message, compact) || key.Public().VerifyHash(hash[:], der) {
		t.Fatal("a signature verified under the other scheme")
	}
	if testKey(t).Public().Verify(message, der) {
		t.Fatal("another key verified the signature")
	}
}

func TestVerifyHashRejectsHighS(t *testing.T) {
	key := testKey(t)
	hash := sha256.Sum256([]byte("message"))
	signature := key.SignHash(hash[:])
	var s secp256k1.ModNScalar
	s.SetByteSlice(signature[32:])
	s.Negate()
	highS := s.Bytes()
	malleated := append(bytes.Clone(signature[:32]), highS[:]...)
	if key.Public().VerifyHash(hash[:], malleated) {
		t.Fatal("VerifyHash accepted the high-S form of a valid signature")
	}
}
