package identity

import (
	"bytes"
	"crypto/sha256"
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
	if !bytes.Equal(parsed.Public().Bytes(), key.Public().Bytes()) {
		t.Fatal("parsed key differs from the original")
	}
	order := secp256k1.Params().N.FillBytes(make([]byte, 32))
	for name, secret := range map[string][]byte{
		"zero":        make([]byte, 32),
		"group order": order,
		"short":       key.Bytes()[:31],
		"long":        append(key.Bytes(), 0),
	} {
		if _, err := ParsePrivKey(secret); err == nil {
			t.Errorf("ParsePrivKey(%s) accepted the scalar", name)
		}
	}
}

func TestPubKeyEncodingsRoundTrip(t *testing.T) {
	pub := testKey(t).Public()
	fromBytes, err := ParsePubKey(pub.Bytes())
	if err != nil || !bytes.Equal(fromBytes.Bytes(), pub.Bytes()) {
		t.Fatalf("ParsePubKey round trip: got %v, err %v", fromBytes, err)
	}
	fromWire, err := UnmarshalPubKey(pub.Marshal())
	if err != nil || !bytes.Equal(fromWire.Bytes(), pub.Bytes()) {
		t.Fatalf("UnmarshalPubKey round trip: got %v, err %v", fromWire, err)
	}
	if _, err := ParsePubKey(pub.key.SerializeUncompressed()); err == nil {
		t.Fatal("ParsePubKey accepted an uncompressed key")
	}
}

func TestUnmarshalPubKeyRejectsOtherKeyTypes(t *testing.T) {
	const ed25519KeyType = 1
	wire := protowire.AppendTag(nil, 1, protowire.VarintType)
	wire = protowire.AppendVarint(wire, ed25519KeyType)
	wire = protowire.AppendTag(wire, 2, protowire.BytesType)
	wire = protowire.AppendBytes(wire, bytes.Repeat([]byte{1}, 32))
	if _, err := UnmarshalPubKey(wire); err == nil {
		t.Fatal("UnmarshalPubKey accepted an ed25519 key")
	}
	if _, err := UnmarshalPubKey([]byte{0xff}); err == nil {
		t.Fatal("UnmarshalPubKey accepted a truncated message")
	}
}

func TestUnmarshalPubKeyRejectsUncompressedData(t *testing.T) {
	pub := testKey(t).Public()
	wire := protowire.AppendTag(nil, 1, protowire.VarintType)
	wire = protowire.AppendVarint(wire, secp256k1KeyType)
	wire = protowire.AppendTag(wire, 2, protowire.BytesType)
	wire = protowire.AppendBytes(wire, pub.key.SerializeUncompressed())
	if _, err := UnmarshalPubKey(wire); err == nil {
		t.Fatal("UnmarshalPubKey accepted 65-byte uncompressed key data")
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
