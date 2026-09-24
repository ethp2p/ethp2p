package enr

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
	"net/netip"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	decrecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/ethp2p/ethp2p/transport"
)

const eip778Example = "enr:-IS4QHCYrYZbAKWCBRlAy5zzaDZXJBGkcnh4MHcBFZntXNFrdvJjX04jRzjzCBOonrkTfj499SZuOh8R33Ls8RRcy5wBgmlkgnY0gmlwhH8AAAGJc2VjcDI1NmsxoQPKY0yuDUmstAHYpMa2_oxVtw0RW_QAdpzBQA8yWM0xOIN1ZHCCdl8"

const hoodiEFBootnode = "enr:-Mq4QLkmuSwbGBUph1r7iHopzRpdqE-gcm5LNZfcE-6T37OCZbRHi22bXZkaqnZ6XdIyEDTelnkmMEQB8w6NbnJUt9GGAZWaowaYh2F0dG5ldHOIABgAAAAAAACEZXRoMpDS8Zl_YAAJEAAIAAAAAAAAgmlkgnY0gmlwhNEmfKCEcXVpY4IyyIlzZWNwMjU2azGhA0hGa4jZJZYQAS-z6ZFK-m4GCFnWS8wfjO0bpSQn6hyEiHN5bmNuZXRzAIN0Y3CCIyiDdWRwgiMo"

func TestEIP778Example(t *testing.T) {
	record, err := Parse(eip778Example)
	if err != nil {
		t.Fatal(err)
	}
	if record.Seq() != 1 {
		t.Fatalf("sequence = %d, want 1", record.Seq())
	}
	if got := record.String(); got != eip778Example {
		t.Fatalf("text round trip = %q, want %q", got, eip778Example)
	}
	if got := record.Encode(); !bytes.Equal(got, mustRawURL(t, eip778Example)) {
		t.Fatal("Encode did not preserve the example record bytes")
	}

	addr, ok, err := record.Get(IP)
	if err != nil || !ok || addr != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("Get(IP) = %v, %t, %v", addr, ok, err)
	}
	port, ok, err := record.Get(UDP)
	if err != nil || !ok || port != 30303 {
		t.Fatalf("Get(UDP) = %d, %t, %v", port, ok, err)
	}

	privateKey := examplePrivateKey(t)
	if !bytes.Equal(record.PublicKey().SerializeCompressed(), privateKey.PubKey().SerializeCompressed()) {
		t.Fatal("decoded public key does not match the EIP-778 example key")
	}
	rebuilt, err := Sign(privateKey, 1,
		IP.Set(netip.MustParseAddr("127.0.0.1")),
		UDP.Set(uint16(30303)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rebuilt.Encode(), record.Encode()) {
		t.Fatal("deterministic signing did not reproduce the EIP-778 example bytes")
	}
}

func TestConsensusLayerQUICRecord(t *testing.T) {
	// Source: Consensys Teku's Hoodi EF bootnode list in
	// ethereum/networks/src/main/java/tech/pegasys/teku/networks/Eth2NetworkConfiguration.java,
	// applyHoodiNetworkDefaults (line 1194 in the local checkout).
	record, err := Parse(hoodiEFBootnode)
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.AddrPort{
		netip.MustParseAddrPort("209.38.124.160:13000"),
	}
	if got := record.QUIC(); !equalAddrPorts(got, want) {
		t.Fatalf("QUIC() = %v, want %v", got, want)
	}
	if got, want := record.PeerID(), transport.NewPubKey(record.PublicKey()).PeerID(); got != want {
		t.Fatalf("PeerID() = %x, transport derivation = %x", got, want)
	}
}

func TestPublicKeyReturnsCopy(t *testing.T) {
	record, err := Parse(eip778Example)
	if err != nil {
		t.Fatal(err)
	}
	wantPeerID := record.PeerID()
	wantPublicKey := record.PublicKey().SerializeCompressed()
	returnedKey := record.PublicKey()
	otherKey := secp256k1.PrivKeyFromBytes(bytes.Repeat([]byte{0x01}, 32)).PubKey()
	*returnedKey = *otherKey

	if got := record.PeerID(); got != wantPeerID {
		t.Fatalf("PeerID changed after mutating returned key: got %x, want %x", got, wantPeerID)
	}
	if got := record.PublicKey().SerializeCompressed(); !bytes.Equal(got, wantPublicKey) {
		t.Fatalf("PublicKey changed after mutating returned key: got %x, want %x", got, wantPublicKey)
	}
}

func TestTransportPubKeyCopiesInput(t *testing.T) {
	callerKey := examplePrivateKey(t).PubKey()
	transportKey := transport.NewPubKey(callerKey)
	wantPeerID := transportKey.PeerID()
	otherKey := secp256k1.PrivKeyFromBytes(bytes.Repeat([]byte{0x02}, 32)).PubKey()
	*callerKey = *otherKey
	if got := transportKey.PeerID(); got != wantPeerID {
		t.Fatalf("transport PeerID changed after mutating input key: got %x, want %x", got, wantPeerID)
	}
}

func TestGetPredefinedAndCustomKeys(t *testing.T) {
	want := map[string]any{
		"ip":    netip.MustParseAddr("203.0.113.7"),
		"ip6":   netip.MustParseAddr("2001:db8::7"),
		"udp":   uint16(30303),
		"udp6":  uint16(30304),
		"tcp":   uint16(30305),
		"tcp6":  uint16(30306),
		"quic":  uint16(30307),
		"quic6": uint16(30308),
	}
	record, err := Sign(examplePrivateKey(t), 9,
		IP.Set(want["ip"].(netip.Addr)),
		IP6.Set(want["ip6"].(netip.Addr)),
		UDP.Set(want["udp"].(uint16)),
		UDP6.Set(want["udp6"].(uint16)),
		TCP.Set(want["tcp"].(uint16)),
		TCP6.Set(want["tcp6"].(uint16)),
		QUIC.Set(want["quic"].(uint16)),
		QUIC6.Set(want["quic6"].(uint16)),
		BytesEntry("attnets").Set([]byte{0x01, 0x80}),
		UintEntry("custom-seq").Set(uint64(1<<40+19)),
	)
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		got  any
		ok   bool
		err  error
		want any
	}{
		{name: "ip", want: want["ip"]},
		{name: "ip6", want: want["ip6"]},
		{name: "udp", want: want["udp"]},
		{name: "udp6", want: want["udp6"]},
		{name: "tcp", want: want["tcp"]},
		{name: "tcp6", want: want["tcp6"]},
		{name: "quic", want: want["quic"]},
		{name: "quic6", want: want["quic6"]},
	}
	checks[0].got, checks[0].ok, checks[0].err = record.Get(IP)
	checks[1].got, checks[1].ok, checks[1].err = record.Get(IP6)
	checks[2].got, checks[2].ok, checks[2].err = record.Get(UDP)
	checks[3].got, checks[3].ok, checks[3].err = record.Get(UDP6)
	checks[4].got, checks[4].ok, checks[4].err = record.Get(TCP)
	checks[5].got, checks[5].ok, checks[5].err = record.Get(TCP6)
	checks[6].got, checks[6].ok, checks[6].err = record.Get(QUIC)
	checks[7].got, checks[7].ok, checks[7].err = record.Get(QUIC6)
	for _, check := range checks {
		if check.err != nil || !check.ok || check.got != check.want {
			t.Errorf("%s = %v, %t, %v; want %v", check.name, check.got, check.ok, check.err, check.want)
		}
	}

	bytesKey := BytesEntry("attnets")
	gotBytes, ok, err := record.Get(bytesKey)
	if err != nil || !ok || !bytes.Equal(gotBytes, []byte{0x01, 0x80}) {
		t.Fatalf("Get(attnets) = %x, %t, %v", gotBytes, ok, err)
	}
	gotBytes[0] = 0xff
	gotBytesAgain, _, _ := record.Get(bytesKey)
	if gotBytesAgain[0] != 0x01 {
		t.Fatal("Get(BytesEntry) returned a mutable view of the record")
	}
	gotUint, ok, err := record.Get(UintEntry("custom-seq"))
	if err != nil || !ok || gotUint != 1<<40+19 {
		t.Fatalf("Get(custom-seq) = %d, %t, %v", gotUint, ok, err)
	}
	if _, ok, err := record.Get(BytesEntry("missing")); err != nil || ok {
		t.Fatalf("missing key = %t, %v; want false, nil", ok, err)
	}
}

func TestGetMalformedValues(t *testing.T) {
	record, err := Sign(examplePrivateKey(t), 1,
		BytesEntry("ip").Set([]byte{127, 0, 1}),
		BytesEntry("ip6").Set([]byte{0, 0, 0, 0}),
		BytesEntry("quic").Set([]byte{1, 0, 0}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := record.Get(IP); !ok || err == nil {
		t.Fatalf("wrong-length IP = %t, %v; want present and malformed", ok, err)
	}
	if _, ok, err := record.Get(IP6); !ok || err == nil {
		t.Fatalf("wrong-length IP6 = %t, %v; want present and malformed", ok, err)
	}
	if _, ok, err := record.Get(QUIC); !ok || err == nil {
		t.Fatalf("oversize QUIC port = %t, %v; want present and malformed", ok, err)
	}
	if _, err := Sign(examplePrivateKey(t), 1, IP.Set(netip.Addr{})); err == nil {
		t.Fatal("Sign accepted an invalid IPv4 address")
	}
	if _, err := Sign(examplePrivateKey(t), 1, IP6.Set(netip.MustParseAddr("::ffff:192.0.2.1"))); err == nil {
		t.Fatal("Sign accepted an IPv4-mapped IPv6 address")
	}
	mapped := netip.MustParseAddr("::ffff:192.0.2.1").As16()
	mappedRecord, err := Sign(examplePrivateKey(t), 1, BytesEntry("ip6").Set(mapped[:]))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := mappedRecord.Get(IP6); !ok || err == nil {
		t.Fatalf("IPv4-mapped IP6 = %t, %v; want present and malformed", ok, err)
	}
}

func TestQUICSkipsInvalidAddressesAndZeroPorts(t *testing.T) {
	key := examplePrivateKey(t)
	validIPv6 := netip.MustParseAddr("2001:db8::7")

	unspecified, err := Sign(key, 1,
		IP.Set(netip.IPv4Unspecified()), QUIC.Set(uint16(30303)),
		IP6.Set(validIPv6), QUIC6.Set(uint16(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := unspecified.QUIC(); len(got) != 0 {
		t.Fatalf("QUIC() for unspecified IPv4 and zero IPv6 port = %v, want empty", got)
	}

	malformedIPv4, err := Sign(key, 2,
		BytesEntry("ip").Set([]byte{192, 0, 2}), QUIC.Set(uint16(30303)),
		IP6.Set(validIPv6), QUIC6.Set(uint16(30304)),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.AddrPort{netip.AddrPortFrom(validIPv6, 30304)}
	if got := malformedIPv4.QUIC(); !equalAddrPorts(got, want) {
		t.Fatalf("QUIC() with malformed IPv4 = %v, want %v", got, want)
	}

	zeroPorts, err := Sign(key, 3,
		IP.Set(netip.MustParseAddr("192.0.2.7")), QUIC.Set(uint16(0)),
		IP6.Set(netip.IPv6Unspecified()), QUIC6.Set(uint16(30304)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := zeroPorts.QUIC(); len(got) != 0 {
		t.Fatalf("QUIC() for zero port and unspecified IPv6 = %v, want empty", got)
	}
}

func TestQUICSkipsMulticastAndPreservesCandidateOrder(t *testing.T) {
	key := examplePrivateKey(t)
	v4 := netip.MustParseAddr("192.0.2.7")
	v6 := netip.MustParseAddr("2001:db8::7")

	ipv4Multicast, err := Sign(key, 4,
		IP.Set(netip.MustParseAddr("224.0.0.1")), QUIC.Set(uint16(9000)),
		IP6.Set(v6), QUIC6.Set(uint16(9001)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ipv4Multicast.QUIC(), []netip.AddrPort{netip.AddrPortFrom(v6, 9001)}; !equalAddrPorts(got, want) {
		t.Fatalf("QUIC() with IPv4 multicast = %v, want %v", got, want)
	}

	ipv6Multicast, err := Sign(key, 5,
		IP.Set(v4), QUIC.Set(uint16(9000)),
		IP6.Set(netip.MustParseAddr("ff02::1")), QUIC6.Set(uint16(9001)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ipv6Multicast.QUIC(), []netip.AddrPort{netip.AddrPortFrom(v4, 9000)}; !equalAddrPorts(got, want) {
		t.Fatalf("QUIC() with IPv6 multicast = %v, want %v", got, want)
	}

	bothFamilies, err := Sign(key, 6,
		IP.Set(v4), QUIC.Set(uint16(9000)),
		IP6.Set(v6), QUIC6.Set(uint16(9001)),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.AddrPort{netip.AddrPortFrom(v4, 9000), netip.AddrPortFrom(v6, 9001)}
	if got := bothFamilies.QUIC(); !equalAddrPorts(got, want) {
		t.Fatalf("QUIC() candidate order = %v, want %v", got, want)
	}
}

func TestSignRejectsInvalidEntriesAndOversize(t *testing.T) {
	key := examplePrivateKey(t)
	if _, err := Sign(key, 1, UDP.Set(uint16(1)), UDP.Set(uint16(2))); err == nil {
		t.Fatal("Sign accepted duplicate keys")
	}
	if _, err := Sign(key, 1, BytesEntry("id").Set([]byte("v4"))); err == nil {
		t.Fatal("Sign accepted a reserved id key")
	}
	if _, err := Sign(key, 1, BytesEntry("secp256k1").Set([]byte{1})); err == nil {
		t.Fatal("Sign accepted a reserved secp256k1 key")
	}
	if _, err := Sign(key, 1, BytesEntry("payload").Set(bytes.Repeat([]byte{0x01}, 256))); err == nil {
		t.Fatal("Sign accepted a record larger than 300 bytes")
	}
	if _, err := Sign(nil, 1); err == nil {
		t.Fatal("Sign accepted a nil private key")
	}
}

func TestUpdate(t *testing.T) {
	key := examplePrivateKey(t)
	original, err := Sign(key, 7,
		IP.Set(netip.MustParseAddr("192.0.2.1")),
		QUIC.Set(uint16(9000)),
		BytesEntry("eth2").Set([]byte{0xaa}),
	)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := original.Update(key, IP.Set(netip.MustParseAddr("192.0.2.2")), UDP.Set(uint16(9001)))
	if err != nil {
		t.Fatal(err)
	}
	if updated.Seq() != 8 {
		t.Fatalf("Seq() = %d; want 8", updated.Seq())
	}
	if addr, _, _ := updated.Get(IP); addr != netip.MustParseAddr("192.0.2.2") {
		t.Fatalf("Get(IP) = %v; want the replaced address", addr)
	}
	if port, _, _ := updated.Get(QUIC); port != 9000 {
		t.Fatalf("Get(QUIC) = %d; want the kept port", port)
	}
	if port, _, _ := updated.Get(UDP); port != 9001 {
		t.Fatalf("Get(UDP) = %d; want the added port", port)
	}
	if eth2, _, _ := updated.Get(BytesEntry("eth2")); !bytes.Equal(eth2, []byte{0xaa}) {
		t.Fatalf("Get(eth2) = %x; want the kept value", eth2)
	}
	if updated.PeerID() != original.PeerID() {
		t.Fatal("Update changed the identity")
	}
	if addr, _, _ := original.Get(IP); addr != netip.MustParseAddr("192.0.2.1") {
		t.Fatal("Update modified the original record")
	}

	other, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.Update(other); err == nil {
		t.Fatal("Update accepted a key that did not sign the record")
	}
	if _, err := original.Update(key, BytesEntry("id").Set([]byte("v5"))); err == nil {
		t.Fatal("Update accepted a reserved key")
	}
}

func TestDecodeRejectsMalformedRecords(t *testing.T) {
	valid, err := Sign(examplePrivateKey(t), 1,
		IP.Set(netip.MustParseAddr("127.0.0.1")),
		UDP.Set(uint16(30303)),
	)
	if err != nil {
		t.Fatal(err)
	}
	baseFields := recordFields(t, valid.Encode())

	badSignature := cloneFields(baseFields)
	signature := mustRLPItem(t, badSignature[0])
	badSig := append([]byte(nil), signature.payload...)
	badSig[0] ^= 1
	badSignature[0] = encodeRLPString(badSig)

	highS := cloneFields(baseFields)
	signature = mustRLPItem(t, highS[0])
	highSignature := append([]byte(nil), signature.payload...)
	curveOrder := secp256k1.Params().N
	highScalar := new(big.Int).Sub(curveOrder, new(big.Int).SetBytes(highSignature[32:]))
	copy(highSignature[32:], highScalar.FillBytes(make([]byte, 32)))
	highS[0] = encodeRLPString(highSignature)

	wrongID := cloneFields(baseFields)
	wrongID[entryField(t, wrongID, "id")+1] = encodeRLPString([]byte("v5"))

	missingPublicKey := removePair(t, baseFields, "secp256k1")

	unsorted := cloneFields(baseFields)
	unsorted[entryField(t, unsorted, "ip")] = encodeRLPString([]byte("z"))

	duplicate := cloneFields(baseFields)
	duplicate[entryField(t, duplicate, "ip")] = encodeRLPString([]byte("id"))

	nonMinimalLength := cloneFields(baseFields)
	signature = mustRLPItem(t, nonMinimalLength[0])
	nonMinimalLength[0] = append([]byte{0xb9, 0, byte(len(signature.payload))}, signature.payload...)

	nonMinimalSingleByte := cloneFields(baseFields)
	nonMinimalSingleByte[1] = []byte{0x81, 0x01}

	leadingZeroInteger := cloneFields(baseFields)
	leadingZeroInteger[1] = encodeRLPString([]byte{0, 1})

	trailing := append(valid.Encode(), 0x80)
	truncated := valid.Encode()[:len(valid.Encode())-1]

	cases := []struct {
		name string
		raw  []byte
	}{
		{name: "bad signature", raw: rebuildFields(badSignature)},
		{name: "high-S signature", raw: rebuildFields(highS)},
		{name: "wrong id", raw: rebuildFields(wrongID)},
		{name: "missing secp256k1", raw: rebuildFields(missingPublicKey)},
		{name: "unsorted keys", raw: rebuildFields(unsorted)},
		{name: "duplicate keys", raw: rebuildFields(duplicate)},
		{name: "non-minimal length prefix", raw: rebuildFields(nonMinimalLength)},
		{name: "single byte encoded as string", raw: rebuildFields(nonMinimalSingleByte)},
		{name: "integer with leading zero", raw: rebuildFields(leadingZeroInteger)},
		{name: "length overrun", raw: []byte{0xf8, 0x40, 0x80}},
		{name: "truncated input", raw: truncated},
		{name: "trailing bytes", raw: trailing},
		{name: "record exceeds 300 bytes", raw: bytes.Repeat([]byte{0x80}, maxRecordSize+1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Decode(test.raw); err == nil {
				t.Fatal("Decode accepted malformed input")
			}
		})
	}
}

func TestParseRejectsMalformedText(t *testing.T) {
	if _, err := Parse(base64.RawURLEncoding.EncodeToString(mustRawURL(t, eip778Example))); err == nil {
		t.Fatal("Parse accepted text without the enr: prefix")
	}
	if _, err := Parse("enr:not+base64"); err == nil {
		t.Fatal("Parse accepted invalid base64url")
	}
}

func TestDecodeKeepsArbitraryRLPValues(t *testing.T) {
	key := examplePrivateKey(t)
	listValue := encodeRLPList(append(encodeRLPString([]byte("fork")), encodeRLPList(nil)...))
	raw := signRawPairs(key, 4, []signedPair{{key: "custom-list", value: listValue}})
	record, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(record.Encode(), raw) {
		t.Fatal("Decode did not preserve the original RLP bytes")
	}
	if _, ok, err := record.Get(BytesEntry("custom-list")); !ok || err == nil {
		t.Fatalf("Get(list as bytes) = %t, %v; want present and malformed", ok, err)
	}
}

func TestDecodeSignRoundTripRandomEntries(t *testing.T) {
	random := rand.New(rand.NewSource(778))
	key := examplePrivateKey(t)
	for i := 0; i < 60; i++ {
		pairs := make([]Pair, 0, 7)
		count := random.Intn(6) + 1
		for j := 0; j < count; j++ {
			value := make([]byte, random.Intn(16))
			_, _ = random.Read(value)
			pairs = append(pairs, BytesEntry(fmt.Sprintf("custom-%02d-%02d", i, j)).Set(value))
		}
		pairs = append(pairs, UintEntry("custom-uint").Set(random.Uint64()))
		original, err := Sign(key, uint64(i), pairs...)
		if err != nil {
			t.Fatalf("Sign case %d: %v", i, err)
		}
		decoded, err := Decode(original.Encode())
		if err != nil {
			t.Fatalf("Decode case %d: %v", i, err)
		}
		if !bytes.Equal(decoded.Encode(), original.Encode()) {
			t.Fatalf("round trip case %d changed the encoded record", i)
		}
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(mustRawURL(nil, eip778Example))
	f.Add([]byte{0xc0})
	f.Add([]byte{0xf8, 0xff})
	f.Fuzz(func(t *testing.T, raw []byte) {
		record, err := Decode(raw)
		if err != nil {
			return
		}
		if !bytes.Equal(record.encodeCanonical(), raw) {
			t.Fatal("accepted ENR fields did not re-encode to identical bytes")
		}
	})
}

func FuzzRLPDecoder(f *testing.F) {
	f.Add([]byte{0xc2, 0x01, 0x80})
	f.Add([]byte{0xb8, 0x38})
	f.Add([]byte{0x81, 0x01})
	f.Add([]byte{0xf8, 0x40, 0x80})
	f.Fuzz(func(t *testing.T, raw []byte) {
		item, _, err := decodeRLP(raw)
		if err != nil {
			return
		}
		if got := encodeParsedItem(item); !bytes.Equal(got, item.raw) {
			t.Fatalf("decoded RLP item re-encoded differently: %x != %x", got, item.raw)
		}
	})
}

func encodeParsedItem(item rlpItem) []byte {
	if item.kind == rlpString {
		return encodeRLPString(item.payload)
	}
	var payload []byte
	for _, child := range item.children {
		payload = append(payload, encodeParsedItem(child)...)
	}
	return encodeRLPList(payload)
}

func examplePrivateKey(t *testing.T) *secp256k1.PrivateKey {
	t.Helper()
	secret, err := hex.DecodeString("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	if err != nil {
		t.Fatal(err)
	}
	return secp256k1.PrivKeyFromBytes(secret)
}

func mustRawURL(t *testing.T, text string) []byte {
	if t != nil {
		t.Helper()
	}
	encoded := text
	if len(text) >= 4 && text[:4] == "enr:" {
		encoded = text[4:]
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		panic(err)
	}
	return raw
}

func recordFields(t *testing.T, raw []byte) [][]byte {
	t.Helper()
	root, err := decodeSingleRLP(raw)
	if err != nil {
		t.Fatal(err)
	}
	if root.kind != rlpList {
		t.Fatal("record root is not a list")
	}
	fields := make([][]byte, len(root.children))
	for i, child := range root.children {
		fields[i] = append([]byte(nil), child.raw...)
	}
	return fields
}

func cloneFields(fields [][]byte) [][]byte {
	cloned := make([][]byte, len(fields))
	for i, field := range fields {
		cloned[i] = append([]byte(nil), field...)
	}
	return cloned
}

func rebuildFields(fields [][]byte) []byte {
	var payload []byte
	for _, field := range fields {
		payload = append(payload, field...)
	}
	return encodeRLPList(payload)
}

func entryField(t *testing.T, fields [][]byte, name string) int {
	t.Helper()
	for i := 2; i < len(fields); i += 2 {
		key := mustRLPItem(t, fields[i])
		if string(key.payload) == name {
			return i
		}
	}
	t.Fatalf("missing test key %q", name)
	return -1
}

func removePair(t *testing.T, fields [][]byte, name string) [][]byte {
	t.Helper()
	index := entryField(t, fields, name)
	removed := make([][]byte, 0, len(fields)-2)
	removed = append(removed, cloneFields(fields[:index])...)
	removed = append(removed, cloneFields(fields[index+2:])...)
	return removed
}

func mustRLPItem(t *testing.T, raw []byte) rlpItem {
	t.Helper()
	item, err := decodeSingleRLP(raw)
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func signRawPairs(key *secp256k1.PrivateKey, seq uint64, extra []signedPair) []byte {
	pairs := append([]signedPair(nil), extra...)
	pairs = append(pairs,
		signedPair{key: "id", value: encodeRLPString([]byte("v4"))},
		signedPair{key: "secp256k1", value: encodeRLPString(key.PubKey().SerializeCompressed())},
	)
	for i := range pairs {
		pairs[i].value = append([]byte(nil), pairs[i].value...)
	}
	for i := 0; i < len(pairs); i++ {
		for j := i + 1; j < len(pairs); j++ {
			if pairs[j].key < pairs[i].key {
				pairs[i], pairs[j] = pairs[j], pairs[i]
			}
		}
	}
	hash := keccak256(encodeRLPList(encodeSignedContent(seq, pairs)))
	compact := decrecdsa.SignCompact(key, hash, true)
	signature := compact[1:]
	body := encodeRLPString(signature)
	body = append(body, encodeUint64(seq)...)
	for _, pair := range pairs {
		body = append(body, encodeRLPString([]byte(pair.key))...)
		body = append(body, pair.value...)
	}
	return encodeRLPList(body)
}

func equalAddrPorts(got, want []netip.AddrPort) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
