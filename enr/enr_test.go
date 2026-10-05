package enr

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"math/rand"
	"net/netip"
	"slices"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/ethp2p/ethp2p/identity"
)

const eip778Example = "enr:-IS4QHCYrYZbAKWCBRlAy5zzaDZXJBGkcnh4MHcBFZntXNFrdvJjX04jRzjzCBOonrkTfj499SZuOh8R33Ls8RRcy5wBgmlkgnY0gmlwhH8AAAGJc2VjcDI1NmsxoQPKY0yuDUmstAHYpMa2_oxVtw0RW_QAdpzBQA8yWM0xOIN1ZHCCdl8"

const hoodiEFBootnode = "enr:-Mq4QLkmuSwbGBUph1r7iHopzRpdqE-gcm5LNZfcE-6T37OCZbRHi22bXZkaqnZ6XdIyEDTelnkmMEQB8w6NbnJUt9GGAZWaowaYh2F0dG5ldHOIABgAAAAAAACEZXRoMpDS8Zl_YAAJEAAIAAAAAAAAgmlkgnY0gmlwhNEmfKCEcXVpY4IyyIlzZWNwMjU2azGhA0hGa4jZJZYQAS-z6ZFK-m4GCFnWS8wfjO0bpSQn6hyEiHN5bmNuZXRzAIN0Y3CCIyiDdWRwgiMo"

func TestEIP778Example(t *testing.T) {
	raw := mustRawURL(t, eip778Example)
	record, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if record.Seq() != 1 {
		t.Fatalf("sequence = %d, want 1", record.Seq())
	}
	if got := record.Encode(); !bytes.Equal(got, raw) {
		t.Fatal("Encode did not preserve the example record bytes")
	}

	privateKey := examplePrivateKey(t)
	if record.PeerID() != identity.PeerIDFromKey(privateKey.Public()) {
		t.Fatal("decoded identity does not match the EIP-778 example key")
	}
	rebuilt := signRawEntries(privateKey, 1, []entry{
		rawEntry("ip", []byte{127, 0, 0, 1}),
		{key: "udp", value: encodeUint[uint16](30303)},
	})
	if !bytes.Equal(rebuilt, record.Encode()) {
		t.Fatal("deterministic signing did not reproduce the EIP-778 example bytes")
	}
}

func TestConsensusLayerQUICRecord(t *testing.T) {
	// Source: Consensys Teku's Hoodi EF bootnode list in
	// ethereum/networks/src/main/java/tech/pegasys/teku/networks/Eth2NetworkConfiguration.java,
	// applyHoodiNetworkDefaults (line 1194 in the local checkout).
	record, err := Decode(mustRawURL(t, hoodiEFBootnode))
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.AddrPort{
		netip.MustParseAddrPort("209.38.124.160:13000"),
	}
	if got := record.QUIC(); !slices.Equal(got, want) {
		t.Fatalf("QUIC() = %v, want %v", got, want)
	}
}

func TestSignAdvertisesBothFamilies(t *testing.T) {
	v4, v6 := netip.MustParseAddrPort("203.0.113.7:30307"), netip.MustParseAddrPort("[2001:db8::7]:30308")
	record, err := Sign(examplePrivateKey(t), 9,
		EntryIP6(v6.Addr()), EntryQUIC6(v6.Port()), EntryIP(v4.Addr()), EntryQUIC(v4.Port()))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := record.QUIC(), []netip.AddrPort{v4, v6}; !slices.Equal(got, want) {
		t.Fatalf("QUIC() = %v, want %v", got, want)
	}
}

func TestQUICSkipsMalformedValues(t *testing.T) {
	mapped := netip.MustParseAddr("::ffff:192.0.2.1").As16()
	for name, entries := range map[string][]entry{
		"short ip":      {rawEntry("ip", []byte{127, 0, 1}), {key: "quic", value: encodeUint[uint16](1)}},
		"short ip6":     {rawEntry("ip6", []byte{0, 0, 0, 0}), {key: "quic6", value: encodeUint[uint16](1)}},
		"mapped ip6":    {rawEntry("ip6", mapped[:]), {key: "quic6", value: encodeUint[uint16](1)}},
		"oversize port": {rawEntry("ip", []byte{192, 0, 2, 1}), {key: "quic", value: encodeUint[uint32](1<<16 + 1)}},
		"list port":     {rawEntry("ip", []byte{192, 0, 2, 1}), {key: "quic", value: encodeRLPList(nil)}},
	} {
		record, err := Decode(signRawEntries(examplePrivateKey(t), 1, entries))
		if err != nil {
			t.Fatal(err)
		}
		if got := record.QUIC(); len(got) != 0 {
			t.Errorf("%s: QUIC() = %v, want none", name, got)
		}
	}
}

func TestQUICSkipsInvalidAddressesAndZeroPorts(t *testing.T) {
	key := examplePrivateKey(t)
	validIPv6 := netip.MustParseAddr("2001:db8::7")

	unspecified, err := signEndpoints(key, 1,
		netip.AddrPortFrom(netip.IPv4Unspecified(), 30303),
		netip.AddrPortFrom(validIPv6, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := unspecified.QUIC(); len(got) != 0 {
		t.Fatalf("QUIC() for unspecified IPv4 and zero IPv6 port = %v, want empty", got)
	}

	malformedIPv4, err := Decode(signRawEntries(key, 2, []entry{
		rawEntry("ip", []byte{192, 0, 2}), {key: "quic", value: encodeUint[uint16](30303)},
		rawEntry("ip6", validIPv6.AsSlice()), {key: "quic6", value: encodeUint[uint16](30304)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.AddrPort{netip.AddrPortFrom(validIPv6, 30304)}
	if got := malformedIPv4.QUIC(); !slices.Equal(got, want) {
		t.Fatalf("QUIC() with malformed IPv4 = %v, want %v", got, want)
	}

	zeroPorts, err := signEndpoints(key, 3,
		netip.MustParseAddrPort("192.0.2.7:0"),
		netip.AddrPortFrom(netip.IPv6Unspecified(), 30304),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := zeroPorts.QUIC(); len(got) != 0 {
		t.Fatalf("QUIC() for zero port and unspecified IPv6 = %v, want empty", got)
	}
}

func TestQUICSkipsMulticast(t *testing.T) {
	key := examplePrivateKey(t)
	v4 := netip.MustParseAddr("192.0.2.7")
	v6 := netip.MustParseAddr("2001:db8::7")

	ipv4Multicast, err := signEndpoints(key, 4, netip.MustParseAddrPort("224.0.0.1:9000"), netip.AddrPortFrom(v6, 9001))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ipv4Multicast.QUIC(), []netip.AddrPort{netip.AddrPortFrom(v6, 9001)}; !slices.Equal(got, want) {
		t.Fatalf("QUIC() with IPv4 multicast = %v, want %v", got, want)
	}

	ipv6Multicast, err := signEndpoints(key, 5, netip.AddrPortFrom(v4, 9000), netip.MustParseAddrPort("[ff02::1]:9001"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ipv6Multicast.QUIC(), []netip.AddrPort{netip.AddrPortFrom(v4, 9000)}; !slices.Equal(got, want) {
		t.Fatalf("QUIC() with IPv6 multicast = %v, want %v", got, want)
	}

}

func TestSignRejectsInvalidEntries(t *testing.T) {
	key := examplePrivateKey(t)
	v4 := netip.MustParseAddr("192.0.2.1")
	v6 := netip.MustParseAddr("2001:db8::1")
	cases := map[string][]Entry{
		"repeated key":     {EntryIP(v4), EntryIP(v4)},
		"caller id":        {EntryID("v4")},
		"invalid ip":       {EntryIP(netip.Addr{})},
		"ip6 under ip":     {EntryIP(v6)},
		"unspecified ip":   {EntryIP(netip.IPv4Unspecified())},
		"multicast ip":     {EntryIP(netip.MustParseAddr("224.0.0.1"))},
		"ip under ip6":     {EntryIP6(v4)},
		"4-in-6 under ip6": {EntryIP6(netip.MustParseAddr("::ffff:192.0.2.1"))},
		"unspecified ip6":  {EntryIP6(netip.IPv6Unspecified())},
		"multicast ip6":    {EntryIP6(netip.MustParseAddr("ff02::1"))},
		"zero quic port":   {EntryQUIC(0)},
		"zero quic6 port":  {EntryQUIC6(0)},
	}
	for name, entries := range cases {
		if _, err := Sign(key, 1, entries...); err == nil {
			t.Errorf("%s: Sign accepted the entries", name)
		}
	}
	if _, err := Sign(key, 1, failingEntry{}); !errors.Is(err, errEncode) {
		t.Errorf("Sign with a failing encoder: err = %v, want the encoder's error", err)
	}
	if _, err := Sign(nil, 1); err == nil {
		t.Fatal("Sign accepted a nil private key")
	}
}

func TestGet(t *testing.T) {
	key := examplePrivateKey(t)
	v4 := netip.MustParseAddr("192.0.2.1")
	// IP stores an IPv4-mapped address unmapped, so it reads back as v4.
	record, err := Sign(key, 1, EntryIP(netip.AddrFrom16(v4.As16())))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := record.Get[EntryIP](); err != nil || netip.Addr(got) != v4 {
		t.Fatalf("Get[EntryIP] = %v, %v; want %v", netip.Addr(got), err, v4)
	}
	if _, err := record.Get[EntryQUIC](); !errors.Is(err, ErrMissing) {
		t.Fatalf("Get[EntryQUIC] on a record without quic: err = %v, want ErrMissing", err)
	}

	short, err := Decode(signRawEntries(key, 2, []entry{rawEntry("ip", []byte{192, 0, 2})}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := short.Get[EntryIP](); err == nil || errors.Is(err, ErrMissing) {
		t.Fatalf("Get[EntryIP] on a 3-byte ip: err = %v, want a decoding error", err)
	}
}

// entryEth2 reads the consensus-layer fork entry as opaque bytes, as an
// application-defined entry would.
type entryEth2 []byte

func (entryEth2) Key() string { return "eth2" }

func (e entryEth2) MarshalValue() ([]byte, error) { return encodeBytes(e), nil }

func (e *entryEth2) UnmarshalValue(raw []byte) error {
	payload, err := decodeBytes(raw)
	if err != nil {
		return err
	}
	*e = bytes.Clone(payload)
	return nil
}

var errEncode = errors.New("unencodable")

// failingEntry fails to encode, so Sign must report its error.
type failingEntry struct{}

func (failingEntry) Key() string { return "custom" }

func (failingEntry) MarshalValue() ([]byte, error) { return nil, errEncode }

func TestApplicationEntry(t *testing.T) {
	bootnode, err := Decode(mustRawURL(t, hoodiEFBootnode))
	if err != nil {
		t.Fatal(err)
	}
	forkID, err := bootnode.Get[entryEth2]()
	if err != nil {
		t.Fatal(err)
	}
	if len(forkID) != 16 {
		t.Fatalf("Hoodi bootnode eth2 = %x, want the 16-byte ENRForkID", forkID)
	}

	signed, err := Sign(examplePrivateKey(t), 1, forkID, EntryIP(netip.MustParseAddr("192.0.2.1")))
	if err != nil {
		t.Fatal(err)
	}
	received, err := Decode(signed.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := received.Get[entryEth2](); err != nil || !bytes.Equal(got, forkID) {
		t.Fatalf("Get[entryEth2] after a round trip = %x, %v; want %x", got, err, forkID)
	}
}

func TestDecodeRejectsMalformedRecords(t *testing.T) {
	key := examplePrivateKey(t)
	publicKey := encodeBytes(key.Public().Bytes())
	// An x coordinate of 2^256-1 exceeds the field prime, so no point has it.
	offCurve := append([]byte{0x02}, bytes.Repeat([]byte{0xff}, 32)...)
	valid, err := Sign(key, 1, EntryIP(netip.MustParseAddr("127.0.0.1")), EntryQUIC(30303))
	if err != nil {
		t.Fatal(err)
	}
	baseFields := recordFields(t, valid.Encode())

	badSignature := slices.Clone(baseFields)
	signature := mustRLPItem(t, badSignature[0])
	badSig := bytes.Clone(signature.payload)
	badSig[0] ^= 1
	badSignature[0] = encodeBytes(badSig)

	highS := slices.Clone(baseFields)
	signature = mustRLPItem(t, highS[0])
	highSignature := bytes.Clone(signature.payload)
	curveOrder := secp256k1.Params().N
	highScalar := new(big.Int).Sub(curveOrder, new(big.Int).SetBytes(highSignature[32:]))
	copy(highSignature[32:], highScalar.FillBytes(make([]byte, 32)))
	highS[0] = encodeBytes(highSignature)

	wrongID := slices.Clone(baseFields)
	wrongID[entryField(t, wrongID, "id")+1] = encodeBytes("v5")

	missingPublicKey := removeEntry(t, baseFields, "secp256k1")

	unsorted := slices.Clone(baseFields)
	unsorted[entryField(t, unsorted, "ip")] = encodeBytes("z")

	duplicate := slices.Clone(baseFields)
	duplicate[entryField(t, duplicate, "ip")] = encodeBytes("id")

	nonMinimalLength := slices.Clone(baseFields)
	signature = mustRLPItem(t, nonMinimalLength[0])
	nonMinimalLength[0] = append([]byte{0xb9, 0, byte(len(signature.payload))}, signature.payload...)

	nonMinimalSingleByte := slices.Clone(baseFields)
	nonMinimalSingleByte[1] = []byte{0x81, 0x01}

	leadingZeroInteger := slices.Clone(baseFields)
	leadingZeroInteger[1] = encodeBytes([]byte{0, 1})

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
		// Validly signed, so only the identity checks can reject these.
		{name: "signed non-v4 scheme", raw: signEntries(key, 1, []entry{
			{key: "id", value: encodeBytes("v5")}, {key: "secp256k1", value: publicKey},
		})},
		{name: "signed without id", raw: signEntries(key, 1, []entry{
			{key: "secp256k1", value: publicKey},
		})},
		{name: "off-curve secp256k1", raw: signEntries(key, 1, []entry{
			{key: "id", value: encodeBytes("v4")}, {key: "secp256k1", value: encodeBytes(offCurve)},
		})},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Decode(test.raw); err == nil {
				t.Fatal("Decode accepted malformed input")
			}
		})
	}
}

func TestDecodeKeepsArbitraryRLPValues(t *testing.T) {
	key := examplePrivateKey(t)
	listValue := encodeRLPList(append(encodeBytes("fork"), encodeRLPList(nil)...))
	raw := signRawEntries(key, 4, []entry{{key: "custom-list", value: listValue}})
	record, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(record.Encode(), raw) {
		t.Fatal("Decode did not preserve the original RLP bytes")
	}
}

func TestDecodeSignRoundTripRandomEntries(t *testing.T) {
	random := rand.New(rand.NewSource(778))
	key := examplePrivateKey(t)
	for i := 0; i < 60; i++ {
		entries := make([]entry, 0, 7)
		count := random.Intn(6) + 1
		for j := 0; j < count; j++ {
			value := make([]byte, random.Intn(16))
			_, _ = random.Read(value)
			entries = append(entries, rawEntry(fmt.Sprintf("custom-%02d-%02d", i, j), value))
		}
		entries = append(entries, entry{key: "custom-uint", value: encodeUint(random.Uint64())})
		original := signRawEntries(key, uint64(i), entries)
		decoded, err := Decode(original)
		if err != nil {
			t.Fatalf("Decode case %d: %v", i, err)
		}
		if !bytes.Equal(decoded.Encode(), original) {
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
		return encodeBytes(item.payload)
	}
	var payload []byte
	for _, child := range item.children {
		payload = append(payload, encodeParsedItem(child)...)
	}
	return encodeRLPList(payload)
}

// rawEntry pairs key with value as an RLP byte string, bypassing typed entry
// encoding so tests can build malformed or arbitrary records.
func rawEntry(key string, value []byte) entry {
	return entry{key: key, value: encodeBytes(value)}
}

func examplePrivateKey(t *testing.T) *identity.PrivKey {
	t.Helper()
	secret, err := hex.DecodeString("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	if err != nil {
		t.Fatal(err)
	}
	key, err := identity.ParsePrivKey(secret)
	if err != nil {
		t.Fatal(err)
	}
	return key
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
		fields[i] = child.raw
	}
	return fields
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

func removeEntry(t *testing.T, fields [][]byte, name string) [][]byte {
	t.Helper()
	index := entryField(t, fields, name)
	removed := make([][]byte, 0, len(fields)-2)
	removed = append(removed, slices.Clone(fields[:index])...)
	removed = append(removed, slices.Clone(fields[index+2:])...)
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

func signRawEntries(key *identity.PrivKey, seq uint64, extra []entry) []byte {
	return signEntries(key, seq, append([]entry{
		{key: "id", value: encodeBytes("v4")},
		{key: "secp256k1", value: encodeBytes(key.Public().Bytes())},
	}, extra...))
}

// signEndpoints signs a record like Sign but without validating the endpoints,
// so reader tests can build records that Sign refuses to produce.
func signEndpoints(key *identity.PrivKey, seq uint64, endpoints ...netip.AddrPort) (*Record, error) {
	var entries []entry
	for _, endpoint := range endpoints {
		ip, port := "ip", "quic"
		if endpoint.Addr().Is6() {
			ip, port = "ip6", "quic6"
		}
		entries = append(entries,
			entry{key: ip, value: encodeBytes(endpoint.Addr().AsSlice())},
			entry{key: port, value: encodeUint(endpoint.Port())},
		)
	}
	return Decode(signRawEntries(key, seq, entries))
}

// encodeCanonical re-encodes r from its decoded fields. Decode requires
// strictly ascending keys, so sorting the value map restores wire order.
func (r *Record) encodeCanonical() []byte {
	root, _, _ := decodeRLP(r.raw)
	body := encodeBytes(root.children[0].payload)
	body = append(body, encodeUint(r.seq)...)
	for _, key := range slices.Sorted(maps.Keys(r.entries)) {
		body = append(body, encodeBytes(key)...)
		body = append(body, r.entries[key]...)
	}
	return encodeRLPList(body)
}
