package enr

import (
	"encoding/binary"
	"errors"
)

type rlpKind uint8

const (
	rlpString rlpKind = iota
	rlpList
)

// maxRLPDepth is above the maximum nesting depth possible in a 300-byte ENR.
const maxRLPDepth = 512

type rlpItem struct {
	kind     rlpKind
	payload  []byte
	raw      []byte
	children []rlpItem
}

func decodeRLP(input []byte) (rlpItem, []byte, error) {
	return decodeRLPAtDepth(input, 0)
}

func decodeRLPAtDepth(input []byte, depth int) (rlpItem, []byte, error) {
	if depth > maxRLPDepth {
		return rlpItem{}, nil, errors.New("RLP nesting exceeds limit")
	}
	if len(input) == 0 {
		return rlpItem{}, nil, errors.New("truncated RLP item")
	}

	prefix := input[0]
	switch {
	case prefix < 0x80:
		return rlpItem{kind: rlpString, payload: input[:1], raw: input[:1]}, input[1:], nil
	case prefix <= 0xb7:
		length := int(prefix - 0x80)
		if len(input) < 1+length {
			return rlpItem{}, nil, errors.New("truncated RLP string")
		}
		payload := input[1 : 1+length]
		if length == 1 && payload[0] < 0x80 {
			return rlpItem{}, nil, errors.New("non-canonical RLP string")
		}
		return rlpItem{kind: rlpString, payload: payload, raw: input[:1+length]}, input[1+length:], nil
	case prefix <= 0xbf:
		length, offset, err := decodeLongLength(input, int(prefix-0xb7))
		if err != nil {
			return rlpItem{}, nil, err
		}
		if length < 56 {
			return rlpItem{}, nil, errors.New("non-canonical long RLP string")
		}
		if length > len(input)-offset {
			return rlpItem{}, nil, errors.New("truncated RLP string")
		}
		end := offset + length
		return rlpItem{kind: rlpString, payload: input[offset:end], raw: input[:end]}, input[end:], nil
	case prefix <= 0xf7:
		length := int(prefix - 0xc0)
		if len(input) < 1+length {
			return rlpItem{}, nil, errors.New("truncated RLP list")
		}
		item, err := decodeRLPList(input, 1, 1+length, depth)
		if err != nil {
			return rlpItem{}, nil, err
		}
		return item, input[1+length:], nil
	default:
		length, offset, err := decodeLongLength(input, int(prefix-0xf7))
		if err != nil {
			return rlpItem{}, nil, err
		}
		if length < 56 {
			return rlpItem{}, nil, errors.New("non-canonical long RLP list")
		}
		if length > len(input)-offset {
			return rlpItem{}, nil, errors.New("truncated RLP list")
		}
		item, err := decodeRLPList(input, offset, offset+length, depth)
		if err != nil {
			return rlpItem{}, nil, err
		}
		return item, input[offset+length:], nil
	}
}

func decodeRLPList(input []byte, start, end, depth int) (rlpItem, error) {
	payload := input[start:end]
	item := rlpItem{kind: rlpList, payload: payload}
	for remaining := payload; len(remaining) > 0; {
		child, rest, err := decodeRLPAtDepth(remaining, depth+1)
		if err != nil {
			return rlpItem{}, err
		}
		item.children = append(item.children, child)
		remaining = rest
	}
	item.raw = input[:end]
	return item, nil
}

func decodeLongLength(input []byte, lengthBytes int) (int, int, error) {
	if lengthBytes > 8 || len(input) < 1+lengthBytes {
		return 0, 0, errors.New("invalid RLP length prefix")
	}
	encoded := input[1 : 1+lengthBytes]
	if encoded[0] == 0 {
		return 0, 0, errors.New("non-canonical RLP length prefix")
	}
	var length uint64
	for _, b := range encoded {
		length = length<<8 | uint64(b)
	}
	if length > uint64(len(input)) {
		return 0, 0, errors.New("RLP length exceeds input")
	}
	return int(length), 1 + lengthBytes, nil
}

func decodeUint(item rlpItem) (uint64, error) {
	if item.kind != rlpString {
		return 0, errors.New("RLP integer is not a string")
	}
	if len(item.payload) > 8 {
		return 0, errors.New("RLP integer exceeds uint64")
	}
	if len(item.payload) > 0 && item.payload[0] == 0 {
		return 0, errors.New("RLP integer has a leading zero")
	}
	var value uint64
	for _, b := range item.payload {
		value = value<<8 | uint64(b)
	}
	return value, nil
}

func decodeSingleRLP(raw []byte) (rlpItem, error) {
	item, rest, err := decodeRLP(raw)
	if err != nil {
		return rlpItem{}, err
	}
	if len(rest) != 0 {
		return rlpItem{}, errors.New("trailing bytes after RLP item")
	}
	return item, nil
}

func encodeRLPString(payload []byte) []byte {
	if len(payload) == 1 && payload[0] < 0x80 {
		return append([]byte(nil), payload[0])
	}
	if len(payload) <= 55 {
		encoded := make([]byte, 1+len(payload))
		encoded[0] = 0x80 + byte(len(payload))
		copy(encoded[1:], payload)
		return encoded
	}
	return encodeLongItem(0xb7, payload)
}

func encodeRLPList(payload []byte) []byte {
	if len(payload) <= 55 {
		encoded := make([]byte, 1+len(payload))
		encoded[0] = 0xc0 + byte(len(payload))
		copy(encoded[1:], payload)
		return encoded
	}
	return encodeLongItem(0xf7, payload)
}

func encodeLongItem(offset byte, payload []byte) []byte {
	var lengthBuffer [8]byte
	binary.BigEndian.PutUint64(lengthBuffer[:], uint64(len(payload)))
	lengthBytes := lengthBuffer[:]
	for len(lengthBytes) > 1 && lengthBytes[0] == 0 {
		lengthBytes = lengthBytes[1:]
	}
	encoded := make([]byte, 1+len(lengthBytes)+len(payload))
	encoded[0] = offset + byte(len(lengthBytes))
	copy(encoded[1:], lengthBytes)
	copy(encoded[1+len(lengthBytes):], payload)
	return encoded
}

func encodeUint64(value uint64) []byte {
	if value == 0 {
		return encodeRLPString(nil)
	}
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], value)
	encoded := buffer[:]
	for encoded[0] == 0 {
		encoded = encoded[1:]
	}
	return encodeRLPString(encoded)
}
