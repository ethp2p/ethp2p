package rs

import "math/bits"

type Bitmap []byte

func NewBitmap(n int) Bitmap {
	return make(Bitmap, divCeil(n, 8))
}

func AllOnesBitmap(n int) Bitmap {
	b := make(Bitmap, divCeil(n, 8))
	for i := range n {
		b.Set(i)
	}
	return b
}

func (b Bitmap) Set(index int) {
	if index >= len(b)*8 {
		return
	}
	b[index/8] |= 1 << (uint(index) % 8)
}

func (b Bitmap) Get(index int) bool {
	if index >= len(b)*8 {
		return false
	}
	return b[index/8]&(1<<(uint(index)%8)) != 0
}

func (b Bitmap) Unset(index int) {
	if index >= len(b)*8 {
		return
	}
	b[index/8] &^= 1 << (uint(index) % 8)
}

func (b Bitmap) OnesCount() int {
	var count int
	for i := range b {
		count += bits.OnesCount8(b[i])
	}
	return count
}

func (b Bitmap) IsZero() bool {
	for i := range b {
		if b[i] != 0 {
			return false
		}
	}
	return true
}

func (b Bitmap) Clone() Bitmap {
	clone := make(Bitmap, len(b))
	copy(clone, b)
	return clone
}

func (b Bitmap) Or(other Bitmap) {
	for i := range min(len(b), len(other)) {
		b[i] |= other[i]
	}
}

func (b Bitmap) And(other Bitmap) {
	for i := range min(len(b), len(other)) {
		b[i] &= other[i]
	}
}

func (b Bitmap) Marshal() ([]byte, error)     { return []byte(b), nil }
func (b *Bitmap) Unmarshal(data []byte) error { *b = Bitmap(data); return nil }

func divCeil(n, d int) int {
	return (n + d - 1) / d
}
