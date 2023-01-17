package mhash

import "math/bits"

// SetBit ...
func (b *BitSet) SetBit(index int) {
	pos := index >> bitSetShift
	offset := index & bitSetMask
	b[pos] |= 1 << offset
}

// ClearBit ...
func (b *BitSet) ClearBit(index int) {
	pos := index >> bitSetShift
	offset := index & bitSetMask
	b[pos] &= ^(1 << offset)
}

// GetBit ...
func (b *BitSet) GetBit(index int) bool {
	pos := index >> bitSetShift
	offset := index & bitSetMask
	return b[pos]&(1<<offset) != 0
}

// IsZero ...
func (b *BitSet) IsZero() bool {
	for _, e := range b {
		if e != 0 {
			return false
		}
	}
	return true
}

// Count ...
func (b *BitSet) Count() int {
	total := 0
	for _, e := range b {
		total += bits.OnesCount8(e)
	}
	return total
}
