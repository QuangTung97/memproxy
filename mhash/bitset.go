package mhash

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
