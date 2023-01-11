package mhash

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBitSet(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var b BitSet
		assert.Equal(t, false, b.GetBit(0))
		assert.Equal(t, false, b.GetBit(10))
		assert.Equal(t, false, b.GetBit(255))
	})

	t.Run("set-and-clear", func(t *testing.T) {
		var b BitSet

		b.SetBit(7)

		assert.Equal(t, false, b.GetBit(6))
		assert.Equal(t, true, b.GetBit(7))
		assert.Equal(t, false, b.GetBit(8))

		b.ClearBit(7)

		assert.Equal(t, false, b.GetBit(6))
		assert.Equal(t, false, b.GetBit(7))
		assert.Equal(t, false, b.GetBit(8))
	})

	t.Run("set-and-clear-multi", func(t *testing.T) {
		var b BitSet

		b.SetBit(0)

		b.SetBit(6)
		b.SetBit(7)

		b.SetBit(8)

		b.SetBit(15)
		b.SetBit(16)

		assert.Equal(t, true, b.GetBit(0))
		assert.Equal(t, false, b.GetBit(1))

		assert.Equal(t, false, b.GetBit(5))
		assert.Equal(t, true, b.GetBit(6))
		assert.Equal(t, true, b.GetBit(7))
		assert.Equal(t, true, b.GetBit(8))
		assert.Equal(t, false, b.GetBit(9))

		b.ClearBit(7)

		assert.Equal(t, true, b.GetBit(6))
		assert.Equal(t, false, b.GetBit(7))
		assert.Equal(t, true, b.GetBit(8))

		assert.Equal(t, true, b.GetBit(15))
		assert.Equal(t, true, b.GetBit(16))

		b.ClearBit(15)

		assert.Equal(t, false, b.GetBit(15))
		assert.Equal(t, true, b.GetBit(16))
	})

	t.Run("set-and-clear-at-bound", func(t *testing.T) {
		var b BitSet

		b.SetBit(255)

		assert.Equal(t, false, b.GetBit(254))
		assert.Equal(t, true, b.GetBit(255))

		b.ClearBit(255)

		assert.Equal(t, false, b.GetBit(254))
		assert.Equal(t, false, b.GetBit(254))
	})

	t.Run("set-outside-bound", func(t *testing.T) {
		var b BitSet

		assert.Panics(t, func() {
			b.SetBit(256)
		})
	})

	t.Run("clear-outside-bound", func(t *testing.T) {
		var b BitSet

		assert.Panics(t, func() {
			b.ClearBit(256)
		})
	})

	t.Run("get-outside-bound", func(t *testing.T) {
		var b BitSet

		assert.Panics(t, func() {
			b.GetBit(256)
		})
	})
}
