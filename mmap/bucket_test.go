package mmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testRootKey struct {
	prefix string
}

func (k testRootKey) String() string {
	return k.prefix
}

// length in bytes
func newHash(prefix uint64, length int) uint64 {
	size := length * 8
	return prefix << (64 - size)

}

func TestNewHash(t *testing.T) {
	assert.Equal(t, uint64(0x1234_5600_0000_0000), newHash(0x1234_56, 3))
}

func TestBucketKey_String(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 3 * 8,
			Hash:    newHash(0x1234_5678, 4),
			Sep:     ":",
		}
		assert.Equal(t, "hello:24:123456", k.String())
	})

	t.Run("near align with bytes", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 23,
			Hash:    newHash(0x1234_ff78, 4),
			Sep:     "/",
		}
		assert.Equal(t, "hello/23/1234fe", k.String())
	})

	t.Run("middle of byte", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 12,
			Hash:    newHash(0x1234_ff78, 4),
			Sep:     "/",
		}
		assert.Equal(t, "hello/12/123", k.String())
	})

	t.Run("single digit", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 4,
			Hash:    newHash(0x5234_ff78, 4),
			Sep:     "/",
		}
		assert.Equal(t, "hello/4/5", k.String())
	})

	t.Run("single bit", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 1,
			Hash:    newHash(0xf4, 1),
			Sep:     "/",
		}
		assert.Equal(t, "hello/1/8", k.String())
	})

	t.Run("size log zero", func(t *testing.T) {
		k := BucketKey[testRootKey]{
			RootKey: testRootKey{
				prefix: "hello",
			},
			SizeLog: 0,
			Hash:    newHash(0xf4, 1),
			Sep:     "/",
		}
		assert.Equal(t, "hello/0/", k.String())
	})
}
