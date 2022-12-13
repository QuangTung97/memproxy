package mapcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestComputeBucketKey(t *testing.T) {
	assert.Equal(t, uint64(0xf79fb6105ae9c754), hashFunc("key01"))

	assert.Equal(t, "", computeBucketKey("key01", 0))
	assert.Equal(t, "8", computeBucketKey("key01", 1))
	assert.Equal(t, "c", computeBucketKey("key01", 2))
	assert.Equal(t, "e", computeBucketKey("key01", 3))
	assert.Equal(t, "f", computeBucketKey("key01", 4))
	assert.Equal(t, "f7", computeBucketKey("key01", 8))

	assert.Equal(t, "f79", computeBucketKey("key01", 12))
	assert.Equal(t, "f798", computeBucketKey("key01", 13))
	assert.Equal(t, "f79e", computeBucketKey("key01", 15))

	assert.Equal(t, "f79f8", computeBucketKey("key01", 17))
	assert.Equal(t, "f79fa", computeBucketKey("key01", 19))
	assert.Equal(t, "f79fb", computeBucketKey("key01", 20))
}
