package mapcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func computeBucketKeyString(key string, sizeLog uint64) string {
	return computeBucketKey(hashFunc(key), sizeLog)
}

func TestComputeBucketKey(t *testing.T) {
	assert.Equal(t, uint64(0xf79fb6105ae9c754), hashFunc("key01"))

	assert.Equal(t, "", computeBucketKeyString("key01", 0))
	assert.Equal(t, "8", computeBucketKeyString("key01", 1))
	assert.Equal(t, "c", computeBucketKeyString("key01", 2))
	assert.Equal(t, "e", computeBucketKeyString("key01", 3))
	assert.Equal(t, "f", computeBucketKeyString("key01", 4))
	assert.Equal(t, "f7", computeBucketKeyString("key01", 8))

	assert.Equal(t, "f79", computeBucketKeyString("key01", 12))
	assert.Equal(t, "f798", computeBucketKeyString("key01", 13))
	assert.Equal(t, "f79e", computeBucketKeyString("key01", 15))

	assert.Equal(t, "f79f8", computeBucketKeyString("key01", 17))
	assert.Equal(t, "f79fa", computeBucketKeyString("key01", 19))
	assert.Equal(t, "f79fb", computeBucketKeyString("key01", 20))
}

func TestComputeHashRange(t *testing.T) {
	assert.Equal(t, HashRange{
		Begin: 0xf600000000000000,
		End:   0xf7ffffffffffffff,
	}, computeHashRange(0xf79fb6105ae9c754, 7))

	assert.Equal(t, HashRange{
		Begin: 0xf700000000000000,
		End:   0xf7ffffffffffffff,
	}, computeHashRange(0xf79fb6105ae9c754, 8))

	assert.Equal(t, HashRange{
		Begin: 0xf790000000000000,
		End:   0xf79fffffffffffff,
	}, computeHashRange(0xf79fb6105ae9c754, 12))

	assert.Equal(t, HashRange{
		Begin: 0xf780000000000000,
		End:   0xf7bfffffffffffff,
	}, computeHashRange(0xf79fb6105ae9c754, 10))

	assert.Equal(t, HashRange{
		Begin: 0x0000000000000000,
		End:   0xffffffffffffffff,
	}, computeHashRange(0xf79fb6105ae9c754, 0))

	assert.Equal(t, HashRange{
		Begin: 0x8000000000000000,
		End:   0xffffffffffffffff,
	}, computeHashRange(0xf79fb6105ae9c754, 1))
}

func TestMarshalCacheBucket(t *testing.T) {
	bucket := CacheBucketContent{
		OriginSizeLogVersion: 0x2233,
		Entries: []Entry{
			{
				Key:  "KEY01",
				Data: []byte("key data 01"),
			},
			{
				Key:  "KEY02",
				Data: []byte("key data 02"),
			},
		},
	}

	data := marshalCacheBucket(bucket)
	resultBucket, err := unmarshalCacheBucket(data)
	assert.Equal(t, nil, err)
	assert.Equal(t, bucket, resultBucket)
}

func TestUnmarshalCacheBucket_Error(t *testing.T) {
	resultBucket, err := unmarshalCacheBucket(nil)
	assert.Equal(t, ErrMissingBucketContent, err)
	assert.Equal(t, CacheBucketContent{}, resultBucket)

	resultBucket, err = unmarshalCacheBucket([]byte{2})
	assert.Equal(t, ErrInvalidBucketContentVersion, err)
	assert.Equal(t, CacheBucketContent{}, resultBucket)

	resultBucket, err = unmarshalCacheBucket([]byte{1})
	assert.Equal(t, ErrMissingSizeLogOrigin, err)
	assert.Equal(t, CacheBucketContent{}, resultBucket)

	resultBucket, err = unmarshalCacheBucket([]byte{1, 22, 0, 0, 0, 0, 0, 0, 0})
	assert.Equal(t, ErrMissingLength, err)
	assert.Equal(t, CacheBucketContent{}, resultBucket)

	resultBucket, err = unmarshalCacheBucket([]byte{1, 22, 0, 0, 0, 0, 0, 0, 0, 2})
	assert.Equal(t, ErrMissingLength, err)
	assert.Equal(t, CacheBucketContent{}, resultBucket)

	resultBucket, err = unmarshalCacheBucket([]byte{1, 22, 0, 0, 0, 0, 0, 0, 0, 2, 1, 'A'})
	assert.Equal(t, ErrMissingLength, err)
	assert.Equal(t, CacheBucketContent{}, resultBucket)
}

func BenchmarkGoMapSet(b *testing.B) {
	type structKey struct {
	}
	key := &structKey{}
	m := map[interface{}]interface{}{}
	m[key] = 245

	for n := 0; n < b.N; n++ {
		_ = m[key]
	}
}
