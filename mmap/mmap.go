package mmap

import (
	"context"
	"math"
	"math/bits"

	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
)

// RootKey constraints
type RootKey interface {
	item.Key
	AvgBucketSizeLog() uint8
}

// Key child key constraint
type Key interface {
	comparable
	Hash() uint64
}

// Value constraints
type Value interface {
	item.Value
}

// HashRange for range of value [begin, end] includes both ends
// More specific: begin <= h <= end
// For each hash value h
type HashRange struct {
	Begin uint64 // inclusive
	End   uint64 // inclusive
}

// Filler ...
type Filler[T any, R any] func(ctx context.Context, rootKey R, hashRange HashRange) func() ([]T, error)

// Map ...
type Map[T Value, R RootKey, K Key] struct {
	item *item.Item[Bucket[T], BucketKey[R]]

	getKeyFunc func(v T) K
	separator  string
}

// New ...
func New[T Value, R RootKey, K Key](
	pipeline memproxy.Pipeline,
	unmarshaler item.Unmarshaler[T],
	filler Filler[T, R],
	getKeyFunc func(v T) K,
	options ...MapOption,
) *Map[T, R, K] {
	conf := computeMapConfig(options)

	bucketFiller := func(ctx context.Context, key BucketKey[R]) func() (Bucket[T], error) {
		fn := filler(ctx, key.RootKey, key.GetHashRange())
		return func() (Bucket[T], error) {
			values, err := fn()
			if err != nil {
				return Bucket[T]{}, err
			}
			return Bucket[T]{
				Values: values,
			}, nil
		}
	}

	return &Map[T, R, K]{
		item: item.New[Bucket[T], BucketKey[R]](
			pipeline,
			NewBucketUnmarshaler(unmarshaler),
			bucketFiller,
			conf.itemOptions...,
		),
		getKeyFunc: getKeyFunc,
		separator:  conf.separator,
	}
}

// Option ...
type Option[T any] struct {
	Valid bool
	Data  T
}

func computeSizeLog(
	avgBucketSizeLog uint8,
	elemCount uint64,
	hash uint64,
) uint8 {
	avgBucketSize := uint64(1) << avgBucketSizeLog
	if elemCount <= avgBucketSize {
		return 0
	}

	sizeLog := uint8(bits.Len64(elemCount-1)) - avgBucketSizeLog

	prevSizeLog := uint64(1) << (avgBucketSizeLog + sizeLog - 1)

	var boundEnd uint64
	if avgBucketSizeLog >= 1 {
		boundValue := (elemCount - 1 - prevSizeLog) >> (avgBucketSizeLog - 1)
		boundEnd = boundValue<<(64-sizeLog) | (math.MaxUint64 >> sizeLog)
	} else {
		boundValue := elemCount - 1 - prevSizeLog
		shift := sizeLog - 1
		boundEnd = boundValue<<(64-shift) | (math.MaxUint64 >> shift)
	}

	if hash <= boundEnd {
		return sizeLog
	}
	return sizeLog - 1
}

// ComputeBucketKey ...
func ComputeBucketKey[R RootKey, K Key](
	elemCount uint64,
	rootKey R, key K,
	separator string,
) BucketKey[R] {
	hash := key.Hash()

	return BucketKey[R]{
		RootKey: rootKey,
		SizeLog: computeSizeLog(rootKey.AvgBucketSizeLog(), elemCount, hash),
		Hash:    hash,
		Sep:     separator,
	}
}

// ComputeBucketKeyString ...
func ComputeBucketKeyString[R RootKey, K Key](
	elemCount uint64,
	rootKey R, key K,
) string {
	return ComputeBucketKeyStringWithSeparator(elemCount, rootKey, key, ":")
}

// ComputeBucketKeyStringWithSeparator ...
func ComputeBucketKeyStringWithSeparator[R RootKey, K Key](
	elemCount uint64,
	rootKey R, key K,
	separator string,
) string {
	return ComputeBucketKey(elemCount, rootKey, key, separator).String()
}

// Get from Map
// The elemCount need *NOT* be exact, but *MUST* be monotonically increasing
// Otherwise Map can return incorrect values
func (m *Map[T, R, K]) Get(
	ctx context.Context,
	elemCount uint64,
	rootKey R, key K,
) func() (Option[T], error) {
	bucketKey := ComputeBucketKey(elemCount, rootKey, key, m.separator)

	fn := m.item.Get(ctx, bucketKey)

	return func() (Option[T], error) {
		bucket, err := fn()
		if err != nil {
			return Option[T]{}, err
		}

		for _, v := range bucket.Values {
			if m.getKeyFunc(v) == key {
				return Option[T]{
					Valid: true,
					Data:  v,
				}, nil
			}
		}

		return Option[T]{}, nil
	}
}
