package mmap

import (
	"context"

	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
)

// RootKey constraints
type RootKey interface {
	item.Key
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
}

// New ...
func New[T Value, R RootKey, K Key](
	pipeline memproxy.Pipeline,
	unmarshaler item.Unmarshaler[T],
	filler Filler[T, R],
	getKeyFunc func(v T) K,
) *Map[T, R, K] {
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
		),
		getKeyFunc: getKeyFunc,
	}
}

// Option ...
type Option[T any] struct {
	Valid bool
	Data  T
}

func (m *Map[T, R, K]) Get(
	ctx context.Context,
	elemCount uint64,
	rootKey R, key K,
) func() (Option[T], error) {
	fn := m.item.Get(ctx, BucketKey[R]{
		RootKey: rootKey,
		SizeLog: uint8(elemCount),
		Hash:    key.Hash(),
		Sep:     ":",
	})

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
