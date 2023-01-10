package mhash

import (
	"context"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
)

// Null ...
type Null[T any] struct {
	Valid bool
	Data  T
}

// Bucket ...
type Bucket[T any] struct {
	Items  []T
	Bitset [256]byte
}

// Marshal ...
func (Bucket[T]) Marshal() ([]byte, error) {
	// TODO
	return nil, nil
}

// BucketKey ...
type BucketKey[R item.Key] struct {
	RootKey R
	Hash    uint64
}

// String ...
func (k BucketKey[R]) String() string {
	return k.RootKey.String()
}

// Filler ...
type Filler[T any, R any] func(ctx context.Context, rootKey R, hash uint64) func() (Bucket[T], error)

// Hash ...
type Hash[T item.Value, R item.Key, K item.Key] struct {
	sess     memproxy.Session
	pipeline memproxy.Pipeline
	getKey   func(v T) K
	filler   Filler[T, R]

	bucketItem *item.Item[Bucket[T], BucketKey[R]]
}

// New ...
func New[T item.Value, R item.Key, K item.Key](
	sess memproxy.Session,
	pipeline memproxy.Pipeline,
	getKey func(v T) K,
	unmarshaler item.Unmarshaler[Bucket[T]],
	filler Filler[T, R],
) *Hash[T, R, K] {
	var itemFiller item.Filler[Bucket[T], BucketKey[R]] = func(
		ctx context.Context, key BucketKey[R],
	) func() (Bucket[T], error) {
		return filler(ctx, key.RootKey, key.Hash)
	}

	return &Hash[T, R, K]{
		sess:     sess,
		pipeline: pipeline,
		getKey:   getKey,
		filler:   filler,

		bucketItem: item.New[Bucket[T], BucketKey[R]](
			sess, pipeline, unmarshaler, itemFiller,
		),
	}
}
