package mhash

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
)

// Null ...
type Null[T any] struct {
	Valid bool
	Data  T
}

const bitSetShift = 3
const bitSetMask = 1<<bitSetShift - 1
const bitSetBytes = 256 / (1 << bitSetShift)

// BitSet ...
type BitSet [bitSetBytes]byte

// Bucket ...
type Bucket[T item.Value] struct {
	Items  []T
	Bitset BitSet
}

// BucketKey ...
type BucketKey[R item.Key] struct {
	RootKey R
	Hash    uint64
	HashLen int
}

// String ...
func (k BucketKey[R]) String() string {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], k.Hash)
	return k.RootKey.String() + ":" + hex.EncodeToString(data[:k.HashLen])
}

// Filler ...
type Filler[T any, R any] func(ctx context.Context, rootKey R, hash uint64) func() ([]byte, error)

// Key types
type Key interface {
	comparable
	Hash() uint64
}

// Hash ...
type Hash[T item.Value, R item.Key, K Key] struct {
	sess     memproxy.Session
	pipeline memproxy.Pipeline
	getKey   func(v T) K
	filler   Filler[T, R]

	bucketItem *item.Item[Bucket[T], BucketKey[R]]
}

// New ...
func New[T item.Value, R item.Key, K Key](
	sess memproxy.Session,
	pipeline memproxy.Pipeline,
	getKey func(v T) K,
	unmarshaler item.Unmarshaler[T],
	filler Filler[T, R],
) *Hash[T, R, K] {
	bucketUnmarshaler := BucketUnmarshalerFromItem(unmarshaler)

	var bucketFiller item.Filler[Bucket[T], BucketKey[R]] = func(
		ctx context.Context, key BucketKey[R],
	) func() (Bucket[T], error) {
		fn := filler(ctx, key.RootKey, key.Hash)
		return func() (Bucket[T], error) {
			data, err := fn()
			if err != nil {
				return Bucket[T]{}, nil
			}
			return bucketUnmarshaler(data)
		}
	}

	return &Hash[T, R, K]{
		sess:     sess,
		pipeline: pipeline,
		getKey:   getKey,
		filler:   filler,

		bucketItem: item.New[Bucket[T], BucketKey[R]](
			sess, pipeline, bucketUnmarshaler, bucketFiller,
		),
	}
}

type getResult[T any] struct {
	resp Null[T]
	err  error
}

// Get ...
func (h *Hash[T, R, K]) Get(ctx context.Context, rootKey R, key K) func() (Null[T], error) {
	rootBucketFn := h.bucketItem.Get(ctx, BucketKey[R]{
		RootKey: rootKey,
		Hash:    0,
		HashLen: 1,
	})

	var result getResult[T]

	h.sess.AddNextCall(func() {
		bucket, err := rootBucketFn()
		if err != nil {
			result.err = err
			return
		}

		for _, bucketItem := range bucket.Items {
			itemKey := h.getKey(bucketItem)
			if itemKey == key {
				result.resp = Null[T]{
					Valid: true,
					Data:  bucketItem,
				}
				return
			}
		}
	})

	return func() (Null[T], error) {
		h.sess.Execute()
		return result.resp, result.err
	}
}
