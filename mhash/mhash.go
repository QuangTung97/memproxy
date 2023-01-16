package mhash

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
	"math"
)

// ErrHashTooDeep when too many levels to go to
var ErrHashTooDeep = errors.New("mhash: hash go too deep")

const maxDeepLevels = int(5)

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
	NextLevel       uint8
	NextLevelPrefix uint64

	Items  []T
	Bitset BitSet
}

// BucketKey ...
type BucketKey[R item.Key] struct {
	RootKey R
	Level   uint8 // from 0
	Hash    uint64
}

// String ...
func (k BucketKey[R]) String() string {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], k.Hash)
	return k.RootKey.String() + ":" + hex.EncodeToString(data[:k.Level])
}

// Filler ...
type Filler[R item.Key] func(ctx context.Context, key BucketKey[R]) func() ([]byte, error)

// Key types
type Key interface {
	comparable
	Hash() uint64
}

// Hash ...
type Hash[T item.Value, R item.Key, K Key] struct {
	sess   memproxy.Session
	getKey func(v T) K

	bucketItem *item.Item[Bucket[T], BucketKey[R]]
}

// BucketData ...
type BucketData[R item.Key] struct {
	Key  BucketKey[R]
	Data []byte
}

// HashUpdater ...
type HashUpdater[T item.Value, R item.Key, K Key] struct {
	sess        memproxy.Session
	getKey      func(v T) K
	unmarshaler item.Unmarshaler[Bucket[T]]
	filler      Filler[R]
	upsertFunc  func(bucket BucketData[R], delete bool)
	doUpsert    func()

	maxHashesPerBucket int
}

// New ...
func New[T item.Value, R item.Key, K Key](
	pipeline memproxy.Pipeline,
	getKey func(v T) K,
	unmarshaler item.Unmarshaler[T],
	filler Filler[R],
) *Hash[T, R, K] {
	bucketUnmarshaler := BucketUnmarshalerFromItem(unmarshaler)

	var bucketFiller item.Filler[Bucket[T], BucketKey[R]] = func(
		ctx context.Context, key BucketKey[R],
	) func() (Bucket[T], error) {
		fn := filler(ctx, key)
		return func() (Bucket[T], error) {
			data, err := fn()
			if err != nil {
				return Bucket[T]{}, err
			}
			return bucketUnmarshaler(data)
		}
	}

	bucketItem := item.New[Bucket[T], BucketKey[R]](
		pipeline, bucketUnmarshaler, bucketFiller,
	)

	return &Hash[T, R, K]{
		sess:   bucketItem.LowerSession(),
		getKey: getKey,

		bucketItem: bucketItem,
	}
}

type getResult[T any] struct {
	resp Null[T]
	err  error
}

func computeHashAtLevel(hash uint64, level uint8) uint64 {
	return hash & (math.MaxUint64 << (64 - 8*level))
}

func computeBitOffsetAtLevel(hash uint64, currentHashLen uint8) int {
	offset := (hash >> (64 - 8 - currentHashLen*8)) & 0xff
	return int(offset)
}

// Get ...
func (h *Hash[T, R, K]) Get(ctx context.Context, rootKey R, key K) func() (Null[T], error) {
	keyHash := key.Hash()

	var rootBucketFn func() (Bucket[T], error)
	var nextCallFn func()

	level := uint8(0)
	levelCalls := 0

	doGetFn := func() {
		rootBucketFn = h.bucketItem.Get(ctx, BucketKey[R]{
			RootKey: rootKey,
			Level:   level,
			Hash:    computeHashAtLevel(keyHash, level),
		})
		h.sess.AddNextCall(nextCallFn)
	}

	var result getResult[T]
	nextCallFn = func() {
		bucket, err := rootBucketFn()
		if err != nil {
			result.err = err
			return
		}

		bitOffset := computeBitOffsetAtLevel(keyHash, level)
		if bucket.Bitset.GetBit(bitOffset) {
			level = bucket.NextLevel
			levelCalls++
			if levelCalls >= maxDeepLevels {
				result.err = ErrHashTooDeep
				return
			}
			doGetFn()
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
	}

	doGetFn()

	return func() (Null[T], error) {
		h.sess.Execute()
		return result.resp, result.err
	}
}
