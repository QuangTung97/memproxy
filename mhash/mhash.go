package mhash

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"

	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
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

type updaterFiller[T item.Value, R item.Key] func(ctx context.Context, key BucketKey[R]) func() (Bucket[T], error)

// HashUpdater ...
type HashUpdater[T item.Value, R item.Key, K Key] struct {
	sess         memproxy.Session
	lowerSession memproxy.Session

	getKey      func(v T) K
	unmarshaler item.Unmarshaler[Bucket[T]]
	filler      updaterFiller[T, R]
	upsertFunc  func(key BucketKey[R], bucket Bucket[T], delete bool)
	doUpsert    func()

	maxHashesPerBucket int
}

// New ...
func New[T item.Value, R item.Key, K Key](
	pipeline memproxy.Pipeline,
	getKey func(v T) K,
	unmarshaler item.Unmarshaler[T],
	filler Filler[R],
	options ...Option,
) *Hash[T, R, K] {
	conf := computeConfig(options...)

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
		conf.itemOptions...,
	)

	return &Hash[T, R, K]{
		sess:   bucketItem.LowerSession(),
		getKey: getKey,

		bucketItem: bucketItem,
	}
}

func computeMaskAtLevel(level uint8) uint64 {
	return math.MaxUint64 << (64 - 8*level)
}

func computeHashAtLevel(hash uint64, level uint8) uint64 {
	return hash & computeMaskAtLevel(level)
}

func computeBitOffsetForNextLevel(hash uint64, nextLevel uint8) int {
	offset := (hash >> (64 - nextLevel*8)) & 0xff
	return int(offset)
}

type callContext struct {
	level       uint8
	levelCalls  int
	err         error
	doComputeFn func()
}

func checkContinueOnNextLevel[T item.Value](
	bucket *Bucket[T], keyHash uint64,
	callCtx *callContext,
) (continuing bool) {
	if bucket.NextLevel == 0 {
		return true
	}

	mask := computeMaskAtLevel(bucket.NextLevel - 1)
	if (keyHash & mask) != bucket.NextLevelPrefix {
		return true
	}

	offset := computeBitOffsetForNextLevel(keyHash, bucket.NextLevel)
	if !bucket.Bitset.GetBit(offset) {
		return true
	}

	callCtx.level = bucket.NextLevel

	callCtx.levelCalls++
	if callCtx.levelCalls >= maxDeepLevels {
		callCtx.err = ErrHashTooDeep
		return false
	}

	callCtx.doComputeFn()
	return false
}

// Get ...
func (h *Hash[T, R, K]) Get(ctx context.Context, rootKey R, key K) func() (Null[T], error) {
	keyHash := key.Hash()

	var rootBucketFn func() (Bucket[T], error)
	var nextCallFn func()

	callCtx := callContext{
		level:      0,
		levelCalls: 0,
	}

	doGetFn := func() {
		rootBucketFn = h.bucketItem.Get(ctx, BucketKey[R]{
			RootKey: rootKey,
			Level:   callCtx.level,
			Hash:    computeHashAtLevel(keyHash, callCtx.level),
		})
		h.sess.AddNextCall(memproxy.NewEmptyCallback(nextCallFn))
	}

	callCtx.doComputeFn = doGetFn

	var resp Null[T]
	nextCallFn = func() {
		bucket, err := rootBucketFn()
		if err != nil {
			callCtx.err = err
			return
		}

		continuing := checkContinueOnNextLevel(&bucket, keyHash, &callCtx)
		if !continuing {
			return
		}

		for _, bucketItem := range bucket.Items {
			itemKey := h.getKey(bucketItem)
			if itemKey == key {
				resp = Null[T]{
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
		return resp, callCtx.err
	}
}

// Reset calls item.Reset
func (h *Hash[T, R, K]) Reset() {
	h.bucketItem.Reset()
}
