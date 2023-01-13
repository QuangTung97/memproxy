package mhash

import (
	"context"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
)

type fillFunc = func() ([]byte, error)

type fillResponse[R item.Key] struct {
	data BucketData[R]
	err  error
}

type emptyStruct = struct{}

type fillContext[R item.Key] struct {
	keys   []BucketKey[R]
	keySet map[BucketKey[R]]emptyStruct
}

// NewUpdater ...
func NewUpdater[T item.Value, R item.Key, K Key](
	sess memproxy.Session,
	getKey func(v T) K,
	unmarshaler item.Unmarshaler[T],
	filler Filler[R],
	upsertFunc func(bucket BucketData[R]),
	maxHashesPerBucket int,
) *HashUpdater[T, R, K] {
	fillResult := map[BucketKey[R]]fillResponse[R]{}

	var globalFillCtx *fillContext[R]

	doFill := func(ctx context.Context) {
		fillCtx := globalFillCtx
		if fillCtx == nil {
			return
		}
		globalFillCtx = nil

		fillFuncList := make([]fillFunc, 0, len(fillCtx.keys))
		for _, key := range fillCtx.keys {
			fillFuncList = append(fillFuncList, filler(ctx, key))
		}

		for i, key := range fillCtx.keys {
			data, err := fillFuncList[i]()

			fillResult[key] = fillResponse[R]{
				data: BucketData[R]{
					Key:  key,
					Data: data,
				},
				err: err,
			}
		}
	}

	var updaterFiller Filler[R] = func(ctx context.Context, key BucketKey[R]) func() ([]byte, error) {
		if globalFillCtx == nil {
			globalFillCtx = &fillContext[R]{
				keys:   nil,
				keySet: map[BucketKey[R]]struct{}{},
			}
		}
		fillCtx := globalFillCtx

		_, existed := fillCtx.keySet[key]
		if !existed {
			fillCtx.keys = append(fillCtx.keys, key)
			fillCtx.keySet[key] = struct{}{}
		}

		return func() ([]byte, error) {
			doFill(ctx)

			resp := fillResult[key]
			return resp.data.Data, resp.err
		}
	}

	var upsertKeys []BucketKey[R]
	upsertKeySet := map[BucketKey[R]]struct{}{}

	updateFuncWrap := func(b BucketData[R]) {
		key := b.Key
		_, existed := upsertKeySet[key]
		if !existed {
			upsertKeySet[key] = struct{}{}
			upsertKeys = append(upsertKeys, key)
		}
		fillResult[key] = fillResponse[R]{
			data: b,
			err:  nil,
		}
	}

	doUpsert := func() {
		for _, key := range upsertKeys {
			upsertFunc(fillResult[key].data)
		}
		upsertKeys = nil
	}

	return &HashUpdater[T, R, K]{
		sess:        sess,
		getKey:      getKey,
		unmarshaler: BucketUnmarshalerFromItem[T](unmarshaler),
		filler:      updaterFiller,
		upsertFunc:  updateFuncWrap,
		doUpsert:    doUpsert,

		maxHashesPerBucket: maxHashesPerBucket,
	}
}

func countNumberOfHashes[T item.Value, K Key](
	b *Bucket[T], getKey func(T) K,
) int {
	hashSet := map[uint64]struct{}{}
	for _, bucketItem := range b.Items {
		itemHash := getKey(bucketItem).Hash()
		hashSet[itemHash] = struct{}{}
	}
	return len(hashSet)
}

func splitBucketItemsWithAndWithoutSameHash[T item.Value, K Key](
	b *Bucket[T], hash uint64, getKey func(T) K,
) (sameHashItems []T) {
	sameHash := false
	for _, bucketItem := range b.Items {
		itemHash := getKey(bucketItem).Hash()
		if itemHash == hash {
			sameHash = true
			break
		}
	}

	if !sameHash {
		return nil
	}

	newItems := make([]T, 0, len(b.Items))
	for _, bucketItem := range b.Items {
		itemHash := getKey(bucketItem).Hash()
		if itemHash == hash {
			sameHashItems = append(sameHashItems, bucketItem)
		} else {
			newItems = append(newItems, bucketItem)
		}
	}

	b.Items = newItems
	return sameHashItems
}

func updateBucketDataItem[T item.Value, K Key](
	b *Bucket[T], value T, getKey func(T) K,
) bool {
	key := getKey(value)

	for i, bucketItem := range b.Items {
		bucketKey := getKey(bucketItem)
		if key == bucketKey {
			b.Items[i] = value
			return true
		}
	}

	return false
}

// UpsertBuckets ...
func (u *HashUpdater[T, R, K]) UpsertBuckets(
	ctx context.Context, rootKey R, value T,
) func() error {
	key := u.getKey(value)
	keyHash := key.Hash()

	level := 0
	var resultErr error

	var fillerFn func() ([]byte, error)
	var nextCallFn func()

	doComputeFn := func() {
		fillerFn = u.filler(ctx, BucketKey[R]{
			RootKey: rootKey,
			Hash:    computeHashAtLevel(keyHash, level),
			Level:   level,
		})
		u.sess.AddNextCall(nextCallFn)
	}

	upsertBucket := func(bucket Bucket[T], level int) {
		newData, err := bucket.Marshal()
		if err != nil {
			resultErr = err
			return
		}

		u.upsertFunc(BucketData[R]{
			Key: BucketKey[R]{
				RootKey: rootKey,
				Hash:    computeHashAtLevel(keyHash, level),
				Level:   level,
			},
			Data: newData,
		})
	}

	nextCallFn = func() {
		data, err := fillerFn()
		if err != nil {
			resultErr = err
			return
		}

		bucket, err := u.unmarshaler(data)
		if err != nil {
			resultErr = err
			return
		}

		offset := computeBitOffsetAtLevel(keyHash, level)
		if bucket.Bitset.GetBit(offset) {
			level++ // TODO Error Too Many Levels
			doComputeFn()
			return
		}

		updated := updateBucketDataItem(&bucket, value, u.getKey)
		if updated {
			upsertBucket(bucket, level)
			return
		}

		if countNumberOfHashes(&bucket, u.getKey) < u.maxHashesPerBucket {
			bucket.Items = append(bucket.Items, value)
			upsertBucket(bucket, level)
			return
		}

		bucket.Bitset.SetBit(offset)

		sameHashItems := splitBucketItemsWithAndWithoutSameHash(&bucket, keyHash, u.getKey)

		upsertBucket(bucket, level)

		sameHashItems = append(sameHashItems, value)

		upsertBucket(Bucket[T]{
			Items: sameHashItems,
		}, level+1)
	}

	doComputeFn()

	return func() error {
		u.sess.Execute()
		u.doUpsert()
		return resultErr
	}
}

// DeleteBuckets ...
func (*HashUpdater[T, R, K]) DeleteBuckets(
	_ context.Context, _ R,
) func() error {
	return func() error {
		return nil
	}
}
