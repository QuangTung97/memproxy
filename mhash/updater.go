package mhash

import (
	"context"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
)

type fillFunc = func() ([]byte, error)

type fillResponse[R item.Key] struct {
	data    BucketData[R]
	deleted bool
	err     error
}

type emptyStruct = struct{}

type fillContext[R item.Key] struct {
	keys   []BucketKey[R]
	keySet map[BucketKey[R]]emptyStruct
}

func (c *fillContext[R]) appendFillKey(
	key BucketKey[R], fillResult map[BucketKey[R]]fillResponse[R],
) {
	_, existed := c.keySet[key]
	if existed {
		return
	}

	_, fillResultExisted := fillResult[key]
	if fillResultExisted {
		return
	}

	c.keySet[key] = struct{}{}
	c.keys = append(c.keys, key)
}

// NewUpdater ...
//
//revive:disable-next-line:argument-limit
func NewUpdater[T item.Value, R item.Key, K Key](
	sess memproxy.Session,
	getKey func(v T) K,
	unmarshaler item.Unmarshaler[T],
	filler Filler[R],
	upsertFunc func(bucket BucketData[R]),
	deleteFunc func(bucketKey BucketKey[R]),
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

		fillCtx.appendFillKey(key, fillResult)

		return func() ([]byte, error) {
			doFill(ctx)

			resp := fillResult[key]
			return resp.data.Data, resp.err
		}
	}

	var upsertKeys []BucketKey[R]
	upsertKeySet := map[BucketKey[R]]struct{}{}

	updateFuncWrap := func(b BucketData[R], deleted bool) {
		key := b.Key
		_, existed := upsertKeySet[key]
		if !existed {
			upsertKeySet[key] = struct{}{}
			upsertKeys = append(upsertKeys, key)
		}
		fillResult[key] = fillResponse[R]{
			data:    b,
			deleted: deleted,
			err:     nil,
		}
	}

	doUpsert := func() {
		for _, key := range upsertKeys {
			result := fillResult[key]
			if result.deleted {
				deleteFunc(result.data.Key)
			} else {
				upsertFunc(result.data)
			}
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

func findMaxPrefix[T item.Value, K Key](
	b *Bucket[T], currentLevel uint8, getKey func(T) K,
) (nextLevel uint8, prefix uint64) {
	var level uint8
	var mask, first uint64

	for level = currentLevel + 1; level < 9; level++ {
		mask = computeMaskAtLevel(level)
		first = getKey(b.Items[0]).Hash() & mask

		for _, it := range b.Items[1:] {
			hash := getKey(it).Hash() & mask
			if first != hash {
				return level, first & (mask << 8)
			}
		}
	}
	return level, first
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

func hashPrefixEqual(a, b uint64, level uint8) bool {
	return computeHashAtLevel(a, level) == computeHashAtLevel(b, level)
}

func splitBucketItemsWithAndWithoutSameHash[T item.Value, K Key](
	b *Bucket[T], inputHash uint64, getKey func(T) K, level uint8,
) (sameHashItems []T) {
	sameHash := false
	for _, bucketItem := range b.Items {
		itemHash := getKey(bucketItem).Hash()
		if hashPrefixEqual(itemHash, inputHash, level) {
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
		if hashPrefixEqual(itemHash, inputHash, level) {
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

//revive:disable-next-line:flag-parameter
func (u *HashUpdater[T, R, K]) doUpsertBucket(
	bucket Bucket[T],
	rootKey R, keyHash uint64, level uint8,
	deleted bool,
) error {
	bucketKey := BucketKey[R]{
		RootKey: rootKey,
		Level:   level,
		Hash:    computeHashAtLevel(keyHash, level),
	}

	if deleted {
		u.upsertFunc(BucketData[R]{Key: bucketKey}, deleted)
		return nil
	}

	newData, err := bucket.Marshal()
	if err != nil {
		return err
	}

	u.upsertFunc(BucketData[R]{
		Key:  bucketKey,
		Data: newData,
	}, deleted)
	return nil
}

// UpsertBucket ...
func (u *HashUpdater[T, R, K]) UpsertBucket(
	ctx context.Context, rootKey R, value T,
) func() error {
	key := u.getKey(value)
	keyHash := key.Hash()

	updateCtx := callContext{
		level:      0,
		levelCalls: 0,
	}

	var fillerFn func() ([]byte, error)
	var nextCallFn func()

	doComputeFn := func() {
		fillerFn = u.filler(ctx, BucketKey[R]{
			RootKey: rootKey,
			Level:   updateCtx.level,
			Hash:    computeHashAtLevel(keyHash, updateCtx.level),
		})
		u.sess.AddNextCall(nextCallFn)
	}
	updateCtx.doComputeFn = doComputeFn

	nextCallFn = func() {
		data, err := fillerFn()
		if err != nil {
			updateCtx.err = err
			return
		}

		bucket, err := u.unmarshaler(data)
		if err != nil {
			updateCtx.err = err
			return
		}

		continuing := checkContinueOnNextLevel(
			&bucket, keyHash, &updateCtx,
		)
		if !continuing {
			return
		}

		updated := updateBucketDataItem(&bucket, value, u.getKey)
		if updated {
			updateCtx.err = u.doUpsertBucket(bucket, rootKey, keyHash, updateCtx.level, false)
			return
		}

		if countNumberOfHashes(&bucket, u.getKey) < u.maxHashesPerBucket {
			bucket.Items = append(bucket.Items, value)
			updateCtx.err = u.doUpsertBucket(bucket, rootKey, keyHash, updateCtx.level, false)
			return
		}

		nextLevel, prefix := findMaxPrefix[T, K](&bucket, updateCtx.level, u.getKey)
		bucket.NextLevel = nextLevel
		bucket.NextLevelPrefix = prefix

		offset := computeBitOffsetForNextLevel(keyHash, nextLevel)
		bucket.Bitset.SetBit(offset)

		sameHashItems := splitBucketItemsWithAndWithoutSameHash(&bucket, keyHash, u.getKey, nextLevel)

		err = u.doUpsertBucket(bucket, rootKey, keyHash, updateCtx.level, false)
		if err != nil {
			updateCtx.err = err
			return
		}

		sameHashItems = append(sameHashItems, value)

		updateCtx.err = u.doUpsertBucket(Bucket[T]{
			NextLevel:       0,
			NextLevelPrefix: 0,

			Items: sameHashItems,
		}, rootKey, keyHash, nextLevel, false)
	}

	doComputeFn()

	return func() error {
		u.execute()
		return updateCtx.err
	}
}

// DeleteBucket ...
//
//revive:disable-next-line:cognitive-complexity
func (u *HashUpdater[T, R, K]) DeleteBucket(
	ctx context.Context, rootKey R, key K,
) func() error {
	keyHash := key.Hash()

	callCtx := callContext{
		level:      0,
		levelCalls: 0,
	}

	var fillerFn func() ([]byte, error)
	var nextCallFn func()

	var scannedBuckets []scannedBucket[T]

	doComputeFn := func() {
		fillerFn = u.filler(ctx, BucketKey[R]{
			RootKey: rootKey,
			Level:   callCtx.level,
			Hash:    computeHashAtLevel(keyHash, callCtx.level),
		})
		u.sess.AddNextCall(nextCallFn)
	}
	callCtx.doComputeFn = doComputeFn

	nextCallFn = func() {
		data, err := fillerFn()
		if err != nil {
			callCtx.err = err
			return
		}

		bucket, err := u.unmarshaler(data)
		if err != nil {
			callCtx.err = err
			return
		}
		scannedBuckets = append(scannedBuckets, scannedBucket[T]{
			bucket: &bucket,
			level:  callCtx.level,
		})

		continuing := checkContinueOnNextLevel(&bucket, keyHash, &callCtx)
		if !continuing {
			return
		}

		prevLen := len(bucket.Items)

		bucket.Items = removeItemInList[T](bucket.Items, func(e T) bool {
			return u.getKey(e) == key
		})

		if prevLen == len(bucket.Items) {
			return
		}

		callCtx.err = u.deleteBucketInChain(scannedBuckets, rootKey, keyHash)
	}

	doComputeFn()

	return func() error {
		u.execute()
		return callCtx.err
	}
}

type scannedBucket[T item.Value] struct {
	level  uint8
	bucket *Bucket[T]
}

func (u *HashUpdater[T, R, K]) deleteBucketInChain(
	scannedBuckets []scannedBucket[T],
	rootKey R, keyHash uint64,
) error {
	n := len(scannedBuckets)

	for calls := n - 1; calls >= 0; calls-- {
		scanned := scannedBuckets[calls]
		bucket := scanned.bucket
		level := scanned.level

		bucketIsZero := bucket.Bitset.IsZero()
		if bucketIsZero {
			bucket.NextLevel = 0
			bucket.NextLevelPrefix = 0
		}

		deleted := len(bucket.Items) == 0 && bucketIsZero

		err := u.doUpsertBucket(*bucket, rootKey, keyHash, level, deleted)
		if err != nil {
			return err
		}

		if !deleted {
			return nil
		}

		if level == 0 {
			return nil
		}

		prevBucket := scannedBuckets[level-1]
		prevBucket.bucket.Bitset.ClearBit(computeBitOffsetForNextLevel(keyHash, level))
	}

	return nil
}

func (u *HashUpdater[T, R, K]) execute() {
	u.sess.Execute()
	u.doUpsert()
}

func removeItemInList[T any](array []T, cond func(e T) bool) []T {
	n := len(array)
	for i := 0; i < n; {
		if cond(array[i]) {
			array[i], array[n-1] = array[n-1], array[i]
			n--
		} else {
			i++
		}
	}
	return array[:n]
}
