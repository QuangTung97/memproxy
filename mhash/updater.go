package mhash

import (
	"context"

	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
)

type fillFunc = func() ([]byte, error)

type fillResponse[T item.Value] struct {
	bucket  Bucket[T]
	deleted bool
	err     error
}

type emptyStruct = struct{}

type fillContext[T item.Value, R item.Key] struct {
	alreadyFilled bool

	keys   []BucketKey[R]
	keySet map[BucketKey[R]]emptyStruct
}

func (c *fillContext[T, R]) appendFillKey(
	key BucketKey[R], fillResult map[BucketKey[R]]fillResponse[T],
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

func (c *fillContext[T, R]) doFill(
	ctx context.Context,
	inputFiller Filler[R],
	bucketUnmarshaler func(data []byte) (Bucket[T], error),
	fillResult map[BucketKey[R]]fillResponse[T],
) {
	if c.alreadyFilled {
		return
	}
	c.alreadyFilled = true

	fillFuncList := make([]fillFunc, 0, len(c.keys))
	for _, key := range c.keys {
		fillFuncList = append(fillFuncList, inputFiller(ctx, key))
	}

	for i, key := range c.keys {
		var bucket Bucket[T]

		data, err := fillFuncList[i]()
		if err == nil {
			bucket, err = bucketUnmarshaler(data)
		}

		fillResult[key] = fillResponse[T]{
			bucket: bucket,
			err:    err,
		}
	}
}

// NewUpdater ...
//
//revive:disable-next-line:argument-limit
func NewUpdater[T item.Value, R item.Key, K Key](
	sess memproxy.Session,
	getKey func(v T) K,
	unmarshaler item.Unmarshaler[T],
	inputFiller Filler[R],
	upsertFunc func(bucket BucketData[R]),
	deleteFunc func(bucketKey BucketKey[R]),
	maxHashesPerBucket int,
) *HashUpdater[T, R, K] {
	fillResult := map[BucketKey[R]]fillResponse[T]{}

	bucketUnmarshaler := BucketUnmarshalerFromItem[T](unmarshaler)

	var globalFillCtx *fillContext[T, R]

	doFill := func(ctx context.Context, fillCtx *fillContext[T, R]) {
		globalFillCtx = nil
		fillCtx.doFill(ctx, inputFiller, bucketUnmarshaler, fillResult)
	}

	var filler updaterFiller[T, R] = func(ctx context.Context, key BucketKey[R]) func() (Bucket[T], error) {
		if globalFillCtx == nil {
			globalFillCtx = &fillContext[T, R]{
				keys:   nil,
				keySet: map[BucketKey[R]]struct{}{},
			}
		}
		fillCtx := globalFillCtx

		fillCtx.appendFillKey(key, fillResult)

		return func() (Bucket[T], error) {
			doFill(ctx, fillCtx)

			resp := fillResult[key]
			return resp.bucket, resp.err
		}
	}

	var upsertKeys []BucketKey[R]
	upsertKeySet := map[BucketKey[R]]struct{}{}

	updateFuncWrap := func(key BucketKey[R], b Bucket[T], deleted bool) {
		_, existed := upsertKeySet[key]
		if !existed {
			upsertKeySet[key] = struct{}{}
			upsertKeys = append(upsertKeys, key)
		}
		fillResult[key] = fillResponse[T]{
			bucket:  b,
			deleted: deleted,
			err:     nil,
		}
	}

	doUpsert := func() {
		for _, key := range upsertKeys {
			result := fillResult[key]

			data, err := result.bucket.Marshal()
			if err != nil {
				panic(err)
			}

			if result.deleted {
				deleteFunc(key)
			} else {
				upsertFunc(BucketData[R]{
					Key:  key,
					Data: data,
				})
			}
		}
		upsertKeySet = map[BucketKey[R]]struct{}{}
		upsertKeys = nil
	}

	return &HashUpdater[T, R, K]{
		sess:         sess,
		lowerSession: sess.GetLower(),

		getKey:      getKey,
		unmarshaler: bucketUnmarshaler,
		filler:      filler,
		upsertFunc:  updateFuncWrap,
		doUpsert:    doUpsert,

		maxHashesPerBucket: maxHashesPerBucket,
	}
}

func findMaxPrefix[T item.Value, K Key](
	b *Bucket[T],
	currentLevel uint8, // for optimization
	getKey func(T) K,
) (nextLevel uint8, prefix uint64) {
	var level uint8
	var mask, first uint64

	maxLevel := uint8(9)
	firstHash := getKey(b.Items[0]).Hash()

	if b.NextLevel > 0 {
		maxLevel = b.NextLevel
		firstHash = b.NextLevelPrefix
	}

	for level = currentLevel; level < maxLevel; level++ {
		mask = computeMaskAtLevel(level)
		first = firstHash & mask

		for _, it := range b.Items {
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
) {
	bucketKey := BucketKey[R]{
		RootKey: rootKey,
		Level:   level,
		Hash:    computeHashAtLevel(keyHash, level),
	}

	if deleted {
		u.upsertFunc(bucketKey, Bucket[T]{}, deleted)
		return
	}

	u.upsertFunc(bucketKey, bucket, false)
}

// UpsertBucket ...
func (u *HashUpdater[T, R, K]) UpsertBucket(
	ctx context.Context, rootKey R, value T,
) func() error {
	key := u.getKey(value)
	keyHash := key.Hash()

	callCtx := callContext{}

	var fillerFn func() (Bucket[T], error)
	var nextCallFn func(withUpdate bool)

	doComputeWithUpdate := func(withUpdate bool) {
		fillerFn = u.filler(ctx, BucketKey[R]{
			RootKey: rootKey,
			Level:   callCtx.level,
			Hash:    computeHashAtLevel(keyHash, callCtx.level),
		})
		if withUpdate {
			nextCallFn(true)
		} else {
			u.sess.AddNextCall(memproxy.NewEmptyCallback(func() {
				nextCallFn(false)
			}))
		}
	}

	callCtx.doComputeFn = func() {
		doComputeWithUpdate(false)
	}

	nextCallUpdateFn := func(bucket Bucket[T]) {
		updated := updateBucketDataItem(&bucket, value, u.getKey)
		if updated {
			u.doUpsertBucket(bucket, rootKey, keyHash, callCtx.level, false)
			return
		}

		bucket.Items = append(bucket.Items, value)

		if countNumberOfHashes(&bucket, u.getKey) <= u.maxHashesPerBucket {
			u.doUpsertBucket(bucket, rootKey, keyHash, callCtx.level, false)
			return
		}

		nextLevel, prefix := findMaxPrefix[T, K](&bucket, callCtx.level, u.getKey)

		sameHashItems := splitBucketItemsWithAndWithoutSameHash(&bucket, keyHash, u.getKey, nextLevel)

		if bucket.NextLevel > 0 && nextLevel < bucket.NextLevel {
			var bitSet BitSet

			bitSet.SetBit(computeBitOffsetForNextLevel(bucket.NextLevelPrefix, nextLevel))
			bitSet.SetBit(computeBitOffsetForNextLevel(keyHash, nextLevel))

			newBucket := Bucket[T]{
				NextLevel:       nextLevel,
				NextLevelPrefix: prefix,
				Bitset:          bitSet,
			}

			u.doUpsertBucket(newBucket, rootKey, keyHash, callCtx.level, false)

			u.doUpsertBucket(bucket, rootKey, bucket.NextLevelPrefix, nextLevel, false)
		} else {
			bucket.NextLevel = nextLevel
			bucket.NextLevelPrefix = prefix

			offset := computeBitOffsetForNextLevel(keyHash, nextLevel)
			bucket.Bitset.SetBit(offset)

			u.doUpsertBucket(bucket, rootKey, keyHash, callCtx.level, false)
		}

		u.doUpsertBucket(Bucket[T]{
			NextLevel:       0,
			NextLevelPrefix: 0,

			Items: sameHashItems,
		}, rootKey, keyHash, nextLevel, false)
	}

	nextCallFn = func(withUpdate bool) {
		bucket, err := fillerFn()
		if err != nil {
			callCtx.err = err
			return
		}

		continuing := checkContinueOnNextLevel(
			&bucket, keyHash, &callCtx,
		)
		if !continuing {
			return
		}

		if withUpdate {
			nextCallUpdateFn(bucket)
		}
	}

	callCtx.doComputeFn()

	u.lowerSession.AddNextCall(memproxy.NewEmptyCallback(func() {
		callCtx = callContext{}
		callCtx.doComputeFn = func() {
			doComputeWithUpdate(true)
		}
		callCtx.doComputeFn()
	}))

	return func() error {
		u.execute()
		return callCtx.err
	}
}

// DeleteBucket ...
//
//revive:disable-next-line:cognitive-complexity
func (u *HashUpdater[T, R, K]) DeleteBucket(
	ctx context.Context, rootKey R, key K,
) func() error {
	keyHash := key.Hash()

	callCtx := callContext{}
	var scannedBuckets []scannedBucket[T]

	var fillerFn func() (Bucket[T], error)
	var nextCallFn func(withUpdate bool)

	doComputeWithUpdate := func(withUpdate bool) {
		fillerFn = u.filler(ctx, BucketKey[R]{
			RootKey: rootKey,
			Level:   callCtx.level,
			Hash:    computeHashAtLevel(keyHash, callCtx.level),
		})
		if withUpdate {
			nextCallFn(true)
		} else {
			u.sess.AddNextCall(memproxy.NewEmptyCallback(func() {
				nextCallFn(false)
			}))
		}
	}

	callCtx.doComputeFn = func() {
		doComputeWithUpdate(false)
	}

	nextCallFn = func(withUpdate bool) {
		bucket, err := fillerFn()
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

		if withUpdate {
			prevLen := len(bucket.Items)

			bucket.Items = removeItemInList[T](bucket.Items, func(e T) bool {
				return u.getKey(e) == key
			})

			if prevLen == len(bucket.Items) {
				return
			}

			callCtx.err = u.deleteBucketInChain(scannedBuckets, rootKey, keyHash)
		}
	}

	callCtx.doComputeFn()

	u.lowerSession.AddNextCall(memproxy.NewEmptyCallback(func() {
		// clear state
		callCtx = callContext{}
		scannedBuckets = scannedBuckets[:0]

		callCtx.doComputeFn = func() {
			doComputeWithUpdate(true)
		}
		callCtx.doComputeFn()
	}))

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

		u.doUpsertBucket(*bucket, rootKey, keyHash, level, deleted)

		if !deleted {
			return nil
		}

		if level == 0 {
			return nil
		}

		prevBucket := scannedBuckets[calls-1]
		prevBucket.bucket.Bitset.ClearBit(computeBitOffsetForNextLevel(keyHash, level))
	}

	return nil
}

func (u *HashUpdater[T, R, K]) execute() {
	u.lowerSession.Execute()
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
