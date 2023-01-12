package mhash

import (
	"context"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
)

// NewUpdater ...
func NewUpdater[T item.Value, R item.Key, K Key](
	sess memproxy.Session,
	getKey func(v T) K,
	unmarshaler item.Unmarshaler[T],
	filler Filler[T, R],
	maxItemsPerBucket int,
) *HashUpdater[T, R, K] {
	return &HashUpdater[T, R, K]{
		sess:        sess,
		getKey:      getKey,
		unmarshaler: BucketUnmarshalerFromItem[T](unmarshaler),
		filler:      filler,

		maxItemsPerBucket: maxItemsPerBucket,
	}
}

// BucketData ...
type BucketData[R item.Key] struct {
	RootKey R
	Hash    uint64
	Data    []byte
}

type updaterResult[R item.Key] struct {
	buckets []BucketData[R]
	err     error
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

// GetUpsertBuckets ...
func (u *HashUpdater[T, R, K]) GetUpsertBuckets(
	ctx context.Context, rootKey R, value T,
) func() ([]BucketData[R], error) {
	key := u.getKey(value)
	keyHash := key.Hash()

	hashLen := 0
	var result updaterResult[R]

	var fillerFn func() ([]byte, error)
	var nextCallFn func()

	doComputeFn := func() {
		fillerFn = u.filler(ctx, rootKey, computeHashAtLevel(keyHash, hashLen))
		u.sess.AddNextCall(nextCallFn)
	}

	appendResultBucket := func(bucket Bucket[T], hashLen int) {
		newData, err := bucket.Marshal()
		if err != nil {
			result.err = err
			return
		}

		result.buckets = append(result.buckets, BucketData[R]{
			RootKey: rootKey,
			Hash:    computeHashAtLevel(keyHash, hashLen),
			Data:    newData,
		})
	}

	nextCallFn = func() {
		data, err := fillerFn()
		if err != nil {
			result.err = err
			return
		}

		bucket, err := u.unmarshaler(data)
		if err != nil {
			result.err = err
			return
		}

		updated := updateBucketDataItem(&bucket, value, u.getKey)
		if updated {
			appendResultBucket(bucket, hashLen)
			return
		}

		if len(bucket.Items) < u.maxItemsPerBucket {
			bucket.Items = append(bucket.Items, value)
			appendResultBucket(bucket, hashLen)
			return
		}

		offset := computeBitOffsetAtLevel(keyHash, hashLen)
		bucket.Bitset.SetBit(offset)

		sameHashItems := splitBucketItemsWithAndWithoutSameHash(&bucket, keyHash, u.getKey)

		appendResultBucket(bucket, hashLen)

		sameHashItems = append(sameHashItems, value)
		appendResultBucket(Bucket[T]{
			Items: sameHashItems,
		}, hashLen+1)
	}

	doComputeFn()

	return func() ([]BucketData[R], error) {
		u.sess.Execute()
		return result.buckets, result.err
	}
}
