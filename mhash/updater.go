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
) *HashUpdater[T, R, K] {
	return &HashUpdater[T, R, K]{
		sess:        sess,
		getKey:      getKey,
		unmarshaler: BucketUnmarshalerFromItem[T](unmarshaler),
		filler:      filler,
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

// GetUpsertBuckets ...
func (u *HashUpdater[T, R, K]) GetUpsertBuckets(
	ctx context.Context, rootKey R, value T,
) func() ([]BucketData[R], error) {
	keyHash := u.getKey(value).Hash()

	hashLen := 0
	var result updaterResult[R]

	fn := u.filler(ctx, rootKey, computeHashAtLevel(keyHash, hashLen))
	u.sess.AddNextCall(func() {
		data, err := fn()
		if err != nil {
			result.err = err
			return
		}

		bucket, err := u.unmarshaler(data)
		if err != nil {
			result.err = err
			return
		}

		bucket.Items = append(bucket.Items, value)

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
	})

	return func() ([]BucketData[R], error) {
		u.sess.Execute()
		return result.buckets, result.err
	}
}
