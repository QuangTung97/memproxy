package mhash

import (
	"context"
	"fmt"
	"github.com/QuangTung97/memproxy"
	"github.com/stretchr/testify/assert"
	"testing"
)

type updaterTest struct {
	updater *HashUpdater[customerUsage, customerUsageRootKey, customerUsageKey]

	fillerFunc     Filler[customerUsageRootKey]
	fillerRootKeys []customerUsageRootKey
	fillerHashList []uint64
	fillerLevels   []int

	upsertBuckets []BucketData[customerUsageRootKey]
}

func newUpdaterTest(maxHashesPerBucket int) *updaterTest {
	sess := &memproxy.SessionMock{}

	var calls []func()
	sess.AddNextCallFunc = func(fn func()) {
		calls = append(calls, fn)
	}
	sess.ExecuteFunc = func() {
		for len(calls) > 0 {
			nextCalls := calls
			calls = nil
			for _, fn := range nextCalls {
				fn()
			}
		}
	}

	u := &updaterTest{}

	var filler Filler[customerUsageRootKey] = func(
		ctx context.Context, key BucketKey[customerUsageRootKey],
	) func() ([]byte, error) {
		u.fillerRootKeys = append(u.fillerRootKeys, key.RootKey)
		u.fillerHashList = append(u.fillerHashList, key.Hash)
		u.fillerLevels = append(u.fillerLevels, key.Level)

		if u.fillerFunc == nil {
			panic("fillerFunc is nil")
		}

		return u.fillerFunc(ctx, key)
	}

	u.updater = NewUpdater[customerUsage, customerUsageRootKey, customerUsageKey](
		sess, customerUsage.getKey, unmarshalCustomerUsage, filler,
		func(bucket BucketData[customerUsageRootKey]) {
			u.upsertBuckets = append(u.upsertBuckets, bucket)
		},
		maxHashesPerBucket,
	)

	return u
}

func (u *updaterTest) stubFill(data []byte, err error) {
	u.fillerFunc = func(ctx context.Context, key BucketKey[customerUsageRootKey]) func() ([]byte, error) {
		return func() ([]byte, error) {
			return data, err
		}
	}
}

func (u *updaterTest) stubFillMulti(dataList ...[]byte) {
	u.fillerFunc = func(ctx context.Context, key BucketKey[customerUsageRootKey]) func() ([]byte, error) {
		index := len(u.fillerRootKeys) - 1
		return func() ([]byte, error) {
			return dataList[index], nil
		}
	}
}

func TestUpdater(t *testing.T) {
	t.Run("insert-from-empty", func(t *testing.T) {
		u := newUpdaterTest(5)

		const keyHash = 0x123422 << (64 - 3*8)

		data := mustMarshalBucket(Bucket[customerUsage]{
			Items: nil,
		})
		u.stubFill(data, nil)

		usage := customerUsage{
			Tenant:     "TENANT01",
			CampaignID: 71,

			Phone:    "0987000111",
			TermCode: "TERM01",
			Hash:     keyHash,

			Usage: 12,
			Age:   22,
		}
		rootKey := usage.getRootKey()

		fn := u.updater.UpsertBuckets(newContext(), rootKey, usage)

		err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, []BucketData[customerUsageRootKey]{
			{
				Key: BucketKey[customerUsageRootKey]{
					RootKey: rootKey,
					Hash:    0x00,
					Level:   0,
				},
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items: []customerUsage{usage},
				}),
			},
		}, u.upsertBuckets)

		assert.Equal(t, []customerUsageRootKey{rootKey}, u.fillerRootKeys)
		assert.Equal(t, []uint64{0x00}, u.fillerHashList)
	})

	t.Run("update-when-existed", func(t *testing.T) {
		u := newUpdaterTest(5)

		const keyHash = 0x123422 << (64 - 3*8)

		usage := customerUsage{
			Tenant:     "TENANT01",
			CampaignID: 71,

			Phone:    "0987000111",
			TermCode: "TERM01",
			Hash:     keyHash,

			Usage: 12,
			Age:   22,
		}

		data := mustMarshalBucket(Bucket[customerUsage]{
			Items: []customerUsage{usage},
		})
		u.stubFill(data, nil)

		usage.Usage = 112
		usage.Age = 249

		rootKey := usage.getRootKey()
		fn := u.updater.UpsertBuckets(newContext(), rootKey, usage)

		err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, []BucketData[customerUsageRootKey]{
			{
				Key: BucketKey[customerUsageRootKey]{
					RootKey: rootKey,
					Hash:    0x00,
					Level:   0,
				},
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items: []customerUsage{usage},
				}),
			},
		}, u.upsertBuckets)

		assert.Equal(t, []customerUsageRootKey{rootKey}, u.fillerRootKeys)
		assert.Equal(t, []uint64{0x00}, u.fillerHashList)
	})

	t.Run("insert-exceed-limit--create-child", func(t *testing.T) {
		u := newUpdaterTest(2)

		newUsage := func(i int) customerUsage {
			return customerUsage{
				Tenant:     "TENANT",
				CampaignID: 70,

				Phone:    "098700011" + fmt.Sprint(i),
				TermCode: "TERM0" + fmt.Sprint(i),
				Hash:     uint64(0x60+i) << (64 - 8),

				Usage: int64(10 + i),
				Age:   int64(20 + i),
			}
		}

		usage1 := newUsage(1)
		usage2 := newUsage(2)
		usage3 := newUsage(3)

		u.stubFillMulti(
			mustMarshalBucket(Bucket[customerUsage]{
				Items: []customerUsage{usage1, usage2},
			}),
			mustMarshalBucket(Bucket[customerUsage]{
				Items: nil,
			}),
		)

		rootKey := usage3.getRootKey()

		fn := u.updater.UpsertBuckets(newContext(), rootKey, usage3)

		err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, []BucketData[customerUsageRootKey]{
			{
				Key: BucketKey[customerUsageRootKey]{
					RootKey: rootKey,
					Hash:    0x00,
					Level:   0,
				},
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items:  []customerUsage{usage1, usage2},
					Bitset: newBitSet(0x63),
				}),
			},
			{
				Key: BucketKey[customerUsageRootKey]{
					RootKey: rootKey,
					Hash:    0x63 << (64 - 8),
					Level:   1,
				},
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items: []customerUsage{usage3},
				}),
			},
		}, u.upsertBuckets)

		assert.Equal(t, []customerUsageRootKey{rootKey}, u.fillerRootKeys)
		assert.Equal(t, []uint64{0x00}, u.fillerHashList)
	})

	t.Run("insert-exceed-limit--create-child", func(t *testing.T) {
		u := newUpdaterTest(2)

		newUsage := func(i int) customerUsage {
			return customerUsage{
				Tenant:     "TENANT",
				CampaignID: 70,

				Phone:    "098700011" + fmt.Sprint(i),
				TermCode: "TERM0" + fmt.Sprint(i),
				Hash:     uint64(0x60+i) << (64 - 8),

				Usage: int64(10 + i),
				Age:   int64(20 + i),
			}
		}

		usage1 := newUsage(1)
		usage2 := newUsage(2)
		usage3 := newUsage(3)

		// usage 1 and 3 have the same hash
		usage1.Hash = usage3.Hash

		u.stubFillMulti(
			mustMarshalBucket(Bucket[customerUsage]{
				Items: []customerUsage{usage1, usage2},
			}),
			mustMarshalBucket(Bucket[customerUsage]{
				Items: nil,
			}),
		)

		rootKey := usage3.getRootKey()

		fn := u.updater.UpsertBuckets(newContext(), rootKey, usage3)

		err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, []BucketData[customerUsageRootKey]{
			{
				Key: BucketKey[customerUsageRootKey]{
					RootKey: rootKey,
					Hash:    0x00,
					Level:   0,
				},
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items:  []customerUsage{usage2},
					Bitset: newBitSet(0x63),
				}),
			},
			{
				Key: BucketKey[customerUsageRootKey]{
					RootKey: rootKey,
					Hash:    0x63 << (64 - 8),
					Level:   1,
				},
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items: []customerUsage{usage1, usage3},
				}),
			},
		}, u.upsertBuckets)

		assert.Equal(t, []customerUsageRootKey{rootKey}, u.fillerRootKeys)
		assert.Equal(t, []uint64{0x00}, u.fillerHashList)
	})

	t.Run("root-with-bit-set--update-child", func(t *testing.T) {
		u := newUpdaterTest(10)

		newUsage := func(i int) customerUsage {
			return customerUsage{
				Tenant:     "TENANT",
				CampaignID: 70,

				Phone:    "098700011" + fmt.Sprint(i),
				TermCode: "TERM0" + fmt.Sprint(i),
				Hash:     uint64(0x60+i) << (64 - 8),

				Usage: int64(10 + i),
				Age:   int64(20 + i),
			}
		}

		usage1 := newUsage(1)
		usage2 := newUsage(2)
		usage3 := newUsage(3)

		usage2.Hash = usage3.Hash

		u.stubFillMulti(
			mustMarshalBucket(Bucket[customerUsage]{
				Items:  []customerUsage{usage1},
				Bitset: newBitSet(0x63),
			}),
			mustMarshalBucket(Bucket[customerUsage]{
				Items: []customerUsage{usage2},
			}),
		)

		rootKey := usage3.getRootKey()

		fn := u.updater.UpsertBuckets(newContext(), rootKey, usage3)

		err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, []BucketData[customerUsageRootKey]{
			{
				Key: BucketKey[customerUsageRootKey]{
					RootKey: rootKey,
					Hash:    0x63 << (64 - 8),
					Level:   1,
				},
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items: []customerUsage{usage2, usage3},
				}),
			},
		}, u.upsertBuckets)

		assert.Equal(t, []customerUsageRootKey{rootKey, rootKey}, u.fillerRootKeys)
		assert.Equal(t, []uint64{0x00, 0x63 << (64 - 8)}, u.fillerHashList)
	})

	t.Run("limit-number-of-hashes-not-items", func(t *testing.T) {
		u := newUpdaterTest(2)

		newUsage := func(i int) customerUsage {
			return customerUsage{
				Tenant:     "TENANT",
				CampaignID: 70,

				Phone:    "098700011" + fmt.Sprint(i),
				TermCode: "TERM0" + fmt.Sprint(i),
				Hash:     uint64(0x67) << (64 - 8), // All the same hash

				Usage: int64(10 + i),
				Age:   int64(20 + i),
			}
		}

		usage1 := newUsage(1)
		usage2 := newUsage(2)
		usage3 := newUsage(3)

		usage2.Hash = usage3.Hash

		u.stubFillMulti(
			mustMarshalBucket(Bucket[customerUsage]{
				Items: []customerUsage{usage1, usage2},
			}),
		)

		rootKey := usage3.getRootKey()

		fn := u.updater.UpsertBuckets(newContext(), rootKey, usage3)

		err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, []BucketData[customerUsageRootKey]{
			{
				Key: BucketKey[customerUsageRootKey]{
					RootKey: rootKey,
					Hash:    0x00,
					Level:   0,
				},
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items: []customerUsage{usage1, usage2, usage3},
				}),
			},
		}, u.upsertBuckets)

		assert.Equal(t, []customerUsageRootKey{rootKey}, u.fillerRootKeys)
		assert.Equal(t, []uint64{0x00}, u.fillerHashList)
	})
}

func TestUpdaterConcurrent(t *testing.T) {
	t.Run("append-three-exceeded", func(t *testing.T) {
		u := newUpdaterTest(2)

		newUsage := func(i int) customerUsage {
			return customerUsage{
				Tenant:     "TENANT",
				CampaignID: 70,

				Phone:    "098700011" + fmt.Sprint(i),
				TermCode: "TERM0" + fmt.Sprint(i),
				Hash:     uint64(0x60+i) << (64 - 8),

				Usage: int64(10 + i),
				Age:   int64(20 + i),
			}
		}

		usage1 := newUsage(1)
		usage2 := newUsage(2)
		usage3 := newUsage(3)

		u.stubFillMulti(
			mustMarshalBucket(Bucket[customerUsage]{
				Items: nil,
			}),
		)

		rootKey := usage1.getRootKey()

		fn1 := u.updater.UpsertBuckets(newContext(), rootKey, usage1)
		fn2 := u.updater.UpsertBuckets(newContext(), rootKey, usage2)
		fn3 := u.updater.UpsertBuckets(newContext(), rootKey, usage3)

		var err error

		err = fn1()
		assert.Equal(t, nil, err)

		err = fn2()
		assert.Equal(t, nil, err)

		err = fn3()
		assert.Equal(t, nil, err)

		assert.Equal(t, []BucketData[customerUsageRootKey]{
			{
				Key: BucketKey[customerUsageRootKey]{
					RootKey: rootKey,
					Hash:    0x00,
					Level:   0,
				},
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items:  []customerUsage{usage1, usage2},
					Bitset: newBitSet(0x63),
				}),
			},
			{
				Key: BucketKey[customerUsageRootKey]{
					RootKey: rootKey,
					Hash:    0x63 << (64 - 8),
					Level:   1,
				},
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items: []customerUsage{usage3},
				}),
			},
		}, u.upsertBuckets)

		// Check Filler Calls
		assert.Equal(t, []customerUsageRootKey{rootKey}, u.fillerRootKeys)
		assert.Equal(t, []uint64{0x00}, u.fillerHashList)
	})
}
