package mhash

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/mocks"
)

type propertyTest struct {
	bucketDataMap map[BucketKey[customerUsageRootKey]][]byte

	hash    *Hash[customerUsage, customerUsageRootKey, customerUsageKey]
	updater *HashUpdater[customerUsage, customerUsageRootKey, customerUsageKey]

	callOrders []string

	scannedBuckets map[BucketKey[customerUsageRootKey]]emptyStruct
}

func (p *propertyTest) addCall(op string, key string) {
	p.callOrders = append(p.callOrders, op+"::"+key)
}

func (p *propertyTest) clearCalls() {
	p.callOrders = []string{}
}

func (p *propertyTest) getBucketDataList() []BucketData[customerUsageRootKey] {
	var bucketDataList []BucketData[customerUsageRootKey]
	for key, data := range p.bucketDataMap {
		bucketDataList = append(bucketDataList, BucketData[customerUsageRootKey]{
			Key:  key,
			Data: data,
		})
	}

	sort.Slice(bucketDataList, func(i, j int) bool {
		return bucketDataList[i].Key.String() < bucketDataList[j].Key.String()
	})
	return bucketDataList
}

func newPropertyTest(maxHashesPerBucket int) *propertyTest {
	sess := newFakeSession()
	pipe := &mocks.PipelineMock{}

	p := &propertyTest{
		callOrders: []string{},

		scannedBuckets: map[BucketKey[customerUsageRootKey]]struct{}{},
	}

	cas := uint64(5562000)
	pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) memproxy.LeaseGetResult {
		p.addCall("lease-get", key)
		return memproxy.LeaseGetResultFunc(func() (memproxy.LeaseGetResponse, error) {
			p.addCall("lease-get-func", key)

			cas++
			return memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseGranted,
				CAS:    cas,
			}, nil
		})
	}

	pipe.LeaseSetFunc = func(
		key string, data []byte, cas uint64, options memproxy.LeaseSetOptions,
	) func() (memproxy.LeaseSetResponse, error) {
		p.addCall("lease-set", key)
		return func() (memproxy.LeaseSetResponse, error) {
			p.addCall("lease-set-func", key)
			return memproxy.LeaseSetResponse{}, nil
		}
	}

	pipe.LowerSessionFunc = func() memproxy.Session {
		return sess
	}

	pipe.ExecuteFunc = func() {}

	bucketDataMap := map[BucketKey[customerUsageRootKey]][]byte{}

	var filler Filler[customerUsageRootKey] = func(
		ctx context.Context, key BucketKey[customerUsageRootKey],
	) func() ([]byte, error) {
		p.addCall("fill-get", key.String())
		return func() ([]byte, error) {
			p.addCall("fill-get-func", key.String())

			data := bucketDataMap[key]

			if len(data) > 0 {
				p.scannedBuckets[key] = struct{}{}
			}

			return data, nil
		}
	}

	h := New[customerUsage, customerUsageRootKey, customerUsageKey](
		pipe,
		customerUsage.getKey,
		unmarshalCustomerUsage, filler,
	)

	upsertFunc := func(bucket BucketData[customerUsageRootKey]) {
		bucketDataMap[bucket.Key] = bucket.Data
	}

	deleteFunc := func(key BucketKey[customerUsageRootKey]) {
		delete(bucketDataMap, key)
	}

	updater := NewUpdater[customerUsage, customerUsageRootKey, customerUsageKey](
		sess,
		customerUsage.getKey,
		unmarshalCustomerUsage,
		filler,
		upsertFunc,
		deleteFunc,
		maxHashesPerBucket,
	)

	p.bucketDataMap = bucketDataMap
	p.hash = h
	p.updater = updater

	return p
}

func mustUnmarshalBucket(data []byte) Bucket[customerUsage] {
	bucket, err := BucketUnmarshalerFromItem(unmarshalCustomerUsage)(data)
	if err != nil {
		panic(err)
	}
	return bucket
}

func TestHash_PropertyBased__Upsert_And_Get(t *testing.T) {
	seed := time.Now().Unix()
	fmt.Println("SEED:", seed)
	rand.Seed(seed)

	p := newPropertyTest(7)

	rootKeys := []customerUsageRootKey{
		{
			Tenant:     "TENANT01",
			CampaignID: 141,
		},
		{
			Tenant:     "TENANT02",
			CampaignID: 142,
		},
		{
			Tenant:     "TENANT03",
			CampaignID: 143,
		},
	}

	var calls []func() error

	type combinedKey struct {
		rootKey customerUsageRootKey
		key     customerUsageKey
	}
	usageMap := map[combinedKey]customerUsage{}

	const termCode = "TERM01"

	const numKeys = 5000

	for k := 0; k < numKeys; k++ {
		rootKey := rootKeys[rand.Intn(len(rootKeys))]
		phone := fmt.Sprintf("0987%06d", rand.Intn(numKeys))
		usage := customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   termCode,
			Hash:       murmur3.Sum64([]byte(phone)),
		}

		usageMap[combinedKey{
			rootKey: usage.getRootKey(),
			key:     usage.getKey(),
		}] = usage

		fn := p.updater.UpsertBucket(newContext(), rootKey, usage)
		calls = append(calls, fn)
	}

	for _, call := range calls {
		err := call()
		if err != nil {
			panic(err)
		}
	}

	var resultCalls []func() (Null[customerUsage], error)
	var combinedKeys []combinedKey

	for k := 0; k < numKeys; k++ {
		rootKey := rootKeys[rand.Intn(len(rootKeys))]
		phone := fmt.Sprintf("0987%06d", rand.Intn(numKeys))
		key := customerUsageKey{
			Phone:    phone,
			TermCode: termCode,
			hash:     murmur3.Sum64([]byte(phone)),
		}

		fn := p.hash.Get(newContext(), rootKey, key)
		resultCalls = append(resultCalls, fn)

		combinedKeys = append(combinedKeys, combinedKey{
			rootKey: rootKey,
			key:     key,
		})
	}

	for i := range resultCalls {
		call := resultCalls[i]
		k := combinedKeys[i]

		result, err := call()
		assert.Equal(t, nil, err)

		expected, ok := usageMap[k]

		assert.Equal(t, ok, result.Valid)
		assert.Equal(t, expected, result.Data)
	}
}

func TestHash_PropertyBased__Simple_Upsert_Delete_Get(t *testing.T) {
	p := newPropertyTest(7)

	rootKey := customerUsageRootKey{
		Tenant:     "TENANT01",
		CampaignID: 141,
	}

	const phone = "0987000111"
	usage := customerUsage{
		Tenant:     rootKey.Tenant,
		CampaignID: rootKey.CampaignID,
		Phone:      phone,
		TermCode:   "TERM01",
		Hash:       murmur3.Sum64([]byte(phone)),
	}

	fn := p.hash.Get(newContext(), rootKey, usage.getKey())
	nullUsage, err := fn()

	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{}, nullUsage)

	upsertFn := p.updater.UpsertBucket(newContext(), rootKey, usage)
	assert.Equal(t, nil, upsertFn())

	// ========================
	// Reset and Get Again
	// ========================
	p.hash.Reset()
	fn = p.hash.Get(newContext(), rootKey, usage.getKey())

	nullUsage, err = fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage,
	}, nullUsage)

	// Delete
	deleteFn := p.updater.DeleteBucket(newContext(), rootKey, usage.getKey())
	assert.Equal(t, nil, deleteFn())

	// ========================
	// Get After Delete
	// ========================
	p.hash.Reset()
	fn = p.hash.Get(newContext(), rootKey, usage.getKey())

	nullUsage, err = fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{}, nullUsage)

	assert.Equal(t, map[BucketKey[customerUsageRootKey]][]byte{}, p.bucketDataMap)
}

func TestHash_PropertyBased__Simple_Upsert_Delete_Multiple_Keys__Same_Root_Key(t *testing.T) {
	p := newPropertyTest(7)

	rootKey := customerUsageRootKey{
		Tenant:     "TENANT01",
		CampaignID: 141,
	}

	newUsage := func(i int) customerUsage {
		phone := fmt.Sprintf("098700011%d", i)
		return customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   fmt.Sprintf("TERM0%d", i),
			Hash:       murmur3.Sum64([]byte(phone)),
		}
	}

	usage1 := newUsage(1)
	usage2 := newUsage(2)
	usage3 := newUsage(3)

	assert.Equal(t, customerUsage{
		Tenant:     rootKey.Tenant,
		CampaignID: rootKey.CampaignID,
		Phone:      "0987000111",
		TermCode:   "TERM01",
		Hash:       murmur3.Sum64([]byte("0987000111")),
	}, usage1)

	fn := p.hash.Get(newContext(), rootKey, usage1.getKey())
	nullUsage, err := fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{}, nullUsage)

	fn = p.hash.Get(newContext(), rootKey, usage2.getKey())
	nullUsage, err = fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{}, nullUsage)

	upsertFn := p.updater.UpsertBucket(newContext(), rootKey, usage1)
	assert.Equal(t, nil, upsertFn())

	upsertFn = p.updater.UpsertBucket(newContext(), rootKey, usage2)
	assert.Equal(t, nil, upsertFn())

	// ========================
	// Get Again
	// ========================
	p.hash.Reset()
	fn = p.hash.Get(newContext(), rootKey, usage1.getKey())

	nullUsage, err = fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage1,
	}, nullUsage)

	fn = p.hash.Get(newContext(), rootKey, usage2.getKey())
	nullUsage, err = fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage2,
	}, nullUsage)

	fn = p.hash.Get(newContext(), rootKey, usage3.getKey())
	nullUsage, err = fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{}, nullUsage)

	// Delete
	deleteFn := p.updater.DeleteBucket(newContext(), rootKey, usage1.getKey())
	assert.Equal(t, nil, deleteFn())

	// ==============================
	// Get After Delete
	// ==============================
	p.hash.Reset()
	fn = p.hash.Get(newContext(), rootKey, usage1.getKey())

	nullUsage, err = fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{}, nullUsage)

	data := mustMarshalBucket(0, 0, Bucket[customerUsage]{
		Items: []customerUsage{usage2},
	})
	assert.Equal(t, map[BucketKey[customerUsageRootKey]][]byte{
		BucketKey[customerUsageRootKey]{
			RootKey: rootKey,
			Level:   0,
		}: data,
	}, p.bucketDataMap)
}

func TestHash_PropertyBased__Multi_Upsert__And_Multi_Delete__In_Two_Step(t *testing.T) {
	p := newPropertyTest(2)

	rootKey := customerUsageRootKey{
		Tenant:     "TENANT01",
		CampaignID: 141,
	}

	newUsage := func(i int, hash uint64) customerUsage {
		phone := fmt.Sprintf("098700011%d", i)
		return customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   fmt.Sprintf("TERM0%d", i),
			Hash:       hash,
		}
	}

	usage1 := newUsage(1, 0x21)
	usage2 := newUsage(2, 0x22)

	assert.Equal(t, []string{}, p.callOrders)

	fn1 := p.updater.UpsertBucket(newContext(), rootKey, usage1)
	fn2 := p.updater.UpsertBucket(newContext(), rootKey, usage2)

	assert.Equal(t, nil, fn1())
	assert.Equal(t, nil, fn2())

	assert.Equal(t, []string{
		"fill-get::TENANT01:141:",
		"fill-get-func::TENANT01:141:",
	}, p.callOrders)

	data := mustMarshalBucket(0, 0, Bucket[customerUsage]{
		Items: []customerUsage{
			usage1,
			usage2,
		},
	})
	assert.Equal(t, map[BucketKey[customerUsageRootKey]][]byte{
		BucketKey[customerUsageRootKey]{
			RootKey: rootKey,
		}: data,
	}, p.bucketDataMap)

	// ==================================
	// Do Delete
	// ==================================
	p.clearCalls()

	fn1 = p.updater.DeleteBucket(newContext(), rootKey, usage1.getKey())
	fn2 = p.updater.DeleteBucket(newContext(), rootKey, usage2.getKey())

	assert.Equal(t, nil, fn1())
	assert.Equal(t, nil, fn2())

	// Because of Cached
	assert.Equal(t, []string{}, p.callOrders)
	assert.Equal(t, map[BucketKey[customerUsageRootKey]][]byte{}, p.bucketDataMap)
}

func TestHash_PropertyBased__Multi_Upsert__Exceed_Limit(t *testing.T) {
	p := newPropertyTest(2)

	rootKey := customerUsageRootKey{
		Tenant:     "TENANT01",
		CampaignID: 141,
	}

	newUsage := func(i int, hash uint64) customerUsage {
		phone := fmt.Sprintf("098700011%d", i)
		return customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   fmt.Sprintf("TERM0%d", i),
			Hash:       hash,
		}
	}

	usage1 := newUsage(1, 0x882101)
	usage2 := newUsage(2, 0x882201)
	usage3 := newUsage(3, 0x882202)

	assert.Equal(t, []string{}, p.callOrders)

	fn1 := p.updater.UpsertBucket(newContext(), rootKey, usage1)
	fn2 := p.updater.UpsertBucket(newContext(), rootKey, usage2)
	fn3 := p.updater.UpsertBucket(newContext(), rootKey, usage3)

	assert.Equal(t, nil, fn1())
	assert.Equal(t, nil, fn2())
	assert.Equal(t, nil, fn3())

	assert.Equal(t, []string{
		"fill-get::TENANT01:141:",
		"fill-get-func::TENANT01:141:",
	}, p.callOrders)

	dataList := p.getBucketDataList()
	assert.Equal(t, 2, len(dataList))

	assert.Equal(t, "TENANT01:141:", dataList[0].Key.String())
	assert.Equal(t, "TENANT01:141:00000000008822", dataList[1].Key.String())

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       7,
		NextLevelPrefix: 0x880000,
		Items: []customerUsage{
			usage1,
		},
		Bitset: newBitSet(0x22),
	}, mustUnmarshalBucket(dataList[0].Data))

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       0,
		NextLevelPrefix: 0,
		Items: []customerUsage{
			usage2, usage3,
		},
	}, mustUnmarshalBucket(dataList[1].Data))
}

func TestHash_PropertyBased__Multi_Upsert__Exceed_Next_Level_Prefix(t *testing.T) {
	p := newPropertyTest(2)

	rootKey := customerUsageRootKey{
		Tenant:     "TENANT01",
		CampaignID: 141,
	}

	newUsage := func(i int, hash uint64) customerUsage {
		phone := fmt.Sprintf("098700011%d", i)
		return customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   fmt.Sprintf("TERM0%d", i),
			Hash:       hash,
		}
	}

	usage1 := newUsage(1, 0x88772101<<(64-4*8))
	usage2 := newUsage(2, 0x88772201<<(64-4*8))
	usage3 := newUsage(3, 0x88772202<<(64-4*8))
	usage4 := newUsage(4, 0x886622<<(64-3*8))
	usage5 := newUsage(5, 0x886623<<(64-3*8))

	assert.Equal(t, []string{}, p.callOrders)

	fn1 := p.updater.UpsertBucket(newContext(), rootKey, usage1)
	fn2 := p.updater.UpsertBucket(newContext(), rootKey, usage2)
	fn3 := p.updater.UpsertBucket(newContext(), rootKey, usage3)
	fn4 := p.updater.UpsertBucket(newContext(), rootKey, usage4)
	fn5 := p.updater.UpsertBucket(newContext(), rootKey, usage5)

	assert.Equal(t, nil, fn1())
	assert.Equal(t, nil, fn2())
	assert.Equal(t, nil, fn3())
	assert.Equal(t, nil, fn4())
	assert.Equal(t, nil, fn5())

	assert.Equal(t, []string{
		"fill-get::TENANT01:141:",
		"fill-get-func::TENANT01:141:",
	}, p.callOrders)

	bucketDataList := p.getBucketDataList()

	assert.Equal(t, 4, len(bucketDataList))
	assert.Equal(t, "TENANT01:141:", bucketDataList[0].Key.String())
	assert.Equal(t, "TENANT01:141:8866", bucketDataList[1].Key.String())
	assert.Equal(t, "TENANT01:141:8877", bucketDataList[2].Key.String())
	assert.Equal(t, "TENANT01:141:887722", bucketDataList[3].Key.String())

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       2,
		NextLevelPrefix: 0x88 << (64 - 8),
		Items:           []customerUsage{},
		Bitset:          newBitSet(0x66, 0x77),
	}, mustUnmarshalBucket(bucketDataList[0].Data))

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       0,
		NextLevelPrefix: 0,
		Items: []customerUsage{
			usage4, usage5,
		},
	}, mustUnmarshalBucket(bucketDataList[1].Data))

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       3,
		NextLevelPrefix: 0x8877 << (64 - 2*8),
		Items: []customerUsage{
			usage1,
		},
		Bitset: newBitSet(0x22),
	}, mustUnmarshalBucket(bucketDataList[2].Data))

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       0,
		NextLevelPrefix: 0,
		Items: []customerUsage{
			usage2, usage3,
		},
	}, mustUnmarshalBucket(bucketDataList[3].Data))

	// Get Data
	nullUsage, err := p.hash.Get(newContext(), rootKey, usage1.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage1,
	}, nullUsage)

	nullUsage, err = p.hash.Get(newContext(), rootKey, usage2.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage2,
	}, nullUsage)

	nullUsage, err = p.hash.Get(newContext(), rootKey, usage3.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage3,
	}, nullUsage)

	nullUsage, err = p.hash.Get(newContext(), rootKey, usage5.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage5,
	}, nullUsage)
}

func TestHash_PropertyBased__Multi_Upsert__Exceed_Next_Level_Prefix__In_Middle(t *testing.T) {
	p := newPropertyTest(2)

	rootKey := customerUsageRootKey{
		Tenant:     "TENANT01",
		CampaignID: 141,
	}

	newUsage := func(i int, hash uint64) customerUsage {
		phone := fmt.Sprintf("098700011%d", i)
		return customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   fmt.Sprintf("TERM0%d", i),
			Hash:       hash,
		}
	}

	usage1 := newUsage(1, 0x87<<(64-1*8))
	usage2 := newUsage(2, 0x886621<<(64-3*8))
	usage3 := newUsage(3, 0x8701<<(64-2*8))

	usage8 := newUsage(8, 0x85<<(64-1*8))

	usage4 := newUsage(4, 0x88662241<<(64-4*8))
	usage5 := newUsage(5, 0x88662242<<(64-4*8))

	usage6 := newUsage(6, 0x886731<<(64-3*8))
	usage7 := newUsage(7, 0x886732<<(64-3*8))

	assert.Equal(t, []string{}, p.callOrders)

	fn1 := p.updater.UpsertBucket(newContext(), rootKey, usage1)
	fn2 := p.updater.UpsertBucket(newContext(), rootKey, usage2)
	fn3 := p.updater.UpsertBucket(newContext(), rootKey, usage3)

	fn8 := p.updater.UpsertBucket(newContext(), rootKey, usage8)

	fn4 := p.updater.UpsertBucket(newContext(), rootKey, usage4)
	fn5 := p.updater.UpsertBucket(newContext(), rootKey, usage5)
	fn6 := p.updater.UpsertBucket(newContext(), rootKey, usage6)
	fn7 := p.updater.UpsertBucket(newContext(), rootKey, usage7)

	assert.Equal(t, nil, fn1())
	assert.Equal(t, nil, fn2())
	assert.Equal(t, nil, fn3())

	assert.Equal(t, nil, fn8())

	assert.Equal(t, nil, fn4())
	assert.Equal(t, nil, fn5())
	assert.Equal(t, nil, fn6())
	assert.Equal(t, nil, fn7())

	assert.Equal(t, []string{
		"fill-get::TENANT01:141:",
		"fill-get-func::TENANT01:141:",
	}, p.callOrders)

	bucketDataList := p.getBucketDataList()

	assert.Equal(t, 6, len(bucketDataList))
	assert.Equal(t, "TENANT01:141:", bucketDataList[0].Key.String())
	assert.Equal(t, "TENANT01:141:87", bucketDataList[1].Key.String())
	assert.Equal(t, "TENANT01:141:88", bucketDataList[2].Key.String())
	assert.Equal(t, "TENANT01:141:8866", bucketDataList[3].Key.String())
	assert.Equal(t, "TENANT01:141:886622", bucketDataList[4].Key.String())
	assert.Equal(t, "TENANT01:141:8867", bucketDataList[5].Key.String())

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       1,
		NextLevelPrefix: 0,
		Items:           []customerUsage{usage8},
		Bitset:          newBitSet(0x87, 0x88),
	}, mustUnmarshalBucket(bucketDataList[0].Data))

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       0,
		NextLevelPrefix: 0,
		Items: []customerUsage{
			usage1, usage3,
		},
	}, mustUnmarshalBucket(bucketDataList[1].Data))

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       2,
		NextLevelPrefix: 0x88 << (64 - 1*8),
		Items:           []customerUsage{},
		Bitset:          newBitSet(0x66, 0x67),
	}, mustUnmarshalBucket(bucketDataList[2].Data))

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       3,
		NextLevelPrefix: 0x8866 << (64 - 2*8),
		Items: []customerUsage{
			usage2,
		},
		Bitset: newBitSet(0x22),
	}, mustUnmarshalBucket(bucketDataList[3].Data))

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       0,
		NextLevelPrefix: 0,
		Items: []customerUsage{
			usage4, usage5,
		},
	}, mustUnmarshalBucket(bucketDataList[4].Data))

	assert.Equal(t, Bucket[customerUsage]{
		NextLevel:       0,
		NextLevelPrefix: 0,
		Items: []customerUsage{
			usage6, usage7,
		},
	}, mustUnmarshalBucket(bucketDataList[5].Data))

	// Get Data
	nullUsage, err := p.hash.Get(newContext(), rootKey, usage1.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage1,
	}, nullUsage)

	nullUsage, err = p.hash.Get(newContext(), rootKey, usage2.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage2,
	}, nullUsage)

	nullUsage, err = p.hash.Get(newContext(), rootKey, usage3.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage3,
	}, nullUsage)

	nullUsage, err = p.hash.Get(newContext(), rootKey, usage4.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage4,
	}, nullUsage)

	nullUsage, err = p.hash.Get(newContext(), rootKey, usage5.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage5,
	}, nullUsage)

	nullUsage, err = p.hash.Get(newContext(), rootKey, usage6.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage6,
	}, nullUsage)

	nullUsage, err = p.hash.Get(newContext(), rootKey, usage7.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage7,
	}, nullUsage)

	nullUsage, err = p.hash.Get(newContext(), rootKey, usage8.getKey())()
	assert.Equal(t, nil, err)
	assert.Equal(t, Null[customerUsage]{
		Valid: true,
		Data:  usage8,
	}, nullUsage)
}

func TestHash_PropertyBased__Multi_Upsert_And_Delete_Single__Exceed_Limit(t *testing.T) {
	p := newPropertyTest(2)

	rootKey := customerUsageRootKey{
		Tenant:     "TENANT01",
		CampaignID: 141,
	}

	newUsage := func(i int, hash uint64) customerUsage {
		phone := fmt.Sprintf("098700011%d", i)
		return customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   fmt.Sprintf("TERM0%d", i),
			Hash:       hash,
		}
	}

	usage1 := newUsage(1, 0x882100)
	usage2 := newUsage(2, 0x882201)
	usage3 := newUsage(3, 0x882202)

	assert.Equal(t, []string{}, p.callOrders)

	fn1 := p.updater.UpsertBucket(newContext(), rootKey, usage1)
	fn2 := p.updater.UpsertBucket(newContext(), rootKey, usage2)
	fn3 := p.updater.UpsertBucket(newContext(), rootKey, usage3)
	fn4 := p.updater.DeleteBucket(newContext(), rootKey, usage2.getKey())

	assert.Equal(t, nil, fn1())
	assert.Equal(t, nil, fn2())
	assert.Equal(t, nil, fn3())
	assert.Equal(t, nil, fn4())

	assert.Equal(t, []string{
		"fill-get::TENANT01:141:",
		"fill-get-func::TENANT01:141:",
	}, p.callOrders)

	data1 := mustMarshalBucket(0, 0, Bucket[customerUsage]{
		NextLevel:       7,
		NextLevelPrefix: 0x880000,
		Items: []customerUsage{
			usage1,
		},
		Bitset: newBitSet(0x22),
	})
	data2 := mustMarshalBucket(0, 0, Bucket[customerUsage]{
		Items: []customerUsage{
			usage3,
		},
	})
	assert.Equal(t, map[BucketKey[customerUsageRootKey]][]byte{
		BucketKey[customerUsageRootKey]{
			RootKey: rootKey,
			Level:   0,
		}: data1,
		BucketKey[customerUsageRootKey]{
			RootKey: rootKey,
			Level:   7,
			Hash:    0x882200,
		}: data2,
	}, p.bucketDataMap)
}

func TestHash_PropertyBased__Multi_Upsert_And_Delete_Two__Exceed_Limit(t *testing.T) {
	p := newPropertyTest(2)

	rootKey := customerUsageRootKey{
		Tenant:     "TENANT01",
		CampaignID: 141,
	}

	newUsage := func(i int, hash uint64) customerUsage {
		phone := fmt.Sprintf("098700011%d", i)
		return customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   fmt.Sprintf("TERM0%d", i),
			Hash:       hash,
		}
	}

	usage1 := newUsage(1, 0x882101)
	usage2 := newUsage(2, 0x882201)
	usage3 := newUsage(3, 0x882202)

	assert.Equal(t, []string{}, p.callOrders)

	fn1 := p.updater.UpsertBucket(newContext(), rootKey, usage1)
	fn2 := p.updater.UpsertBucket(newContext(), rootKey, usage2)
	fn3 := p.updater.UpsertBucket(newContext(), rootKey, usage3)
	fn4 := p.updater.DeleteBucket(newContext(), rootKey, usage2.getKey())
	fn5 := p.updater.DeleteBucket(newContext(), rootKey, usage3.getKey())

	assert.Equal(t, nil, fn1())
	assert.Equal(t, nil, fn2())
	assert.Equal(t, nil, fn3())
	assert.Equal(t, nil, fn4())
	assert.Equal(t, nil, fn5())

	assert.Equal(t, []string{
		"fill-get::TENANT01:141:",
		"fill-get-func::TENANT01:141:",
	}, p.callOrders)

	data1 := mustMarshalBucket(0, 0, Bucket[customerUsage]{
		Items: []customerUsage{
			usage1,
		},
	})
	assert.Equal(t, map[BucketKey[customerUsageRootKey]][]byte{
		BucketKey[customerUsageRootKey]{
			RootKey: rootKey,
			Level:   0,
		}: data1,
	}, p.bucketDataMap)
}

func TestHash_PropertyBased__Multi_Upsert_And_Delete_All__Exceed_Limit(t *testing.T) {
	p := newPropertyTest(2)

	rootKey := customerUsageRootKey{
		Tenant:     "TENANT01",
		CampaignID: 141,
	}

	newUsage := func(i int, hash uint64) customerUsage {
		phone := fmt.Sprintf("098700011%d", i)
		return customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   fmt.Sprintf("TERM0%d", i),
			Hash:       hash,
		}
	}

	usage1 := newUsage(1, 0x882101)
	usage2 := newUsage(2, 0x882201)
	usage3 := newUsage(3, 0x882202)

	assert.Equal(t, []string{}, p.callOrders)

	fn1 := p.updater.UpsertBucket(newContext(), rootKey, usage1)
	fn2 := p.updater.UpsertBucket(newContext(), rootKey, usage2)
	fn3 := p.updater.UpsertBucket(newContext(), rootKey, usage3)

	fn4 := p.updater.DeleteBucket(newContext(), rootKey, usage2.getKey())
	fn5 := p.updater.DeleteBucket(newContext(), rootKey, usage3.getKey())
	fn6 := p.updater.DeleteBucket(newContext(), rootKey, usage1.getKey())

	assert.Equal(t, nil, fn1())
	assert.Equal(t, nil, fn2())
	assert.Equal(t, nil, fn3())
	assert.Equal(t, nil, fn4())
	assert.Equal(t, nil, fn5())
	assert.Equal(t, nil, fn6())

	assert.Equal(t, []string{
		"fill-get::TENANT01:141:",
		"fill-get-func::TENANT01:141:",
	}, p.callOrders)

	assert.Equal(t, map[BucketKey[customerUsageRootKey]][]byte{}, p.bucketDataMap)
}

func TestHash_PropertyBased__Upsert_And_Get__Without_Using_Hash_Func__2(t *testing.T) {
	seed := time.Now().Unix()
	seed = 1673937882
	fmt.Println("SEED:", seed)
	rand.Seed(seed)

	p := newPropertyTest(2)

	rootKeys := []customerUsageRootKey{
		{
			Tenant:     "TENANT01",
			CampaignID: 141,
		},
		{
			Tenant:     "TENANT02",
			CampaignID: 142,
		},
		{
			Tenant:     "TENANT03",
			CampaignID: 143,
		},
	}

	var calls []func() error

	type combinedKey struct {
		rootKey customerUsageRootKey
		key     customerUsageKey
	}
	usageMap := map[combinedKey]customerUsage{}

	const termCode = "TERM01"

	const numKeys = 5000

	var combinedKeys []combinedKey

	for k := 0; k < numKeys; k++ {
		rootKey := rootKeys[rand.Intn(len(rootKeys))]
		phone := fmt.Sprintf("0987%06d", rand.Intn(numKeys))
		hashNum := uint64(rand.Intn(numKeys*2) * 183)
		usage := customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   termCode,
			Hash:       hashNum,
		}

		combinedKeys = append(combinedKeys, combinedKey{
			rootKey: rootKey,
			key:     usage.getKey(),
		})

		usageMap[combinedKey{
			rootKey: usage.getRootKey(),
			key:     usage.getKey(),
		}] = usage

		fn := p.updater.UpsertBucket(newContext(), rootKey, usage)
		calls = append(calls, fn)
	}

	for _, call := range calls {
		err := call()
		if err != nil {
			panic(err)
		}
	}

	var resultCalls []func() (Null[customerUsage], error)

	for _, rootKey := range combinedKeys {
		fn := p.hash.Get(newContext(), rootKey.rootKey, rootKey.key)
		resultCalls = append(resultCalls, fn)
	}

	for i := range resultCalls {
		call := resultCalls[i]
		key := combinedKeys[i]

		result, err := call()
		assert.Equal(t, nil, err)

		expected, ok := usageMap[key]

		assert.Equal(t, ok, result.Valid)
		assert.Equal(t, expected, result.Data)
	}

	fmt.Println("SEED:", seed)
}

func TestHash_PropertyBased__Upsert_And_Get__Without_Using_Hash_Func__Disable_Seed(t *testing.T) {
	seed := time.Now().Unix()
	seed = 1673936625
	fmt.Println("SEED:", seed)
	rand.Seed(seed)

	p := newPropertyTest(2)

	rootKeys := []customerUsageRootKey{
		{
			Tenant:     "TENANT01",
			CampaignID: 141,
		},
		{
			Tenant:     "TENANT02",
			CampaignID: 142,
		},
		{
			Tenant:     "TENANT03",
			CampaignID: 143,
		},
	}

	var calls []func() error

	type combinedKey struct {
		rootKey customerUsageRootKey
		key     customerUsageKey
	}
	usageMap := map[combinedKey]customerUsage{}

	const termCode = "TERM01"

	const numKeys = 5000

	var combinedKeys []combinedKey

	for k := 0; k < numKeys; k++ {
		rootKey := rootKeys[rand.Intn(len(rootKeys))]
		phone := fmt.Sprintf("0987%06d", rand.Intn(numKeys))
		hashNum := uint64(rand.Intn(numKeys / 2))
		usage := customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   termCode,
			Hash:       hashNum,
		}

		combinedKeys = append(combinedKeys, combinedKey{
			rootKey: rootKey,
			key:     usage.getKey(),
		})

		usageMap[combinedKey{
			rootKey: usage.getRootKey(),
			key:     usage.getKey(),
		}] = usage

		fn := p.updater.UpsertBucket(newContext(), rootKey, usage)
		calls = append(calls, fn)
	}

	for _, call := range calls {
		err := call()
		if err != nil {
			panic(err)
		}
	}

	var resultCalls []func() (Null[customerUsage], error)

	for _, rootKey := range combinedKeys {
		fn := p.hash.Get(newContext(), rootKey.rootKey, rootKey.key)
		resultCalls = append(resultCalls, fn)
	}

	for i := range resultCalls {
		call := resultCalls[i]
		key := combinedKeys[i]

		result, err := call()
		assert.Equal(t, nil, err)

		expected, ok := usageMap[key]

		assert.Equal(t, ok, result.Valid)
		assert.Equal(t, expected, result.Data)
	}

	fmt.Println("SEED:", seed)
}

func TestHash_PropertyBased__Upsert_And_Get__Without_Using_Hash_Func(t *testing.T) {
	seed := time.Now().UnixNano()
	fmt.Println("SEED:", seed)
	rand.Seed(seed)

	p := newPropertyTest(2)

	rootKeys := []customerUsageRootKey{
		{
			Tenant:     "TENANT01",
			CampaignID: 141,
		},
		{
			Tenant:     "TENANT02",
			CampaignID: 142,
		},
		{
			Tenant:     "TENANT03",
			CampaignID: 143,
		},
	}

	var calls []func() error

	type combinedKey struct {
		rootKey customerUsageRootKey
		key     customerUsageKey
	}
	usageMap := map[combinedKey]customerUsage{}

	const termCode = "TERM01"

	const numKeys = 5000

	var combinedKeys []combinedKey

	for k := 0; k < numKeys; k++ {
		rootKey := rootKeys[rand.Intn(len(rootKeys))]
		phone := fmt.Sprintf("0987%06d", rand.Intn(numKeys))
		hashNum := uint64(rand.Intn(numKeys / 2))
		usage := customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   termCode,
			Hash:       hashNum,
		}

		combinedKeys = append(combinedKeys, combinedKey{
			rootKey: rootKey,
			key:     usage.getKey(),
		})

		usageMap[combinedKey{
			rootKey: usage.getRootKey(),
			key:     usage.getKey(),
		}] = usage

		fn := p.updater.UpsertBucket(newContext(), rootKey, usage)
		calls = append(calls, fn)
	}

	for _, call := range calls {
		err := call()
		if err != nil {
			panic(err)
		}
	}

	var resultCalls []func() (Null[customerUsage], error)

	for _, rootKey := range combinedKeys {
		fn := p.hash.Get(newContext(), rootKey.rootKey, rootKey.key)
		resultCalls = append(resultCalls, fn)
	}

	for i := range resultCalls {
		call := resultCalls[i]
		key := combinedKeys[i]

		result, err := call()
		assert.Equal(t, nil, err)

		expected, ok := usageMap[key]

		assert.Equal(t, ok, result.Valid)
		assert.Equal(t, expected, result.Data)
	}

	fmt.Println("SEED:", seed)
}

func TestHash_PropertyBased__Upsert_And_Get__Without_Using_Hash_Func__Multi_Times(t *testing.T) {
	for i := 0; i < 5; i++ {
		TestHash_PropertyBased__Upsert_And_Get__Without_Using_Hash_Func(t)
	}
}

func TestHash_PropertyBased__Upsert_Delete_And_Get__Without_Use_Hash_Func(t *testing.T) {
	seed := time.Now().UnixNano()
	fmt.Println("SEED:", seed)
	rand.Seed(seed)

	maxPerBucket := 2 + rand.Intn(3)
	fmt.Println("MAX HASHES PER BUCKETS:", maxPerBucket)
	p := newPropertyTest(maxPerBucket)

	rootKeys := []customerUsageRootKey{
		{
			Tenant:     "TENANT01",
			CampaignID: 141,
		},
		{
			Tenant:     "TENANT02",
			CampaignID: 142,
		},
		{
			Tenant:     "TENANT03",
			CampaignID: 143,
		},
	}

	var calls []func() error

	type combinedKey struct {
		rootKey customerUsageRootKey
		key     customerUsageKey
	}
	usageMap := map[combinedKey]customerUsage{}

	const termCode = "TERM01"

	const numKeys = 5000

	var combinedKeys []combinedKey

	randRange := 137 + rand.Intn(400)
	fmt.Println("RAND RANGE:", randRange)

	for k := 0; k < numKeys; k++ {
		rootKey := rootKeys[rand.Intn(len(rootKeys))]
		phone := fmt.Sprintf("0987%06d", rand.Intn(randRange))
		hashNum := uint64(rand.Intn(randRange))

		usage := customerUsage{
			Tenant:     rootKey.Tenant,
			CampaignID: rootKey.CampaignID,
			Phone:      phone,
			TermCode:   termCode,
			Hash:       hashNum,
		}

		combinedKeys = append(combinedKeys, combinedKey{
			rootKey: rootKey,
			key:     usage.getKey(),
		})

		usageMap[combinedKey{
			rootKey: usage.getRootKey(),
			key:     usage.getKey(),
		}] = usage

		fn := p.updater.UpsertBucket(newContext(), rootKey, usage)
		calls = append(calls, fn)
	}

	for k := 0; k < numKeys; k++ {
		rootKey := rootKeys[rand.Intn(len(rootKeys))]
		phone := fmt.Sprintf("0987%06d", rand.Intn(randRange))
		hashNum := uint64(rand.Intn(randRange))

		key := customerUsageKey{
			Phone:    phone,
			TermCode: termCode,
			hash:     hashNum,
		}

		delete(usageMap, combinedKey{
			rootKey: rootKey,
			key:     key,
		})

		fn := p.updater.DeleteBucket(newContext(), rootKey, key)
		calls = append(calls, fn)
	}

	for _, call := range calls {
		err := call()
		if err != nil {
			panic(err)
		}
	}

	var resultCalls []func() (Null[customerUsage], error)

	p.scannedBuckets = map[BucketKey[customerUsageRootKey]]struct{}{}

	for _, rootKey := range combinedKeys {
		fn := p.hash.Get(newContext(), rootKey.rootKey, rootKey.key)
		resultCalls = append(resultCalls, fn)
	}

	for i := range resultCalls {
		call := resultCalls[i]
		key := combinedKeys[i]

		result, err := call()
		assert.Equal(t, nil, err)

		expected, ok := usageMap[key]

		assert.Equal(t, ok, result.Valid)
		assert.Equal(t, expected, result.Data)
	}

	for key := range p.bucketDataMap {
		_, existed := p.scannedBuckets[key]
		if !existed {
			t.Fatalf("Invariant Violated: No Delete Completely")
		}
	}

	for key := range p.scannedBuckets {
		_, existed := p.bucketDataMap[key]
		if !existed {
			t.Fatalf("Invariant Violated: Scan Too Much")
		}
	}

	fmt.Println("SEED:", seed)
}

func TestHash_Property_Based__Run_Multi(t *testing.T) {
	for i := 0; i < 5; i++ {
		TestHash_PropertyBased__Upsert_Delete_And_Get__Without_Use_Hash_Func(t)
	}
}
