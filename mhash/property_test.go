package mhash

import (
	"context"
	"fmt"
	"github.com/QuangTung97/memproxy"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

type propertyTest struct {
	bucketDataMap map[BucketKey[customerUsageRootKey]][]byte

	hash    *Hash[customerUsage, customerUsageRootKey, customerUsageKey]
	updater *HashUpdater[customerUsage, customerUsageRootKey, customerUsageKey]

	callOrders []string
}

func (p *propertyTest) addCall(op string, key string) {
	p.callOrders = append(p.callOrders, op+"::"+key)
}

func (p *propertyTest) clearCalls() {
	p.callOrders = []string{}
}

func newPropertyTest(maxHashesPerBucket int) *propertyTest {
	sess := newFakeSession()
	pipe := &memproxy.PipelineMock{}

	p := &propertyTest{
		callOrders: []string{},
	}

	cas := uint64(5562000)
	pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		p.addCall("lease-get", key)
		return func() (memproxy.LeaseGetResponse, error) {
			p.addCall("lease-get-func", key)

			cas++
			return memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseGranted,
				CAS:    cas,
			}, nil
		}
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

	bucketDataMap := map[BucketKey[customerUsageRootKey]][]byte{}

	var filler Filler[customerUsageRootKey] = func(
		ctx context.Context, key BucketKey[customerUsageRootKey],
	) func() ([]byte, error) {
		p.addCall("fill-get", key.String())
		return func() ([]byte, error) {
			p.addCall("fill-get-func", key.String())
			return bucketDataMap[key], nil
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

	const numKeys = 1000

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

	// Get Again
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

	// Get After Delete
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

	// Get Again
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

	// Get After Delete
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

	//==================================
	// Do Delete
	//==================================
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

	usage1 := newUsage(1, 0x8821)
	usage2 := newUsage(2, 0x8822)
	usage3 := newUsage(3, 0x8822)

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

	data1 := mustMarshalBucket(0, 0, Bucket[customerUsage]{
		NextLevel:       8,
		NextLevelPrefix: 0x8800,
		Items: []customerUsage{
			usage1,
		},
		Bitset: newBitSet(0x22),
	})
	data2 := mustMarshalBucket(0, 0, Bucket[customerUsage]{
		Items: []customerUsage{
			usage2, usage3,
		},
	})
	assert.Equal(t, map[BucketKey[customerUsageRootKey]][]byte{
		BucketKey[customerUsageRootKey]{
			RootKey: rootKey,
			Level:   0,
		}: data1,
		BucketKey[customerUsageRootKey]{
			RootKey: rootKey,
			Level:   8,
			Hash:    0x8822,
		}: data2,
	}, p.bucketDataMap)
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

	usage1 := newUsage(1, 0x8821)
	usage2 := newUsage(2, 0x8822)
	usage3 := newUsage(3, 0x8822)

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
		NextLevel:       8,
		NextLevelPrefix: 0x8800,
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
			Level:   8,
			Hash:    0x8822,
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

	usage1 := newUsage(1, 0x8821)
	usage2 := newUsage(2, 0x8822)
	usage3 := newUsage(3, 0x8822)

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

	usage1 := newUsage(1, 0x8821)
	usage2 := newUsage(2, 0x8822)
	usage3 := newUsage(3, 0x8822)

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
