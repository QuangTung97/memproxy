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
}

func newPropertyTest() *propertyTest {
	sess := newFakeSession()
	pipe := &memproxy.PipelineMock{}

	cas := uint64(5562000)
	pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		return func() (memproxy.LeaseGetResponse, error) {
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
		return func() (memproxy.LeaseSetResponse, error) {
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
		return func() ([]byte, error) {
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
		7,
	)

	return &propertyTest{
		bucketDataMap: bucketDataMap,
		hash:          h,
		updater:       updater,
	}
}

func TestHash_PropertyBased__Upsert_And_Get(t *testing.T) {
	seed := time.Now().Unix()
	fmt.Println("SEED:", seed)
	rand.Seed(seed)

	p := newPropertyTest()

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
