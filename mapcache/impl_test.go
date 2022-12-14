package mapcache

import (
	"context"
	"github.com/QuangTung97/memproxy"
	"github.com/stretchr/testify/assert"
	"testing"
)

type mapCacheTest struct {
	pipe   *memproxy.PipelineMock
	filler *FillerMock
	mc     MapCache
}

type ctxTestKeyType struct {
}

var ctxTestKey = &ctxTestKeyType{}

func newTestContext() context.Context {
	return context.WithValue(context.Background(), ctxTestKey, "test-value")
}

func newMapCacheTest() *mapCacheTest {
	sess := &memproxy.SessionMock{}
	pipe := &memproxy.PipelineMock{}
	filler := &FillerMock{}

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

	provider := NewProvider(filler)
	return &mapCacheTest{
		pipe:   pipe,
		filler: filler,
		mc: provider.New(newTestContext(), sess, pipe, "rootkey", SizeLog{
			Current:  8,
			Previous: 7,
			Version:  51,
		}),
	}
}

func (m *mapCacheTest) stubGet(resp memproxy.GetResponse, err error) {
	m.pipe.GetFunc = func(key string, options memproxy.GetOptions) func() (memproxy.GetResponse, error) {
		return func() (memproxy.GetResponse, error) {
			return resp, nil
		}
	}
}

func (m *mapCacheTest) stubLeaseGet(resp memproxy.LeaseGetResponse, err error) {
	m.pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		return func() (memproxy.LeaseGetResponse, error) {
			return resp, err
		}
	}
}

func (m *mapCacheTest) stubFillerGetBucket(resp GetBucketResponse, err error) {
	m.filler.GetBucketFunc = func(
		ctx context.Context, rootKey string, hashRange HashRange,
	) func() (GetBucketResponse, error) {
		return func() (GetBucketResponse, error) {
			return resp, err
		}
	}
}

func (m *mapCacheTest) stubLeaseSet(err error) {
	m.pipe.LeaseSetFunc = func(
		key string, data []byte, cas uint64, options memproxy.LeaseSetOptions,
	) func() (memproxy.LeaseSetResponse, error) {
		return func() (memproxy.LeaseSetResponse, error) {
			return memproxy.LeaseSetResponse{}, err
		}
	}
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get__Do_Fill__Returns_Data(t *testing.T) {
	m := newMapCacheTest()

	const key1 = "key01"

	m.stubGet(memproxy.GetResponse{
		Found: false,
	}, nil)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    887,
	}, nil)

	m.stubFillerGetBucket(GetBucketResponse{
		Entries: []Entry{
			{
				Key:  key1,
				Data: []byte("key data 01"),
			},
		},
	}, nil)

	m.stubLeaseSet(nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{
		Found: true,
		Data:  []byte("key data 01"),
	}, resp)

	calls := m.pipe.GetCalls()
	assert.Equal(t, 2, len(calls))
	assert.Equal(t, "rootkey:8:"+computeBucketKeyString(key1, 8), calls[0].Key)
	assert.Equal(t, "rootkey:7:"+computeBucketKeyString(key1, 7), calls[1].Key)

	leaseGetCalls := m.pipe.LeaseGetCalls()
	assert.Equal(t, 1, len(leaseGetCalls))
	assert.Equal(t, "rootkey:8:"+computeBucketKeyString(key1, 8), leaseGetCalls[0].Key)
}

func TestMapCache_Do_Call__Get__Found__Returns_Immediately(t *testing.T) {
	m := newMapCacheTest()

	const key1 = "key01"
	const key2 = "key02"

	m.stubGet(memproxy.GetResponse{
		Found: true,
		Data: marshalCacheBucket(CacheBucketContent{
			OriginSizeLogVersion: 42,
			Entries: []Entry{
				{
					Key:  key1,
					Data: []byte("content data 1"),
				},
				{
					Key:  key2,
					Data: []byte("content data 2"),
				},
			},
		}),
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{
		Found: true,
		Data:  []byte("content data 1"),
	}, resp)

	calls := m.pipe.GetCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, "rootkey:8:"+computeBucketKeyString(key1, 8), calls[0].Key)
}
