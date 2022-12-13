package mapcache

import (
	"context"
	"github.com/QuangTung97/memproxy"
	"github.com/stretchr/testify/assert"
	"testing"
)

type mapCacheTest struct {
	pipe *memproxy.PipelineMock
	mc   MapCache
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

	provider := NewProvider(nil)
	return &mapCacheTest{
		pipe: pipe,
		mc:   provider.New(newTestContext(), sess, pipe, "rootkey", 8),
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

	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{}, resp)

	calls := m.pipe.GetCalls()
	assert.Equal(t, 2, len(calls))
	assert.Equal(t, "rootkey:8:"+computeBucketKey(key1, 8), calls[0].Key)
	assert.Equal(t, "rootkey:7:"+computeBucketKey(key1, 7), calls[1].Key)
}
