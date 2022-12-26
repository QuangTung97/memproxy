package mapcache

import (
	"context"
	"errors"
	"github.com/QuangTung97/memproxy"
	"github.com/stretchr/testify/assert"
	"testing"
)

type mapCacheTest struct {
	pipe   *memproxy.PipelineMock
	filler *FillerMock
	mc     MapCache
	inv    Invalidator
}

type ctxTestKeyType struct {
}

var ctxTestKey = &ctxTestKeyType{}

func newTestContext() context.Context {
	return context.WithValue(context.Background(), ctxTestKey, "test-value")
}

func defaultSizeLog() SizeLog {
	return SizeLog{
		Current:  8,
		Previous: 7,
		Version:  51,
	}
}

func newMapCacheTest(sizeLog SizeLog) *mapCacheTest {
	sess := &memproxy.SessionMock{}

	filler := &FillerMock{}
	fillerFactory := &FillerFactoryMock{
		NewFunc: func() Filler {
			return filler
		},
	}

	client := &memproxy.MemcacheMock{}
	pipe := &memproxy.PipelineMock{}

	pipe.DeleteFunc = func(key string, options memproxy.DeleteOptions) func() (memproxy.DeleteResponse, error) {
		return nil
	}

	client.PipelineFunc = func(ctx context.Context, sess memproxy.Session) memproxy.Pipeline {
		return pipe
	}

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

	const rootKey = "rootkey"

	provider := NewProvider(client, fillerFactory)
	return &mapCacheTest{
		pipe:   pipe,
		filler: filler,
		mc:     provider.New(newTestContext(), sess, rootKey, sizeLog, NewOptions{Params: "root-params"}),
		inv:    NewInvalidatorFactory().New(rootKey, sizeLog),
	}
}

func (m *mapCacheTest) stubGet(resp memproxy.GetResponse, err error) {
	m.pipe.GetFunc = func(key string, options memproxy.GetOptions) func() (memproxy.GetResponse, error) {
		return func() (memproxy.GetResponse, error) {
			return resp, err
		}
	}
}

func (m *mapCacheTest) stubGetMulti(resp ...memproxy.GetResponse) {
	m.pipe.GetFunc = func(key string, options memproxy.GetOptions) func() (memproxy.GetResponse, error) {
		index := len(m.pipe.GetCalls()) - 1
		return func() (memproxy.GetResponse, error) {
			return resp[index], nil
		}
	}
}

func (m *mapCacheTest) stubGetMultiErrors(errList ...error) {
	m.pipe.GetFunc = func(key string, options memproxy.GetOptions) func() (memproxy.GetResponse, error) {
		index := len(m.pipe.GetCalls()) - 1
		return func() (memproxy.GetResponse, error) {
			return memproxy.GetResponse{}, errList[index]
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
		ctx context.Context, newOptions NewOptions, hashRange HashRange,
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
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"
	const key2 = "key02"

	m.stubGet(memproxy.GetResponse{
		Found: false,
	}, nil)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    887,
	}, nil)

	entries := []Entry{
		{
			Key:  key1,
			Data: []byte("key data 01"),
		},
		{
			Key:  key2,
			Data: []byte("key data 02"),
		},
	}

	m.stubFillerGetBucket(GetBucketResponse{
		Entries: entries,
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
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), calls[0].Key)
	assert.Equal(t, "rootkey:7:50:"+computeBucketKeyString(key1, 7), calls[1].Key)

	leaseGetCalls := m.pipe.LeaseGetCalls()
	assert.Equal(t, 1, len(leaseGetCalls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), leaseGetCalls[0].Key)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 1, len(setCalls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), setCalls[0].Key)
	assert.Equal(t, marshalCacheBucket(CacheBucketContent{
		OriginSizeLogVersion: 51,
		Entries:              entries,
	}), setCalls[0].Data)
	assert.Equal(t, uint64(887), setCalls[0].Cas)

	getBucketCalls := m.filler.GetBucketCalls()
	assert.Equal(t, 1, len(getBucketCalls))
	assert.Equal(t, newTestContext(), getBucketCalls[0].Ctx)
	assert.Equal(t, NewOptions{Params: "root-params"}, getBucketCalls[0].Options)
	assert.Equal(t, computeHashRange(hashFunc(key1), 8), getBucketCalls[0].HashRange)
}

func TestMapCache_Do_Call__Get__Found__Returns_Immediately(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

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
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), calls[0].Key)
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get__Do_Fill__Returns_Not_Found(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"
	const key2 = "key02"

	m.stubGet(memproxy.GetResponse{
		Found: false,
	}, nil)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    887,
	}, nil)

	entries := []Entry{
		{
			Key:  key2,
			Data: []byte("key data 02"),
		},
	}

	m.stubFillerGetBucket(GetBucketResponse{
		Entries: entries,
	}, nil)

	m.stubLeaseSet(nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{}, resp)

	calls := m.pipe.GetCalls()
	assert.Equal(t, 2, len(calls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), calls[0].Key)
	assert.Equal(t, "rootkey:7:50:"+computeBucketKeyString(key1, 7), calls[1].Key)

	leaseGetCalls := m.pipe.LeaseGetCalls()
	assert.Equal(t, 1, len(leaseGetCalls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), leaseGetCalls[0].Key)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 1, len(setCalls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), setCalls[0].Key)
	assert.Equal(t, marshalCacheBucket(CacheBucketContent{
		OriginSizeLogVersion: 51,
		Entries:              entries,
	}), setCalls[0].Data)
	assert.Equal(t, uint64(887), setCalls[0].Cas)
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get__Do_Get_Lower__Found(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"
	const key2 = "key02"

	entries := []Entry{
		{
			Key:  key1,
			Data: []byte("key data 01"),
		},
		{
			Key:  key2,
			Data: []byte("key data 02"),
		},
	}

	m.stubGetMulti(
		memproxy.GetResponse{
			Found: false,
		},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 50,
				Entries:              entries,
			}),
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    887,
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
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), calls[0].Key)
	assert.Equal(t, "rootkey:7:50:"+computeBucketKeyString(key1, 7), calls[1].Key)

	leaseGetCalls := m.pipe.LeaseGetCalls()
	assert.Equal(t, 1, len(leaseGetCalls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), leaseGetCalls[0].Key)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 1, len(setCalls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), setCalls[0].Key)

	assert.Equal(t, uint64(887), setCalls[0].Cas)

	bucket, err := unmarshalCacheBucket(setCalls[0].Data)
	assert.Equal(t, nil, err)
	assert.Equal(t, CacheBucketContent{
		OriginSizeLogVersion: 50,
		Entries: []Entry{
			{
				Key:  key1,
				Data: []byte("key data 01"),
			},
		},
	}, bucket)
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get_Found(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"
	const key2 = "key02"

	entries := []Entry{
		{
			Key:  key1,
			Data: []byte("key data 01"),
		},
		{
			Key:  key2,
			Data: []byte("key data 02"),
		},
	}

	m.stubGetMulti(
		memproxy.GetResponse{
			Found: false,
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusFound,
		Data: marshalCacheBucket(CacheBucketContent{
			OriginSizeLogVersion: 51,
			Entries:              entries,
		}),
		CAS: 887,
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{
		Found: true,
		Data:  []byte("key data 01"),
	}, resp)

	calls := m.pipe.GetCalls()
	assert.Equal(t, 2, len(calls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), calls[0].Key)
	assert.Equal(t, "rootkey:7:50:"+computeBucketKeyString(key1, 7), calls[1].Key)

	leaseGetCalls := m.pipe.LeaseGetCalls()
	assert.Equal(t, 1, len(leaseGetCalls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), leaseGetCalls[0].Key)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 0, len(setCalls))
}

func TestMapCache_Do_Call__Get__Error(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"

	m.stubGet(memproxy.GetResponse{}, errors.New("some error"))

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, errors.New("some error"), err)
	assert.Equal(t, GetResponse{}, resp)

	calls := m.pipe.GetCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), calls[0].Key)

	leaseGetCalls := m.pipe.LeaseGetCalls()
	assert.Equal(t, 0, len(leaseGetCalls))

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 0, len(setCalls))
}

func TestMapCache_Do_Call__Get_Found_But_Invalid_Data(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"

	m.stubGet(memproxy.GetResponse{
		Found: true,
		Data:  []byte{22},
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, ErrInvalidBucketContentVersion, err)
	assert.Equal(t, GetResponse{}, resp)

	calls := m.pipe.GetCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), calls[0].Key)

	leaseGetCalls := m.pipe.LeaseGetCalls()
	assert.Equal(t, 0, len(leaseGetCalls))

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 0, len(setCalls))
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get_Error(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"

	m.stubGetMulti(
		memproxy.GetResponse{
			Found: false,
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{}, errors.New("some error"))

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, errors.New("some error"), err)
	assert.Equal(t, GetResponse{}, resp)
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get_Data_Invalid(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"

	m.stubGetMulti(
		memproxy.GetResponse{
			Found: false,
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusFound,
		CAS:    4455,
		Data:   nil,
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, ErrMissingBucketContent, err)
	assert.Equal(t, GetResponse{}, resp)
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get__Then_Get_Lower_Error(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"

	m.stubGetMultiErrors(
		nil,
		errors.New("some get error"),
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, errors.New("some get error"), err)
	assert.Equal(t, GetResponse{}, resp)

	deleteCalls := m.pipe.DeleteCalls()
	assert.Equal(t, 1, len(deleteCalls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), deleteCalls[0].Key)
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get__Then_Get_Lower_With_Invalid_Data(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"

	m.stubGetMulti(
		memproxy.GetResponse{
			Found: false,
		},
		memproxy.GetResponse{
			Found: true,
			Data:  []byte{1},
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, ErrMissingSizeLogOrigin, err)
	assert.Equal(t, GetResponse{}, resp)

	deleteCalls := m.pipe.DeleteCalls()
	assert.Equal(t, 1, len(deleteCalls))
	assert.Equal(t, "rootkey:8:51:"+computeBucketKeyString(key1, 8), deleteCalls[0].Key)
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get__Then_Cache_Get__Filter_Hash_Range(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  1,
		Previous: 0,
		Version:  71,
	})

	const key1 = "key01"
	const key2 = "key02"
	const key3 = "key03"
	const key4 = "key05"

	entry1 := Entry{
		Key:  key1,
		Data: []byte("key data 01"),
	}
	entry2 := Entry{
		Key:  key2,
		Data: []byte("key data 02"),
	}
	entry3 := Entry{
		Key:  key3,
		Data: []byte("key data 03"),
	}
	entry4 := Entry{
		Key:  key4,
		Data: []byte("key data 04"),
	}

	entries := []Entry{
		entry1, entry2, entry3, entry4,
	}

	m.stubGetMulti(
		memproxy.GetResponse{
			Found: false,
		},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 70,
				Entries:              entries,
			}),
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	m.stubLeaseSet(nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{
		Found: true,
		Data:  []byte("key data 01"),
	}, resp)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 1, len(setCalls))
	assert.Equal(t, "rootkey:1:71:"+computeBucketKeyString(key1, 1), setCalls[0].Key)
	assert.Equal(t, uint64(4455), setCalls[0].Cas)

	assert.Equal(t, "8", computeBucketKeyString(key1, 1))
	assert.Equal(t, "0", computeBucketKeyString(key2, 1))
	assert.Equal(t, "0", computeBucketKeyString(key3, 1))
	assert.Equal(t, "8", computeBucketKeyString(key4, 1))

	cacheBucket, err := unmarshalCacheBucket(setCalls[0].Data)
	assert.Equal(t, nil, err)
	assert.Equal(t, CacheBucketContent{
		OriginSizeLogVersion: 70,
		Entries: []Entry{
			entry1, entry4,
		},
	}, cacheBucket)

	deleteCalls := m.pipe.DeleteCalls()
	assert.Equal(t, 0, len(deleteCalls))
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get__Then_GetBucket_Error(t *testing.T) {
	m := newMapCacheTest(defaultSizeLog())

	const key1 = "key01"

	m.stubGetMultiErrors(
		nil,
		nil,
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	m.stubFillerGetBucket(GetBucketResponse{}, errors.New("get bucket error"))

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, errors.New("get bucket error"), err)
	assert.Equal(t, GetResponse{}, resp)
}

func TestMapCache_Do_Call__Get__Not_Found__SizeLog_Bigger__Do_Lease_Get__And_Get_Two_Lower_Buckets(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  6,
		Previous: 7,
		Version:  61,
	})

	const key1 = "key01"

	m.stubGetMultiErrors(
		nil,
		nil,
		nil,
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
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

	getCalls := m.pipe.GetCalls()
	assert.Equal(t, 3, len(getCalls))

	hash := hashFunc(key1)

	assert.Equal(t, "f6", computeBucketKey(hash, 7))

	assert.Equal(t, "rootkey:6:61:f4", getCalls[0].Key)
	assert.Equal(t, "rootkey:7:60:f4", getCalls[1].Key)
	assert.Equal(t, "rootkey:7:60:f6", getCalls[2].Key)
}

func TestMapCache_Do_Call__Get__Not_Found__Do_Lease_Get__Then_Cache_Get__Combine_Buckets(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  0,
		Previous: 1,
		Version:  71,
	})

	const key1 = "key01"
	const key2 = "key02"
	const key3 = "key03"
	const key4 = "key05"

	entry1 := Entry{
		Key:  key1,
		Data: []byte("key data 01"),
	}
	entry2 := Entry{
		Key:  key2,
		Data: []byte("key data 02"),
	}
	entry3 := Entry{
		Key:  key3,
		Data: []byte("key data 03"),
	}
	entry4 := Entry{
		Key:  key4,
		Data: []byte("key data 04"),
	}

	m.stubGetMulti(
		memproxy.GetResponse{
			Found: false,
		},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 70,
				Entries: []Entry{
					entry1, entry4,
				},
			}),
		},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 70,
				Entries: []Entry{
					entry2, entry3,
				},
			}),
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	m.stubLeaseSet(nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{
		Found: true,
		Data:  []byte("key data 01"),
	}, resp)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 1, len(setCalls))
	assert.Equal(t, "rootkey:0:71:", setCalls[0].Key)
	assert.Equal(t, uint64(4455), setCalls[0].Cas)

	assert.Equal(t, "8", computeBucketKeyString(key1, 1))
	assert.Equal(t, "0", computeBucketKeyString(key2, 1))
	assert.Equal(t, "0", computeBucketKeyString(key3, 1))
	assert.Equal(t, "8", computeBucketKeyString(key4, 1))

	getCalls := m.pipe.GetCalls()
	assert.Equal(t, 3, len(getCalls))
	assert.Equal(t, "rootkey:0:71:", getCalls[0].Key)
	assert.Equal(t, "rootkey:1:70:0", getCalls[1].Key)
	assert.Equal(t, "rootkey:1:70:8", getCalls[2].Key)

	cacheBucket, err := unmarshalCacheBucket(setCalls[0].Data)
	assert.Equal(t, nil, err)
	assert.Equal(t, CacheBucketContent{
		OriginSizeLogVersion: 70,
		Entries: []Entry{
			entry1, entry4,
			entry2, entry3,
		},
	}, cacheBucket)
}

func TestMapCache__Two_Lower_Buckets__Second_One_Not_Found(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  0,
		Previous: 1,
		Version:  71,
	})

	const key1 = "key01"
	const key2 = "key02"
	const key3 = "key03"
	const key4 = "key05"

	entry1 := Entry{
		Key:  key1,
		Data: []byte("key data 01"),
	}
	entry2 := Entry{
		Key:  key2,
		Data: []byte("key data 02"),
	}
	entry3 := Entry{
		Key:  key3,
		Data: []byte("key data 03"),
	}
	entry4 := Entry{
		Key:  key4,
		Data: []byte("key data 04"),
	}

	m.stubGetMulti(
		memproxy.GetResponse{
			Found: false,
		},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 70,
				Entries: []Entry{
					entry1, entry4,
				},
			}),
		},
		memproxy.GetResponse{},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	m.stubLeaseSet(nil)

	m.stubFillerGetBucket(GetBucketResponse{
		Entries: []Entry{
			entry1, entry2,
			entry3, entry4,
		},
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{
		Found: true,
		Data:  []byte("key data 01"),
	}, resp)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 1, len(setCalls))
	assert.Equal(t, "rootkey:0:71:", setCalls[0].Key)
	assert.Equal(t, uint64(4455), setCalls[0].Cas)

	assert.Equal(t, "8", computeBucketKeyString(key1, 1))
	assert.Equal(t, "0", computeBucketKeyString(key2, 1))
	assert.Equal(t, "0", computeBucketKeyString(key3, 1))
	assert.Equal(t, "8", computeBucketKeyString(key4, 1))

	getCalls := m.pipe.GetCalls()
	assert.Equal(t, 3, len(getCalls))
	assert.Equal(t, "rootkey:0:71:", getCalls[0].Key)
	assert.Equal(t, "rootkey:1:70:0", getCalls[1].Key)
	assert.Equal(t, "rootkey:1:70:8", getCalls[2].Key)

	cacheBucket, err := unmarshalCacheBucket(setCalls[0].Data)
	assert.Equal(t, nil, err)
	assert.Equal(t, CacheBucketContent{
		OriginSizeLogVersion: 71,
		Entries: []Entry{
			entry1, entry2,
			entry3, entry4,
		},
	}, cacheBucket)

	assert.Equal(t, 1, len(m.filler.GetBucketCalls()))
	assert.Equal(t, computeHashRange(hashFunc(key1), 0), m.filler.GetBucketCalls()[0].HashRange)
}

func TestMapCache__Two_Lower_Buckets__First_One_Wrong_Version(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  0,
		Previous: 1,
		Version:  71,
	})

	const key1 = "key01"
	const key2 = "key02"
	const key3 = "key03"
	const key4 = "key05"

	entry1 := Entry{
		Key:  key1,
		Data: []byte("key data 01"),
	}
	entry2 := Entry{
		Key:  key2,
		Data: []byte("key data 02"),
	}
	entry3 := Entry{
		Key:  key3,
		Data: []byte("key data 03"),
	}
	entry4 := Entry{
		Key:  key4,
		Data: []byte("key data 04"),
	}

	m.stubGetMulti(
		memproxy.GetResponse{
			Found: false,
		},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 69,
				Entries: []Entry{
					entry1, entry4,
				},
			}),
		},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 70,
				Entries: []Entry{
					entry2, entry3,
				},
			}),
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	m.stubLeaseSet(nil)

	m.stubFillerGetBucket(GetBucketResponse{
		Entries: []Entry{
			entry1, entry2,
			entry3, entry4,
		},
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{
		Found: true,
		Data:  []byte("key data 01"),
	}, resp)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 1, len(setCalls))
	assert.Equal(t, "rootkey:0:71:", setCalls[0].Key)
	assert.Equal(t, uint64(4455), setCalls[0].Cas)

	assert.Equal(t, "8", computeBucketKeyString(key1, 1))
	assert.Equal(t, "0", computeBucketKeyString(key2, 1))
	assert.Equal(t, "0", computeBucketKeyString(key3, 1))
	assert.Equal(t, "8", computeBucketKeyString(key4, 1))

	getCalls := m.pipe.GetCalls()
	assert.Equal(t, 3, len(getCalls))
	assert.Equal(t, "rootkey:0:71:", getCalls[0].Key)
	assert.Equal(t, "rootkey:1:70:0", getCalls[1].Key)
	assert.Equal(t, "rootkey:1:70:8", getCalls[2].Key)

	cacheBucket, err := unmarshalCacheBucket(setCalls[0].Data)
	assert.Equal(t, nil, err)
	assert.Equal(t, CacheBucketContent{
		OriginSizeLogVersion: 71,
		Entries: []Entry{
			entry1, entry2,
			entry3, entry4,
		},
	}, cacheBucket)

	assert.Equal(t, 1, len(m.filler.GetBucketCalls()))
}

func TestMapCache__Two_Lower_Buckets__Second_One_Wrong_Version(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  0,
		Previous: 1,
		Version:  71,
	})

	const key1 = "key01"
	const key2 = "key02"
	const key3 = "key03"
	const key4 = "key05"

	entry1 := Entry{
		Key:  key1,
		Data: []byte("key data 01"),
	}
	entry2 := Entry{
		Key:  key2,
		Data: []byte("key data 02"),
	}
	entry3 := Entry{
		Key:  key3,
		Data: []byte("key data 03"),
	}
	entry4 := Entry{
		Key:  key4,
		Data: []byte("key data 04"),
	}

	m.stubGetMulti(
		memproxy.GetResponse{
			Found: false,
		},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 70,
				Entries: []Entry{
					entry1, entry4,
				},
			}),
		},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 68,
				Entries: []Entry{
					entry2, entry3,
				},
			}),
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	m.stubLeaseSet(nil)

	m.stubFillerGetBucket(GetBucketResponse{
		Entries: []Entry{
			entry1, entry2,
			entry3, entry4,
		},
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{
		Found: true,
		Data:  []byte("key data 01"),
	}, resp)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 1, len(setCalls))
	assert.Equal(t, "rootkey:0:71:", setCalls[0].Key)
	assert.Equal(t, uint64(4455), setCalls[0].Cas)

	assert.Equal(t, "8", computeBucketKeyString(key1, 1))
	assert.Equal(t, "0", computeBucketKeyString(key2, 1))
	assert.Equal(t, "0", computeBucketKeyString(key3, 1))
	assert.Equal(t, "8", computeBucketKeyString(key4, 1))

	getCalls := m.pipe.GetCalls()
	assert.Equal(t, 3, len(getCalls))
	assert.Equal(t, "rootkey:0:71:", getCalls[0].Key)
	assert.Equal(t, "rootkey:1:70:0", getCalls[1].Key)
	assert.Equal(t, "rootkey:1:70:8", getCalls[2].Key)

	cacheBucket, err := unmarshalCacheBucket(setCalls[0].Data)
	assert.Equal(t, nil, err)
	assert.Equal(t, CacheBucketContent{
		OriginSizeLogVersion: 71,
		Entries: []Entry{
			entry1, entry2,
			entry3, entry4,
		},
	}, cacheBucket)

	assert.Equal(t, 1, len(m.filler.GetBucketCalls()))
}

func TestMapCache__Two_Lower_Buckets__First_One_Get_Error(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  0,
		Previous: 1,
		Version:  71,
	})

	const key1 = "key01"

	m.stubGetMultiErrors(
		nil,
		errors.New("some error"),
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, errors.New("some error"), err)
	assert.Equal(t, GetResponse{}, resp)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 0, len(setCalls))

	getCalls := m.pipe.GetCalls()
	assert.Equal(t, 3, len(getCalls))
	assert.Equal(t, "rootkey:0:71:", getCalls[0].Key)
	assert.Equal(t, "rootkey:1:70:0", getCalls[1].Key)
	assert.Equal(t, "rootkey:1:70:8", getCalls[2].Key)

	assert.Equal(t, 0, len(m.filler.GetBucketCalls()))
}

func TestMapCache__Two_Lower_Buckets__Second_One_Get_Error(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  0,
		Previous: 1,
		Version:  71,
	})

	const key1 = "key01"

	m.stubGetMultiErrors(
		nil,
		nil,
		errors.New("some error"),
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, errors.New("some error"), err)
	assert.Equal(t, GetResponse{}, resp)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 0, len(setCalls))

	getCalls := m.pipe.GetCalls()
	assert.Equal(t, 3, len(getCalls))
	assert.Equal(t, "rootkey:0:71:", getCalls[0].Key)
	assert.Equal(t, "rootkey:1:70:0", getCalls[1].Key)
	assert.Equal(t, "rootkey:1:70:8", getCalls[2].Key)

	assert.Equal(t, 0, len(m.filler.GetBucketCalls()))
}

func TestMapCache__Two_Lower_Buckets__First_One_Data_Invalid(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  0,
		Previous: 1,
		Version:  71,
	})

	const key1 = "key01"

	m.stubGetMulti(
		memproxy.GetResponse{},
		memproxy.GetResponse{
			Found: true,
			Data:  []byte{2},
		},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 70,
			}),
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, ErrInvalidBucketContentVersion, err)
	assert.Equal(t, GetResponse{}, resp)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 0, len(setCalls))

	getCalls := m.pipe.GetCalls()
	assert.Equal(t, 3, len(getCalls))
	assert.Equal(t, "rootkey:0:71:", getCalls[0].Key)
	assert.Equal(t, "rootkey:1:70:0", getCalls[1].Key)
	assert.Equal(t, "rootkey:1:70:8", getCalls[2].Key)

	assert.Equal(t, 0, len(m.filler.GetBucketCalls()))
}

func TestMapCache__Two_Lower_Buckets__Second_One_Data_Invalid(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  0,
		Previous: 1,
		Version:  71,
	})

	const key1 = "key01"

	m.stubGetMulti(
		memproxy.GetResponse{},
		memproxy.GetResponse{
			Found: true,
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: 70,
			}),
		},
		memproxy.GetResponse{
			Found: true,
			Data:  []byte{2},
		},
	)

	m.stubLeaseGet(memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusLeaseGranted,
		CAS:    4455,
		Data:   nil,
	}, nil)

	// Check Map Cache Get
	resp, err := m.mc.Get(key1, GetOptions{})()

	assert.Equal(t, ErrInvalidBucketContentVersion, err)
	assert.Equal(t, GetResponse{}, resp)

	setCalls := m.pipe.LeaseSetCalls()
	assert.Equal(t, 0, len(setCalls))

	getCalls := m.pipe.GetCalls()
	assert.Equal(t, 3, len(getCalls))
	assert.Equal(t, "rootkey:0:71:", getCalls[0].Key)
	assert.Equal(t, "rootkey:1:70:0", getCalls[1].Key)
	assert.Equal(t, "rootkey:1:70:8", getCalls[2].Key)

	assert.Equal(t, 0, len(m.filler.GetBucketCalls()))
}

func TestMapCache_Get_Delete_Keys(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  5,
		Previous: 4,
		Version:  71,
	})
	const key1 = "KEY01"

	keys := m.inv.DeleteKeys(key1, DeleteKeyOptions{})
	assert.Equal(t, []string{
		"rootkey:5:71:" + computeBucketKeyString(key1, 5),
		"rootkey:4:70:" + computeBucketKeyString(key1, 4),
	}, keys)
}

func TestMapCache_Get_Delete_Keys__With_Previous_Higher(t *testing.T) {
	m := newMapCacheTest(SizeLog{
		Current:  8,
		Previous: 9,
		Version:  71,
	})
	const key1 = "KEY01"

	keys := m.inv.DeleteKeys(key1, DeleteKeyOptions{})

	hashRange := computeHashRange(hashFunc(key1), 8)
	assert.Equal(t, []string{
		"rootkey:8:71:" + computeBucketKeyString(key1, 8),
		"rootkey:9:70:" + computeBucketKey(hashRange.Begin, 9),
		"rootkey:9:70:" + computeBucketKey(hashRange.End, 9),
	}, keys)
}
