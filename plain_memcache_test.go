package memproxy

import (
	"context"
	"testing"
	"time"

	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/stretchr/testify/assert"
)

type plainMemcacheTest struct {
	pipe Pipeline
}

func newPlainMemcacheTest(t *testing.T) *plainMemcacheTest {
	client, err := memcache.New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})

	err = client.Pipeline().FlushAll()()
	if err != nil {
		panic(err)
	}

	cache := NewPlainMemcache(client, WithPlainMemcacheLeaseDuration(7))

	return &plainMemcacheTest{
		pipe: cache.Pipeline(context.Background()),
	}
}

func TestPlainMemcache_LeaseGet_Granted_And_LeaseSet__Then_LeaseGet_Found(t *testing.T) {
	m := newPlainMemcacheTest(t)

	const key = "key01"

	// Lease Get
	leaseGetResp, err := m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)

	cas := leaseGetResp.CAS
	leaseGetResp.CAS = 0

	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseGranted,
	}, leaseGetResp)

	assert.Greater(t, cas, uint64(0))

	// Do Set
	value := []byte("some value 01")

	setResp, err := m.pipe.LeaseSet(key, value, cas, LeaseSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseSetResponse{
		Status: LeaseSetStatusStored,
	}, setResp)

	// Lease Get Again
	leaseGetResp, err = m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusFound,
		CAS:    cas + 1,
		Data:   value,
	}, leaseGetResp)

	// Get Again
	leaseGetResp, err = m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)

	leaseGetResp.CAS = 1
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusFound,
		CAS:    1,
		Data:   value,
	}, leaseGetResp)
}

func TestPlainMemcache_LeaseGet_Rejected(t *testing.T) {
	m := newPlainMemcacheTest(t)

	const key = "key01"

	// Lease Get
	leaseGetResp, err := m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)

	cas := leaseGetResp.CAS
	leaseGetResp.CAS = 0

	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseGranted,
	}, leaseGetResp)

	assert.Greater(t, cas, uint64(0))

	// Lease Get Rejected
	leaseGetResp, err = m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseRejected,
		CAS:    cas,
	}, leaseGetResp)

	// Do Set
	value := []byte("some value 01")
	setResp, err := m.pipe.LeaseSet(key, value, cas, LeaseSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseSetResponse{
		Status: LeaseSetStatusStored,
	}, setResp)

	// Lease Get Again
	leaseGetResp, err = m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusFound,
		CAS:    cas + 1,
		Data:   value,
	}, leaseGetResp)
}

func TestPlainMemcache_LeaseSet_After_Delete__Rejected(t *testing.T) {
	m := newPlainMemcacheTest(t)

	const key = "key01"

	// Lease Get
	leaseGetResp, err := m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)

	cas := leaseGetResp.CAS
	leaseGetResp.CAS = 0

	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseGranted,
	}, leaseGetResp)
	assert.Greater(t, cas, uint64(0))

	// Delete
	delResp, err := m.pipe.Delete(key, DeleteOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, DeleteResponse{}, delResp)

	// Do Set
	value := []byte("some value 01")
	setResp, err := m.pipe.LeaseSet(key, value, cas, LeaseSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseSetResponse{
		Status: LeaseSetStatusNotStored,
	}, setResp)

	// Lease Get Again
	leaseGetResp, err = m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseGranted,
		CAS:    cas + 1,
	}, leaseGetResp)
}

func TestPlainMemcache_LeaseGet_After_Delete(t *testing.T) {
	m := newPlainMemcacheTest(t)

	const key = "key01"

	// Lease Get
	leaseGetResp, err := m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)

	cas := leaseGetResp.CAS
	leaseGetResp.CAS = 0

	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseGranted,
	}, leaseGetResp)

	assert.Greater(t, cas, uint64(0))

	// Do Set
	value := []byte("some value 01")

	setResp, err := m.pipe.LeaseSet(key, value, cas, LeaseSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseSetResponse{
		Status: LeaseSetStatusStored,
	}, setResp)

	// Lease Get Again
	leaseGetResp, err = m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusFound,
		CAS:    cas + 1,
		Data:   value,
	}, leaseGetResp)

	// Do Delete
	deleteResp, err := m.pipe.Delete(key, DeleteOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, DeleteResponse{}, deleteResp)

	leaseGetResp, err = m.pipe.LeaseGet(key, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseGranted,
		CAS:    cas + 2,
	}, leaseGetResp)
}

func TestPlainMemcache__Lease_Get__Pipeline(t *testing.T) {
	m1 := newPlainMemcacheTest(t)
	m2 := newPlainMemcacheTest(t)

	const key1 = "key01"
	const key2 = "key02"

	fn1 := m1.pipe.LeaseGet(key1, LeaseGetOptions{})
	fn2 := m1.pipe.LeaseGet(key2, LeaseGetOptions{})

	getResp, err := m2.pipe.LeaseGet(key1, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)

	cas := getResp.CAS
	getResp.CAS = 0

	assert.Greater(t, cas, uint64(0))

	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseGranted,
	}, getResp)

	// After Do Flush Pipeline
	getResp, err = fn1.Result()
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseRejected,
		CAS:    cas,
	}, getResp)

	getResp, err = fn2.Result()
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseGranted,
		CAS:    cas + 1,
	}, getResp)
}

func TestPlainMemcache__Lease_Get_Then_Execute(t *testing.T) {
	m1 := newPlainMemcacheTest(t)
	m2 := newPlainMemcacheTest(t)

	const key1 = "key01"
	const key2 = "key02"

	m1.pipe.LeaseGet(key1, LeaseGetOptions{})
	m1.pipe.LeaseGet(key2, LeaseGetOptions{})

	m1.pipe.Execute()
	time.Sleep(10 * time.Millisecond)

	getResp, err := m2.pipe.LeaseGet(key1, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)

	cas := getResp.CAS
	getResp.CAS = 0

	assert.Greater(t, cas, uint64(0))

	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusLeaseRejected,
	}, getResp)
}

func TestPlainMemcache__Finish_Do_Flush(t *testing.T) {
	m1 := newPlainMemcacheTest(t)
	m2 := newPlainMemcacheTest(t)

	const key1 = "key01"

	getResp, err := m1.pipe.LeaseGet(key1, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetStatusLeaseGranted, getResp.Status)

	cas := getResp.CAS

	data := []byte("some value 01")
	m1.pipe.LeaseSet(key1, data, cas, LeaseSetOptions{})

	m1.pipe.Finish()

	getResp, err = m2.pipe.LeaseGet(key1, LeaseGetOptions{}).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusFound,
		CAS:    cas + 1,
		Data:   data,
	}, getResp)
}

func TestPlainMemcache__With_Existing_Session(t *testing.T) {
	client, err := memcache.New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})

	err = client.Pipeline().FlushAll()()
	if err != nil {
		panic(err)
	}

	cache := NewPlainMemcache(client,
		WithPlainMemcacheLeaseDuration(7),
		WithPlainMemcacheSessionProvider(NewSessionProvider()),
	)

	provider := NewSessionProvider()
	sess := provider.New()

	pipe := cache.Pipeline(context.Background(), WithPipelineExistingSession(sess))

	assert.Same(t, sess.GetLower(), pipe.LowerSession())
}

func TestPlainMemcache__Invalid_Key(t *testing.T) {
	client, err := memcache.New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})

	err = client.Pipeline().FlushAll()()
	if err != nil {
		panic(err)
	}

	cache := NewPlainMemcache(client,
		WithPlainMemcacheLeaseDuration(7),
		WithPlainMemcacheSessionProvider(NewSessionProvider()),
	)

	pipe := cache.Pipeline(context.Background())
	fn := pipe.LeaseGet(" abcd ", LeaseGetOptions{})

	resp, err := fn.Result()
	assert.Equal(t, memcache.ErrInvalidKeyFormat, err)
	assert.Equal(t, LeaseGetResponse{}, resp)
}
