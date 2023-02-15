package proxy

import (
	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/QuangTung97/memproxy"
	"github.com/stretchr/testify/assert"
	"testing"
)

func clearMemcache(c *memcache.Client) {
	pipe := c.Pipeline()
	defer pipe.Finish()
	err := pipe.FlushAll()()
	if err != nil {
		panic(err)
	}
}

func newMemcacheWithProxy(t *testing.T) memproxy.Memcache {
	clearClient, err := memcache.New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}
	clearMemcache(clearClient)
	err = clearClient.Close()
	if err != nil {
		panic(err)
	}

	server1 := SimpleServerConfig{
		ID:   1,
		Host: "localhost",
		Port: 11211,
	}

	servers := []SimpleServerConfig{server1}
	mc, closeFunc, err := NewSimpleReplicatedMemcache(servers, 1, NewSimpleStats(servers))
	if err != nil {
		panic(err)
	}
	t.Cleanup(closeFunc)

	return mc
}

func TestProxyIntegration(t *testing.T) {
	t.Run("simple-lease-get-set", func(t *testing.T) {
		mc := newMemcacheWithProxy(t)
		pipe := mc.Pipeline(newContext())
		defer pipe.Finish()

		fn1 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetStatusLeaseGranted, resp.Status)

		fn2 := pipe.LeaseSet("KEY01", []byte("some data 01"), resp.CAS, memproxy.LeaseSetOptions{})
		setResp, err := fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseSetResponse{
			Status: memproxy.LeaseSetStatusStored,
		}, setResp)

		fn3 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp, err = fn3()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetStatusFound, resp.Status)
		assert.Equal(t, []byte("some data 01"), resp.Data)
	})

	t.Run("simple-lease-get-set-multi", func(t *testing.T) {
		mc := newMemcacheWithProxy(t)
		pipe := mc.Pipeline(newContext())
		defer pipe.Finish()

		const key1 = "KEY01"
		const key2 = "KEY02"

		value1 := []byte("some data 01")
		value2 := []byte("some data 02")

		fn1 := pipe.LeaseGet(key1, memproxy.LeaseGetOptions{})
		fn2 := pipe.LeaseGet(key2, memproxy.LeaseGetOptions{})

		resp1, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetStatusLeaseGranted, resp1.Status)

		resp2, err := fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetStatusLeaseGranted, resp2.Status)

		// DO Set
		fn3 := pipe.LeaseSet(key1, value1, resp1.CAS, memproxy.LeaseSetOptions{})
		setResp, err := fn3()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseSetResponse{
			Status: memproxy.LeaseSetStatusStored,
		}, setResp)

		fn4 := pipe.LeaseSet(key2, value2, resp2.CAS, memproxy.LeaseSetOptions{})
		setResp, err = fn4()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseSetResponse{
			Status: memproxy.LeaseSetStatusStored,
		}, setResp)

		// Get Again
		fn5 := pipe.LeaseGet(key1, memproxy.LeaseGetOptions{})
		fn6 := pipe.LeaseGet(key2, memproxy.LeaseGetOptions{})

		resp1, err = fn5()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetStatusFound, resp1.Status)
		assert.Equal(t, value1, resp1.Data)

		resp2, err = fn6()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetStatusFound, resp2.Status)
		assert.Equal(t, value2, resp2.Data)
	})

	t.Run("lease-finish-and-then-new-pipeline", func(t *testing.T) {
		mc := newMemcacheWithProxy(t)
		pipe1 := mc.Pipeline(newContext())

		const key1 = "KEY01"
		value1 := []byte("some data 01")

		fn1 := pipe1.LeaseGet(key1, memproxy.LeaseGetOptions{})
		resp, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetStatusLeaseGranted, resp.Status)

		// DO Set
		pipe1.LeaseSet(key1, value1, resp.CAS, memproxy.LeaseSetOptions{})
		pipe1.Finish()

		// Get Again
		pipe2 := mc.Pipeline(newContext())
		fn3 := pipe2.LeaseGet(key1, memproxy.LeaseGetOptions{})
		resp, err = fn3()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetStatusFound, resp.Status)
		assert.Equal(t, value1, resp.Data)
	})
}
