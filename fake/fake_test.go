package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/memproxy"
)

func newPipelineTest() memproxy.Pipeline {
	mc := New()
	return mc.Pipeline(context.Background())
}

func TestPipeline(t *testing.T) {
	t.Run("lease-get-then-set", func(t *testing.T) {
		pipe := newPipelineTest()
		defer pipe.Finish()

		fn1 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		fn2 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})

		resp1, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    1,
		}, resp1)

		resp2, err := fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseRejected,
			CAS:    1,
		}, resp2)

		// Do Set
		setFn := pipe.LeaseSet("KEY01", []byte("data 01"), 1, memproxy.LeaseSetOptions{})
		setResp, err := setFn()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseSetResponse{
			Status: memproxy.LeaseSetStatusStored,
		}, setResp)

		fn3 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp3, err := fn3()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    1,
			Data:   []byte("data 01"),
		}, resp3)
	})

	t.Run("lease-get-2-different-keys", func(t *testing.T) {
		pipe := newPipelineTest()
		defer pipe.Finish()

		fn1 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		fn2 := pipe.LeaseGet("KEY02", memproxy.LeaseGetOptions{})

		resp1, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    1,
		}, resp1)

		resp2, err := fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2,
		}, resp2)
	})

	t.Run("set-not-stored", func(t *testing.T) {
		pipe := newPipelineTest()
		defer pipe.Finish()

		fn1 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})

		resp1, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    1,
		}, resp1)

		setFn := pipe.LeaseSet("KEY01", []byte("data 01"), 3, memproxy.LeaseSetOptions{})
		setResp, err := setFn()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseSetResponse{
			Status: memproxy.LeaseSetStatusNotStored,
		}, setResp)

		fn2 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp2, err := fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseRejected,
			CAS:    1,
		}, resp2)
	})

	t.Run("set-not-exist-not-stored", func(t *testing.T) {
		pipe := newPipelineTest()
		defer pipe.Finish()

		setFn := pipe.LeaseSet("KEY01", []byte("data 01"), 3, memproxy.LeaseSetOptions{})
		setResp, err := setFn()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseSetResponse{
			Status: memproxy.LeaseSetStatusNotStored,
		}, setResp)

		fn2 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp2, err := fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    1,
		}, resp2)
	})

	t.Run("lease-get-and-delete", func(t *testing.T) {
		pipe := newPipelineTest()
		defer pipe.Finish()

		fn1 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp1, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    1,
		}, resp1)

		delResp, err := pipe.Delete("KEY01", memproxy.DeleteOptions{})()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.DeleteResponse{}, delResp)

		fn2 := pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp2, err := fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2,
		}, resp2)
	})

	t.Run("lease-get-and-delete-and-lease-get-on-another-pipeline", func(t *testing.T) {
		mc := New()
		pipe1 := mc.Pipeline(context.Background())

		fn1 := pipe1.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp1, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    1,
		}, resp1)

		_, err = pipe1.Delete("KEY01", memproxy.DeleteOptions{})()
		assert.Equal(t, nil, err)

		pipe2 := mc.Pipeline(context.Background())
		fn2 := pipe2.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp2, err := fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2,
		}, resp2)
	})
}

func TestPipeline__Do_Finish(t *testing.T) {
	t.Run("call-finish", func(t *testing.T) {
		mc := New()
		pipe1 := mc.Pipeline(context.Background())

		resp1, err := pipe1.LeaseGet("KEY01", memproxy.LeaseGetOptions{})()
		assert.Equal(t, nil, err)

		pipe1.LeaseSet("KEY01", []byte("data 01"), resp1.CAS, memproxy.LeaseSetOptions{})

		pipe1.Finish()

		pipe2 := mc.Pipeline(context.Background())

		resp2, err := pipe2.LeaseGet("KEY01", memproxy.LeaseGetOptions{})()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    1,
			Data:   []byte("data 01"),
		}, resp2)
	})

	t.Run("call-execute", func(t *testing.T) {
		mc := New()
		pipe1 := mc.Pipeline(context.Background())

		resp1, err := pipe1.LeaseGet("KEY01", memproxy.LeaseGetOptions{})()
		assert.Equal(t, nil, err)

		pipe1.LeaseSet("KEY01", []byte("data 01"), resp1.CAS, memproxy.LeaseSetOptions{})

		pipe1.Execute()

		pipe2 := mc.Pipeline(context.Background())

		resp2, err := pipe2.LeaseGet("KEY01", memproxy.LeaseGetOptions{})()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    1,
			Data:   []byte("data 01"),
		}, resp2)
	})

	t.Run("lower-session", func(t *testing.T) {
		mc := New()
		pipe := mc.Pipeline(context.Background())

		sess := pipe.LowerSession()

		calls := 0
		sess.AddNextCall(memproxy.NewSimpleCallBack(func() {
			calls++
		}))
		sess.Execute()

		assert.Equal(t, 1, calls)

		assert.Nil(t, mc.Close())
	})
}
