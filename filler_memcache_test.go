package memproxy

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type fillerMemcacheTest struct {
	sess       *SessionMock
	originPipe *PipelineMock
	filler     *FillerMock
	pipe       Pipeline
}

type ctxTestKeyType struct {
}

var ctxTestKey = &ctxTestKeyType{}

func newTestContext() context.Context {
	return context.WithValue(context.Background(), ctxTestKey, "test-value")
}

func newFillerMemcacheTest() *fillerMemcacheTest {
	provider := &SessionProviderMock{}
	origin := &MemcacheMock{}
	originPipe := &PipelineMock{}
	filler := &FillerMock{}

	sess := &SessionMock{}

	provider.NewFunc = func() Session {
		return sess
	}

	origin.PipelineWithSessionFunc = func(ctx context.Context, sess Session) Pipeline {
		return originPipe
	}

	var calls []func()
	sess.AddNextCallFunc = func(fn func()) {
		calls = append(calls, fn)
	}
	sess.AddDelayedCallFunc = func(d time.Duration, fn func()) {
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

	mc := NewFillerMemcache(origin, provider, filler)
	return &fillerMemcacheTest{
		sess:       sess,
		originPipe: originPipe,
		filler:     filler,
		pipe:       mc.Pipeline(newTestContext()),
	}
}

func (m *fillerMemcacheTest) stubLeaseGet(resp LeaseGetResponse, err error) {
	m.originPipe.LeaseGetFunc = func(key string, options LeaseGetOptions) func() (LeaseGetResponse, error) {
		return func() (LeaseGetResponse, error) {
			return resp, err
		}
	}
}

type leaseGetResult struct {
	resp LeaseGetResponse
	err  error
}

func (m *fillerMemcacheTest) stubLeaseGetMulti(results ...leaseGetResult) {
	m.originPipe.LeaseGetFunc = func(key string, options LeaseGetOptions) func() (LeaseGetResponse, error) {
		index := len(m.originPipe.LeaseGetCalls()) - 1
		return func() (LeaseGetResponse, error) {
			r := results[index]
			return r.resp, r.err
		}
	}
}

func (m *fillerMemcacheTest) stubLeaseSet() {
	m.originPipe.LeaseSetFunc = func(key string, data []byte, cas uint64, options LeaseSetOptions) func() (LeaseSetResponse, error) {
		return func() (LeaseSetResponse, error) {
			return LeaseSetResponse{}, nil
		}
	}
}

func (m *fillerMemcacheTest) stubFill(respData []byte) {
	m.filler.FillFunc = func(ctx context.Context, key string, completeFn func(resp FillResponse, err error)) {
		m.sess.AddNextCall(func() {
			completeFn(FillResponse{Data: respData}, nil)
		})
	}
}

func TestFillerMemcache__Call_Origin_Lease_Get(t *testing.T) {
	m := newFillerMemcacheTest()

	const key1 = "KEY01"

	m.stubLeaseGet(LeaseGetResponse{
		Status: LeaseGetStatusFound,
		CAS:    22,
		Data:   []byte("test Data"),
	}, nil)

	_, _ = m.pipe.LeaseGet(key1, LeaseGetOptions{})()

	calls := m.originPipe.LeaseGetCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, key1, calls[0].Key)
	assert.Equal(t, LeaseGetOptions{}, calls[0].Options)
}

func TestFillerMemcache__Get_Found__Returns_Success(t *testing.T) {
	m := newFillerMemcacheTest()

	const key1 = "KEY01"

	m.stubLeaseGet(LeaseGetResponse{
		Status: LeaseGetStatusFound,
		CAS:    22,
		Data:   []byte("test Data"),
	}, nil)

	resp, err := m.pipe.LeaseGet(key1, LeaseGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusFound,
		CAS:    22,
		Data:   []byte("test Data"),
	}, resp)
}

func TestFillerMemcache__Get_Granted__Call_Filler(t *testing.T) {
	m := newFillerMemcacheTest()

	const key1 = "KEY01"

	m.stubLeaseGet(LeaseGetResponse{
		Status: LeaseGetStatusLeaseGranted,
		CAS:    33,
	}, nil)

	m.stubFill([]byte("test data"))

	m.stubLeaseSet()

	resp, err := m.pipe.LeaseGet(key1, LeaseGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusFound,
		Data:   []byte("test data"),
	}, resp)

	assert.Equal(t, 1, len(m.filler.FillCalls()))
	assert.Equal(t, newTestContext(), m.filler.FillCalls()[0].Ctx)
	assert.Equal(t, key1, m.filler.FillCalls()[0].Key)

	setCalls := m.originPipe.LeaseSetCalls()
	assert.Equal(t, 1, len(setCalls))
	assert.Equal(t, key1, setCalls[0].Key)
	assert.Equal(t, []byte("test data"), setCalls[0].Data)
	assert.Equal(t, uint64(33), setCalls[0].Cas)
	assert.Equal(t, LeaseSetOptions{}, setCalls[0].Options)
}

func newRejectedResult() leaseGetResult {
	return leaseGetResult{
		resp: LeaseGetResponse{
			Status: LeaseGetStatusLeaseRejected,
		},
	}
}

func TestFillerMemcache__Get_Rejected__Next_Found(t *testing.T) {
	m := newFillerMemcacheTest()

	const key1 = "KEY01"

	m.stubLeaseGetMulti(
		newRejectedResult(),
		leaseGetResult{
			resp: LeaseGetResponse{
				Status: LeaseGetStatusFound,
				CAS:    44,
				Data:   []byte("found test data"),
			},
		},
	)

	resp, err := m.pipe.LeaseGet(key1, LeaseGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		Status: LeaseGetStatusFound,
		CAS:    44,
		Data:   []byte("found test data"),
	}, resp)

	addCalls := m.sess.AddDelayedCallCalls()
	assert.Equal(t, 1, len(addCalls))
	assert.Equal(t, 5*time.Millisecond, addCalls[0].D)
}

func TestFillerMemcache__Get_Rejected__Until_Give_Up(t *testing.T) {
	m := newFillerMemcacheTest()

	const key1 = "KEY01"

	m.stubLeaseGetMulti(
		newRejectedResult(), // 5ms
		newRejectedResult(), // 20ms
		newRejectedResult(), // 80ms
		newRejectedResult(), // 320ms
		newRejectedResult(), // 1280ms
		newRejectedResult(), // give up
	)

	resp, err := m.pipe.LeaseGet(key1, LeaseGetOptions{})()
	assert.Equal(t, ErrExceededRejectRetryLimit, err)
	assert.Equal(t, LeaseGetResponse{}, resp)

	addCalls := m.sess.AddDelayedCallCalls()
	assert.Equal(t, 5, len(addCalls))
	assert.Equal(t, 5*time.Millisecond, addCalls[0].D)
	assert.Equal(t, 20*time.Millisecond, addCalls[1].D)
	assert.Equal(t, 80*time.Millisecond, addCalls[2].D)
	assert.Equal(t, 320*time.Millisecond, addCalls[3].D)
	assert.Equal(t, 1280*time.Millisecond, addCalls[4].D)
}
