package proxy

import (
	"context"
	"errors"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type pipelineTest struct {
	mc1 *mocks.MemcacheMock
	mc2 *mocks.MemcacheMock

	pipe1 *mocks.PipelineMock
	pipe2 *mocks.PipelineMock

	client memproxy.Memcache
	route  *RouteMock
	pipe   memproxy.Pipeline
}

const serverID1 = 31
const serverID2 = 32

type ctxKeyType struct {
}

var ctxKey = &ctxKeyType{}

func newContext() context.Context {
	return context.WithValue(context.Background(), ctxKey, "some value")
}

func newPipelineTest(t *testing.T) *pipelineTest {
	p := &pipelineTest{}

	mc1 := &mocks.MemcacheMock{}
	mc2 := &mocks.MemcacheMock{}

	p.mc1 = mc1
	p.mc2 = mc2

	p.pipe1 = &mocks.PipelineMock{}
	p.pipe2 = &mocks.PipelineMock{}

	p.mc1.PipelineFunc = func(
		ctx context.Context, sess memproxy.Session, options ...memproxy.PipelineOption,
	) memproxy.Pipeline {
		return p.pipe1
	}

	p.mc2.PipelineFunc = func(
		ctx context.Context, sess memproxy.Session, options ...memproxy.PipelineOption,
	) memproxy.Pipeline {
		return p.pipe2
	}

	var newCalls []SimpleServerConfig

	server1 := SimpleServerConfig{
		ID:   serverID1,
		Host: "localhost",
		Port: 11211,
	}
	server2 := SimpleServerConfig{
		ID:   serverID2,
		Host: "localhost",
		Port: 11212,
	}

	route := &RouteMock{}

	mc, err := New[SimpleServerConfig](Config[SimpleServerConfig]{
		Servers: []SimpleServerConfig{server1, server2},
		Route:   route,
	}, func(conf SimpleServerConfig) (memproxy.Memcache, error) {
		index := len(newCalls)
		newCalls = append(newCalls, conf)
		return []memproxy.Memcache{
			mc1,
			mc2,
		}[index], nil
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []SimpleServerConfig{server1, server2}, newCalls)

	sessProvider := memproxy.NewSessionProvider(time.Now, time.Sleep)

	p.client = mc
	p.route = route
	p.pipe = p.client.Pipeline(newContext(), sessProvider.New())

	return p
}

func (p *pipelineTest) stubSelect(idList ...ServerID) {
	p.route.SelectServerFunc = func(key string, failedServers []ServerID) ServerID {
		index := len(p.route.SelectServerCalls()) - 1
		return idList[index]
	}
}

func (p *pipelineTest) stubLeaseGet1(resp memproxy.LeaseGetResponse, err error) {
	p.pipe1.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		return func() (memproxy.LeaseGetResponse, error) {
			return resp, err
		}
	}
}

func (p *pipelineTest) stubLeaseGet2(resp memproxy.LeaseGetResponse, err error) {
	p.pipe2.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		return func() (memproxy.LeaseGetResponse, error) {
			return resp, err
		}
	}
}

func (p *pipelineTest) stubLeaseSet1(resp memproxy.LeaseSetResponse, err error) {
	p.pipe1.LeaseSetFunc = func(
		key string, data []byte, cas uint64, options memproxy.LeaseSetOptions,
	) func() (memproxy.LeaseSetResponse, error) {
		return func() (memproxy.LeaseSetResponse, error) {
			return resp, err
		}
	}
}

func TestPipeline(t *testing.T) {
	t.Run("call-select-server", func(t *testing.T) {
		p := newPipelineTest(t)

		p.stubSelect(serverID1)

		p.stubLeaseGet1(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    123,
			Data:   []byte("data default"),
		}, nil)

		p.pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})

		calls := p.route.SelectServerCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "KEY01", calls[0].Key)
		assert.Equal(t, 0, len(calls[0].FailedServers))

		pipeCalls := p.mc1.PipelineCalls()
		assert.Equal(t, 1, len(pipeCalls))
		assert.Equal(t, newContext(), pipeCalls[0].Ctx)
	})

	t.Run("call-lease-get", func(t *testing.T) {
		p := newPipelineTest(t)

		p.stubSelect(serverID1)
		p.stubLeaseGet1(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    7622,
			Data:   []byte("data 01"),
		}, nil)

		p.pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})

		getCalls := p.pipe1.LeaseGetCalls()
		assert.Equal(t, 1, len(getCalls))
		assert.Equal(t, "KEY01", getCalls[0].Key)
	})

	t.Run("lease-get-returns-data-on-found", func(t *testing.T) {
		p := newPipelineTest(t)

		p.stubSelect(serverID1)
		p.stubLeaseGet1(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    7622,
			Data:   []byte("data 01"),
		}, nil)

		fn := p.pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    7622,
			Data:   []byte("data 01"),
		}, resp)
	})

	t.Run("lease-get-with-error-retry-on-other-server", func(t *testing.T) {
		p := newPipelineTest(t)

		p.stubSelect(
			serverID1,
			serverID2,
		)
		p.stubLeaseGet1(memproxy.LeaseGetResponse{}, errors.New("some error"))
		p.stubLeaseGet2(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    443,
			Data:   []byte("found 01"),
		}, nil)

		fn := p.pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    443,
			Data:   []byte("found 01"),
		}, resp)

		// Check Init New Pipeline
		calls := p.route.SelectServerCalls()
		assert.Equal(t, 2, len(calls))

		assert.Equal(t, "KEY01", calls[0].Key)
		assert.Equal(t, 0, len(calls[0].FailedServers))

		assert.Equal(t, "KEY01", calls[1].Key)
		assert.Equal(t, []ServerID{serverID1}, calls[1].FailedServers)

		pipeCalls := p.mc1.PipelineCalls()
		assert.Equal(t, 1, len(pipeCalls))
		assert.Equal(t, newContext(), pipeCalls[0].Ctx)
	})
}

func TestPipeline__LeaseGet_Then_Set(t *testing.T) {
	t.Run("lease-get-then-set-no-fallback-on-error", func(t *testing.T) {
		p := newPipelineTest(t)

		p.stubSelect(serverID1)
		p.stubLeaseGet1(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2255,
		}, nil)

		fn := p.pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2255,
		}, resp)

		// Do Lease Set
		p.stubLeaseSet1(memproxy.LeaseSetResponse{}, nil)

		setFn := p.pipe.LeaseSet("KEY01", []byte("set data 01"), 2255, memproxy.LeaseSetOptions{})
		setResp, err := setFn()

		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseSetResponse{}, setResp)

		setCalls := p.pipe1.LeaseSetCalls()
		assert.Equal(t, 1, len(setCalls))
		assert.Equal(t, "KEY01", setCalls[0].Key)
		assert.Equal(t, uint64(2255), setCalls[0].Cas)
		assert.Equal(t, []byte("set data 01"), setCalls[0].Data)
	})

	t.Run("lease-set-without-lease-get--do-nothing", func(t *testing.T) {
		p := newPipelineTest(t)

		p.stubLeaseSet1(memproxy.LeaseSetResponse{}, nil)

		setFn := p.pipe.LeaseSet("KEY01", []byte("set data 01"), 2255, memproxy.LeaseSetOptions{})
		setResp, err := setFn()

		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseSetResponse{}, setResp)

		setCalls := p.pipe1.LeaseSetCalls()
		assert.Equal(t, 0, len(setCalls))
	})
}
