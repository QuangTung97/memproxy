package proxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

type pipelineTest struct {
	mc1 *mocks.MemcacheMock
	mc2 *mocks.MemcacheMock

	pipe1 *mocks.PipelineMock
	pipe2 *mocks.PipelineMock

	client   memproxy.Memcache
	route    *RouteMock
	selector *SelectorMock
	pipe     memproxy.Pipeline

	actions []string
}

func leaseGetAction(key string) string {
	return fmt.Sprintf("lease-get: %s", key)
}

func leaseGetFuncAction(key string) string {
	return fmt.Sprintf("lease-get-func: %s", key)
}

func pipelineExecuteAction(server ServerID) string {
	return fmt.Sprintf("pipe-exec: %d", server)
}

func leaseSetAction(key string) string {
	return fmt.Sprintf("lease-set: %s", key)
}

func leaseSetFuncAction(key string) string {
	return fmt.Sprintf("lease-set-func: %s", key)
}

func (p *pipelineTest) appendAction(s string) {
	p.actions = append(p.actions, s)
}

const serverID1 ServerID = 31
const serverID2 ServerID = 32

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

	p.pipe1 = &mocks.PipelineMock{
		ExecuteFunc: func() {
			p.appendAction(pipelineExecuteAction(serverID1))
		},
	}
	p.pipe2 = &mocks.PipelineMock{
		ExecuteFunc: func() {
			p.appendAction(pipelineExecuteAction(serverID2))
		},
	}

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

	selector := &SelectorMock{}
	selector.SetFailedServerFunc = func(server ServerID) {
	}
	selector.ResetFunc = func() {
	}

	route := &RouteMock{}
	route.NewSelectorFunc = func() Selector {
		return selector
	}

	mc, err := New[SimpleServerConfig](Config[SimpleServerConfig]{
		Servers: []SimpleServerConfig{server1, server2},
		Route:   route,
	}, func(conf SimpleServerConfig) memproxy.Memcache {
		index := len(newCalls)
		newCalls = append(newCalls, conf)
		return []memproxy.Memcache{
			mc1,
			mc2,
		}[index]
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, []SimpleServerConfig{server1, server2}, newCalls)

	sessProvider := memproxy.NewSessionProvider()

	p.client = mc
	p.route = route
	p.selector = selector
	p.pipe = p.client.Pipeline(newContext(), sessProvider.New())

	return p
}

func (p *pipelineTest) stubSelect(idList ...ServerID) {
	p.selector.SelectServerFunc = func(key string) ServerID {
		index := len(p.selector.SelectServerCalls()) - 1
		return idList[index]
	}
}

func (p *pipelineTest) stubHasNextAvail(hasNext bool) {
	p.selector.HasNextAvailableServerFunc = func() bool {
		return hasNext
	}
}

func (p *pipelineTest) stubPipeLeaseGet(pipe *mocks.PipelineMock, resp memproxy.LeaseGetResponse, err error) {
	pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		p.appendAction(leaseGetAction(key))
		return func() (memproxy.LeaseGetResponse, error) {
			p.appendAction(leaseGetFuncAction(key))
			return resp, err
		}
	}
}

func (p *pipelineTest) stubLeaseGet1(resp memproxy.LeaseGetResponse, err error) {
	p.stubPipeLeaseGet(p.pipe1, resp, err)
}

func (p *pipelineTest) stubLeaseGet2(resp memproxy.LeaseGetResponse, err error) {
	p.stubPipeLeaseGet(p.pipe2, resp, err)
}

func (p *pipelineTest) stubPipeLeaseSet(
	pipe *mocks.PipelineMock,
	resp memproxy.LeaseSetResponse, err error,
) {
	pipe.LeaseSetFunc = func(
		key string, data []byte, cas uint64, options memproxy.LeaseSetOptions,
	) func() (memproxy.LeaseSetResponse, error) {
		p.appendAction(leaseSetAction(key))
		return func() (memproxy.LeaseSetResponse, error) {
			p.appendAction(leaseSetFuncAction(key))
			return resp, err
		}
	}
}

func (p *pipelineTest) stubLeaseSet1(resp memproxy.LeaseSetResponse, err error) {
	p.stubPipeLeaseSet(p.pipe1, resp, err)
}

func (p *pipelineTest) stubLeaseSet2(resp memproxy.LeaseSetResponse, err error) {
	p.stubPipeLeaseSet(p.pipe2, resp, err)
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

		calls := p.selector.SelectServerCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "KEY01", calls[0].Key)

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

		assert.Equal(t, 1, len(p.selector.ResetCalls()))
		assert.Equal(t, 1, len(p.pipe1.ExecuteCalls()))
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

		p.stubHasNextAvail(true)

		fn := p.pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    443,
			Data:   []byte("found 01"),
		}, resp)

		// Check Init New Pipeline
		calls := p.selector.SelectServerCalls()
		assert.Equal(t, 2, len(calls))
		assert.Equal(t, "KEY01", calls[0].Key)
		assert.Equal(t, "KEY01", calls[1].Key)

		pipeCalls := p.mc1.PipelineCalls()
		assert.Equal(t, 1, len(pipeCalls))
		assert.Equal(t, newContext(), pipeCalls[0].Ctx)

		// Do Call Set Server Failed
		setCalls := p.selector.SetFailedServerCalls()
		assert.Equal(t, 1, len(setCalls))
		assert.Equal(t, serverID1, setCalls[0].Server)
	})

	t.Run("lease_get_with_error__has_next_false__not_retry_on_other_server", func(t *testing.T) {
		p := newPipelineTest(t)

		p.stubSelect(
			serverID1,
			serverID2,
		)

		getError := errors.New("some error")
		p.stubLeaseGet1(memproxy.LeaseGetResponse{}, getError)

		p.stubHasNextAvail(false)

		fn := p.pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp, err := fn()

		assert.Equal(t, getError, err)
		assert.Equal(t, memproxy.LeaseGetResponse{}, resp)

		// Check Init New Pipeline
		calls := p.selector.SelectServerCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "KEY01", calls[0].Key)

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

	t.Run("lease-get-with-fallback-on-error--then-set", func(t *testing.T) {
		p := newPipelineTest(t)

		p.stubSelect(serverID1, serverID2)

		getError := errors.New("some error")
		p.stubLeaseGet1(memproxy.LeaseGetResponse{}, getError)
		p.stubLeaseGet2(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2255,
		}, nil)

		p.stubHasNextAvail(true)

		fn := p.pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2255,
		}, resp)

		// Do Lease Set
		p.stubLeaseSet2(memproxy.LeaseSetResponse{}, nil)

		setFn := p.pipe.LeaseSet("KEY01", []byte("set data 01"), 2255, memproxy.LeaseSetOptions{})
		setResp, err := setFn()

		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseSetResponse{}, setResp)

		setCalls := p.pipe2.LeaseSetCalls()
		assert.Equal(t, 1, len(setCalls))
		assert.Equal(t, "KEY01", setCalls[0].Key)
		assert.Equal(t, uint64(2255), setCalls[0].Cas)
		assert.Equal(t, []byte("set data 01"), setCalls[0].Data)

		// Check Action
		assert.Equal(t, []string{
			leaseGetAction("KEY01"),
			pipelineExecuteAction(serverID1),
			leaseGetFuncAction("KEY01"),

			leaseGetAction("KEY01"),
			pipelineExecuteAction(serverID2),
			leaseGetFuncAction("KEY01"),

			leaseSetAction("KEY01"),
			leaseSetFuncAction("KEY01"),
		}, p.actions)
	})
}

func TestPipeline__LeaseGet_Multi(t *testing.T) {
	t.Run("get-multi-and-execute-multi", func(t *testing.T) {
		p := newPipelineTest(t)

		p.stubSelect(serverID1, serverID2)

		p.stubLeaseGet1(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2255,
		}, nil)
		p.stubLeaseGet2(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2266,
		}, nil)

		fn1 := p.pipe.LeaseGet("KEY01", memproxy.LeaseGetOptions{})
		fn2 := p.pipe.LeaseGet("KEY02", memproxy.LeaseGetOptions{})

		resp, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2255,
		}, resp)

		resp, err = fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    2266,
		}, resp)

		selectCalls := p.selector.SelectServerCalls()
		assert.Equal(t, 2, len(selectCalls))
		assert.Equal(t, "KEY01", selectCalls[0].Key)
		assert.Equal(t, "KEY02", selectCalls[1].Key)

		assert.Equal(t, []string{
			leaseGetAction("KEY01"),
			leaseGetAction("KEY02"),
			pipelineExecuteAction(serverID1),
			pipelineExecuteAction(serverID2),
			leaseGetFuncAction("KEY01"),
			leaseGetFuncAction("KEY02"),
		}, p.actions)
	})
}
