package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
	"github.com/QuangTung97/memproxy/mocks"
	"github.com/QuangTung97/memproxy/proxy"
)

type itemTest struct {
	pipe1 *mocks.PipelineMock
	pipe2 *mocks.PipelineMock

	randFunc func(n uint64) uint64

	fillCalls []userKey
	stats     *ServerStatsMock

	it *item.Item[userValue, userKey]

	actions []string
}

type userValue struct {
	Tenant string `json:"tenant"`
	Name   string `json:"name"`
	Age    int64  `json:"age"`
}

type userKey struct {
	Tenant string
	Name   string
}

func (u userValue) GetKey() userKey {
	return userKey{
		Tenant: u.Tenant,
		Name:   u.Name,
	}
}

func (u userValue) Marshal() ([]byte, error) {
	return json.Marshal(u)
}

func unmarshalUser(data []byte) (userValue, error) {
	var user userValue
	err := json.Unmarshal(data, &user)
	return user, err
}

func (k userKey) String() string {
	return k.Tenant + ":" + k.Name
}

const server1 = proxy.ServerID(11)
const server2 = proxy.ServerID(12)

func newItemTest() *itemTest {
	result := &itemTest{
		stats: &ServerStatsMock{},
	}

	result.stats.NotifyServerFailedFunc = func(server proxy.ServerID) {
		result.addAction("notify-server-failed: ", server)
	}
	return result
}

func (i *itemTest) initItem() {
	mc1 := &mocks.MemcacheMock{}
	mc2 := &mocks.MemcacheMock{}

	i.pipe1 = &mocks.PipelineMock{}
	i.pipe2 = &mocks.PipelineMock{}

	mc1.PipelineFunc = func(ctx context.Context, options ...memproxy.PipelineOption) memproxy.Pipeline {
		i.addAction("pipeline 1")
		return i.pipe1
	}
	mc2.PipelineFunc = func(ctx context.Context, options ...memproxy.PipelineOption) memproxy.Pipeline {
		i.addAction("pipeline 2")
		return i.pipe2
	}

	i.pipe1.ExecuteFunc = func() {
		i.addAction("execute 1")
	}
	i.pipe2.ExecuteFunc = func() {
		i.addAction("execute 2")
	}

	mcMap := map[proxy.ServerID]memproxy.Memcache{
		server1: mc1,
		server2: mc2,
	}

	servers := []proxy.SimpleServerConfig{
		{ID: server1, Host: "localhost1"},
		{ID: server2, Host: "localhost2"},
	}

	mc, err := proxy.New[proxy.SimpleServerConfig](
		proxy.Config[proxy.SimpleServerConfig]{
			Servers: servers,
			Route: proxy.NewReplicatedRoute(
				[]proxy.ServerID{server1, server2},
				i.stats,
				proxy.WithRandFunc(func(n uint64) uint64 {
					return i.randFunc(n)
				}),
			),
		},
		func(conf proxy.SimpleServerConfig) memproxy.Memcache {
			return mcMap[conf.ID]
		},
	)
	if err != nil {
		panic(err)
	}

	age := 100

	i.it = item.New[userValue, userKey](
		mc.Pipeline(context.Background()),
		unmarshalUser,
		func(ctx context.Context, key userKey) func() (userValue, error) {
			i.addAction("fill: ", key.String())
			return func() (userValue, error) {
				i.addAction("fill-func: ", key.String())

				i.fillCalls = append(i.fillCalls, key)
				age++
				return userValue{
					Tenant: key.Tenant,
					Name:   key.Name,
					Age:    int64(age),
				}, nil
			}
		},
	)
}

func (i *itemTest) stubServersFailStatus(
	healthyServers []proxy.ServerID,
	failedServers []proxy.ServerID,
) {
	i.stats.IsServerFailedFunc = func(server proxy.ServerID) bool {
		for _, s := range healthyServers {
			if s == server {
				return false
			}
		}
		for _, s := range failedServers {
			if s == server {
				return true
			}
		}
		panic(fmt.Sprint("not found server:", server))
	}
}

func (i *itemTest) stubServerMem(servers map[proxy.ServerID]float64) {
	i.stats.GetMemUsageFunc = func(server proxy.ServerID) float64 {
		mem, ok := servers[server]
		if !ok {
			panic(fmt.Sprint("not found server:", server))
		}
		return mem
	}
}

func (i *itemTest) stubRand(r uint64) {
	i.randFunc = func(n uint64) uint64 {
		i.addAction("rand-func")
		return r
	}
}

func (i *itemTest) stubLeaseGet(
	pipe *mocks.PipelineMock,
	resp memproxy.LeaseGetResponse,
	err error,
) {
	pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) memproxy.LeaseGetResult {
		i.addAction("lease-get: ", key)
		return memproxy.LeaseGetResultFunc(func() (memproxy.LeaseGetResponse, error) {
			i.addAction("lease-get-func: ", key)
			return resp, err
		})
	}
}

func (i *itemTest) stubLeaseGetMulti(
	pipe *mocks.PipelineMock,
	respList ...memproxy.LeaseGetResponse,
) {
	pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) memproxy.LeaseGetResult {
		index := len(pipe.LeaseGetCalls()) - 1
		i.addAction("lease-get: ", key)
		return memproxy.LeaseGetResultFunc(func() (memproxy.LeaseGetResponse, error) {
			i.addAction("lease-get-func: ", key)
			return respList[index], nil
		})
	}
}

func (i *itemTest) addAction(s string, args ...any) {
	var vals []any
	vals = append(vals, s)
	vals = append(vals, args...)

	i.actions = append(i.actions, fmt.Sprint(vals...))
}

func mustMarshalUser(u userValue) []byte {
	data, err := json.Marshal(u)
	if err != nil {
		panic(err)
	}
	return data
}

func TestItemProxy__SimpleGet(t *testing.T) {
	i := newItemTest()

	i.stubRand(proxy.RandomMaxValues / 3)
	i.stubServersFailStatus(
		[]proxy.ServerID{server1, server2},
		nil,
	)
	i.stubServerMem(map[proxy.ServerID]float64{
		server1: 200,
		server2: 200,
	})

	// Do Init
	i.initItem()

	i.stubLeaseGet(i.pipe1, memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusFound,
		Data: mustMarshalUser(userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}),
	}, nil)

	fn := i.it.Get(context.Background(), userKey{
		Tenant: "TENANT01",
		Name:   "USER01",
	})
	resp, err := fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, userValue{
		Tenant: "TENANT01",
		Name:   "USER01",
		Age:    88,
	}, resp)
}

func TestItemProxy__FailOver__LeaseGetRejected(t *testing.T) {
	i := newItemTest()

	i.stubRand(proxy.RandomMaxValues / 3)
	i.stubServersFailStatus(
		[]proxy.ServerID{server1, server2},
		nil,
	)
	i.stubServerMem(map[proxy.ServerID]float64{
		server1: 200,
		server2: 200,
	})

	// Do Init
	i.initItem()

	i.stubLeaseGet(i.pipe1, memproxy.LeaseGetResponse{}, errors.New("server down"))
	i.stubLeaseGetMulti(i.pipe2,
		memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseRejected,
			CAS:    2311,
		},
		memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			Data: mustMarshalUser(userValue{
				Tenant: "TENANT01",
				Name:   "USER01",
				Age:    81,
			}),
		},
	)

	fn := i.it.Get(context.Background(), userKey{
		Tenant: "TENANT01",
		Name:   "USER01",
	})
	resp, err := fn()
	assert.Equal(t, nil, err)
	assert.Equal(t, userValue{
		Tenant: "TENANT01",
		Name:   "USER01",
		Age:    81,
	}, resp)

	assert.Equal(t, []string{
		"rand-func",
		"pipeline 1",

		"lease-get: TENANT01:USER01",
		"execute 1",
		"lease-get-func: TENANT01:USER01",

		"notify-server-failed: 11",

		"rand-func",
		"pipeline 2",
		"lease-get: TENANT01:USER01",
		"execute 2",
		"lease-get-func: TENANT01:USER01",

		"rand-func",
		"lease-get: TENANT01:USER01",
		"execute 2",
		"lease-get-func: TENANT01:USER01",
	}, i.actions)
}

func TestItemProxy__FailOver__Filler__On_Multi_Keys(t *testing.T) {
	i := newItemTest()

	i.stubRand(proxy.RandomMaxValues / 3)
	i.stubServersFailStatus(
		[]proxy.ServerID{server1, server2},
		nil,
	)
	i.stubServerMem(map[proxy.ServerID]float64{
		server1: 200,
		server2: 200,
	})

	// Do Init
	i.initItem()

	firstResp := []memproxy.LeaseGetResponse{
		{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    544,
		},
		{},
	}
	firstErr := []error{
		nil,
		errors.New("server failed"),
	}

	i.pipe1.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) memproxy.LeaseGetResult {
		i.addAction("lease-get: ", key)
		index := len(i.pipe1.LeaseGetCalls()) - 1
		return memproxy.LeaseGetResultFunc(func() (memproxy.LeaseGetResponse, error) {
			i.addAction("lease-get-func: ", key)
			return firstResp[index], firstErr[index]
		})
	}

	i.pipe1.LeaseSetFunc = func(
		key string, data []byte, cas uint64, options memproxy.LeaseSetOptions,
	) func() (memproxy.LeaseSetResponse, error) {
		i.addAction("lease-set: ", key)
		return func() (memproxy.LeaseSetResponse, error) {
			i.addAction("lease-set-func: ", key)
			return memproxy.LeaseSetResponse{}, nil
		}
	}

	i.stubLeaseGetMulti(i.pipe2,
		memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseRejected,
			CAS:    2311,
		},
		memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			Data: mustMarshalUser(userValue{
				Tenant: "TENANT02",
				Name:   "USER02",
				Age:    82,
			}),
		},
	)

	fn1 := i.it.Get(context.Background(), userKey{
		Tenant: "TENANT01",
		Name:   "USER01",
	})
	fn2 := i.it.Get(context.Background(), userKey{
		Tenant: "TENANT02",
		Name:   "USER02",
	})

	resp, err := fn1()
	assert.Equal(t, nil, err)
	assert.Equal(t, userValue{
		Tenant: "TENANT01",
		Name:   "USER01",
		Age:    101,
	}, resp)

	resp, err = fn2()
	assert.Equal(t, nil, err)
	assert.Equal(t, userValue{
		Tenant: "TENANT02",
		Name:   "USER02",
		Age:    82,
	}, resp)

	assert.Equal(t, []string{
		"rand-func",
		"pipeline 1",

		"lease-get: TENANT01:USER01",
		"lease-get: TENANT02:USER02",
		"execute 1",
		"lease-get-func: TENANT01:USER01",
		"lease-get-func: TENANT02:USER02",

		"notify-server-failed: 11",

		"rand-func",
		"pipeline 2",

		"lease-get: TENANT02:USER02",
		"execute 2",
		"lease-get-func: TENANT02:USER02",

		"fill: TENANT01:USER01",
		"fill-func: TENANT01:USER01",

		"lease-set: TENANT01:USER01",
		"execute 1",

		"rand-func",
		"lease-get: TENANT02:USER02",
		"execute 2",
		"lease-get-func: TENANT02:USER02",
	}, i.actions)
}
