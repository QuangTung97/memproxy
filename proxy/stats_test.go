package proxy

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestSimpleStatsClient(t *testing.T) {
	client := NewSimpleStatsClient(SimpleServerConfig{
		Host: "localhost",
		Port: 11211,
	})
	fmt.Println(client.GetMemUsage())

	assert.Equal(t, nil, client.Close())
}

type serverStatsTest struct {
	clients map[ServerID]*StatsClientMock
	stats   *SimpleServerStats

	mut     sync.Mutex
	newArgs []SimpleServerConfig

	newFunc func(conf SimpleServerConfig) StatsClient
}

func (s *serverStatsTest) getNewArgs() []SimpleServerConfig {
	s.mut.Lock()
	defer s.mut.Unlock()

	result := make([]SimpleServerConfig, len(s.newArgs))
	copy(result, s.newArgs)

	return result
}

func newServerStatsTest(t *testing.T, options ...SimpleStatsOption) *serverStatsTest {
	s := &serverStatsTest{}

	s.clients = map[ServerID]*StatsClientMock{
		serverID1: {
			CloseFunc: func() error { return nil },
		},
		serverID2: {
			CloseFunc: func() error { return nil },
		},
	}

	s.stubGetMem(serverID1, 8000, nil)
	s.stubGetMem(serverID2, 9000, nil)

	s.newFunc = func(conf SimpleServerConfig) StatsClient {
		return s.clients[conf.ID]
	}

	s.stats = NewSimpleServerStats[SimpleServerConfig]([]SimpleServerConfig{
		{
			ID:   serverID1,
			Host: "localhost",
			Port: 11201,
		},
		{
			ID:   serverID2,
			Host: "localhost",
			Port: 11202,
		},
	}, func(conf SimpleServerConfig) StatsClient {
		s.mut.Lock()
		s.newArgs = append(s.newArgs, conf)
		s.mut.Unlock()

		return s.newFunc(conf)
	}, options...)

	assert.Equal(t, []SimpleServerConfig{
		{
			ID:   serverID1,
			Host: "localhost",
			Port: 11201,
		},
		{
			ID:   serverID2,
			Host: "localhost",
			Port: 11202,
		},
	}, s.getNewArgs())

	return s
}

func (s *serverStatsTest) stubGetMem(serverID ServerID, mem uint64, err error) {
	s.clients[serverID].GetMemUsageFunc = func() (uint64, error) {
		return mem, err
	}
}

func TestSimpleServerStats(t *testing.T) {
	t.Run("get-mem", func(t *testing.T) {
		s := newServerStatsTest(t)
		defer s.stats.Shutdown()

		getCalls := s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 1, len(getCalls))

		assert.Equal(t, float64(8000), s.stats.GetMemUsage(serverID1))
		assert.Equal(t, float64(9000), s.stats.GetMemUsage(serverID2))

		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		s.stubGetMem(serverID1, 18000, nil)

		s.stats.NotifyServerFailed(serverID1)
		time.Sleep(40 * time.Millisecond)

		assert.Equal(t, float64(18000), s.stats.GetMemUsage(serverID1))

		getCalls = s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 2, len(getCalls))

		// Check Failed Again
		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))
	})

	t.Run("server-get-mem-error--is-server-failed", func(t *testing.T) {
		s := newServerStatsTest(t, WithSimpleStatsCheckDuration(200*time.Millisecond))

		assert.Equal(t, 2, len(s.getNewArgs()))

		getCalls := s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 1, len(getCalls))

		assert.Equal(t, float64(8000), s.stats.GetMemUsage(serverID1))
		assert.Equal(t, float64(9000), s.stats.GetMemUsage(serverID2))

		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		s.stubGetMem(serverID1, 0, errors.New("some error"))
		newClient := &StatsClientMock{
			CloseFunc: func() error { return nil },
			GetMemUsageFunc: func() (uint64, error) {
				return 888, nil
			},
		}
		s.newFunc = func(conf SimpleServerConfig) StatsClient {
			return newClient
		}

		s.stats.NotifyServerFailed(serverID1)
		time.Sleep(40 * time.Millisecond)

		assert.Equal(t, float64(8000), s.stats.GetMemUsage(serverID1))

		assert.Equal(t, true, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))
		assert.Equal(t, 2, len(s.getNewArgs()))

		// Notify Again But Nothing Is Called
		s.stats.NotifyServerFailed(serverID1)
		time.Sleep(40 * time.Millisecond)

		assert.Equal(t, 2, len(s.getNewArgs()))

		// Wait for Stats Check Duration
		time.Sleep(140 * time.Millisecond)

		assert.Equal(t, float64(888), s.stats.GetMemUsage(serverID1))

		// Check client calls
		getCalls = s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 2, len(getCalls))

		assert.Equal(t, 1, len(newClient.GetMemUsageCalls()))

		assert.Equal(t, 3, len(s.getNewArgs()))
		assert.Equal(t, SimpleServerConfig{
			ID:   serverID1,
			Host: "localhost",
			Port: 11201,
		}, s.getNewArgs()[2])

		// Check Failed Again
		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		// Check Call After Shutdown
		s.stats.Shutdown()

		assert.Equal(t, 1, len(s.clients[serverID1].CloseCalls()))
		assert.Equal(t, 1, len(s.clients[serverID2].CloseCalls()))
		assert.Equal(t, 1, len(newClient.CloseCalls()))
	})

	t.Run("server-get-mem-error--is-server-failed--with-options", func(t *testing.T) {
		s := newServerStatsTest(t,
			WithSimpleStatsCheckDuration(200*time.Millisecond),
			WithSimpleStatsErrorLogger(func(err error) {
				fmt.Println("Option Logger:", err)
			}),
			WithSimpleStatsMemLogger(func(server ServerID, mem uint64, err error) {
				fmt.Println("MEM USED:", server, mem, err)
			}),
		)

		getCalls := s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 1, len(getCalls))

		assert.Equal(t, float64(8000), s.stats.GetMemUsage(serverID1))
		assert.Equal(t, float64(9000), s.stats.GetMemUsage(serverID2))

		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		s.stubGetMem(serverID1, 0, errors.New("some error"))
		newClient := &StatsClientMock{
			CloseFunc: func() error { return nil },
			GetMemUsageFunc: func() (uint64, error) {
				return 888, nil
			},
		}
		s.newFunc = func(conf SimpleServerConfig) StatsClient {
			return newClient
		}

		s.stats.NotifyServerFailed(serverID1)
		time.Sleep(40 * time.Millisecond)

		assert.Equal(t, float64(8000), s.stats.GetMemUsage(serverID1))

		assert.Equal(t, true, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		// Wait for Stats Duration Timeout
		time.Sleep(180 * time.Millisecond)

		assert.Equal(t, float64(888), s.stats.GetMemUsage(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))

		// Check client calls
		getCalls = s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 2, len(getCalls))

		assert.Equal(t, 1, len(newClient.GetMemUsageCalls()))

		assert.Equal(t, 3, len(s.getNewArgs()))
		assert.Equal(t, SimpleServerConfig{
			ID:   serverID1,
			Host: "localhost",
			Port: 11201,
		}, s.getNewArgs()[2])

		assert.Equal(t, 1, len(newClient.GetMemUsageCalls()))

		// Notify Again
		s.stats.NotifyServerFailed(serverID1)
		time.Sleep(40 * time.Millisecond)

		assert.Equal(t, 2, len(newClient.GetMemUsageCalls()))

		// Check Failed Again
		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		// Check Call After Shutdown
		s.stats.Shutdown()

		assert.Equal(t, 1, len(s.clients[serverID1].CloseCalls()))
		assert.Equal(t, 1, len(s.clients[serverID2].CloseCalls()))
		assert.Equal(t, 1, len(newClient.CloseCalls()))
	})

	t.Run("multiple-time-out-happened", func(t *testing.T) {
		s := newServerStatsTest(t, WithSimpleStatsCheckDuration(100*time.Millisecond))

		assert.Equal(t, 2, len(s.getNewArgs()))

		getCalls := s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 1, len(getCalls))

		assert.Equal(t, float64(8000), s.stats.GetMemUsage(serverID1))
		assert.Equal(t, float64(9000), s.stats.GetMemUsage(serverID2))

		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		// Wait for Timeout
		time.Sleep(110 * time.Millisecond)

		getCalls = s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 2, len(getCalls))

		// Wait for Timeout
		time.Sleep(110 * time.Millisecond)

		getCalls = s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 3, len(getCalls))

		// Check Call After Shutdown
		s.stats.Shutdown()

		assert.Equal(t, 1, len(s.clients[serverID1].CloseCalls()))
		assert.Equal(t, 1, len(s.clients[serverID2].CloseCalls()))
	})
}
