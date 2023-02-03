package proxy

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSimpleStatsClient(t *testing.T) {
	client, err := NewSimpleStatsClient(SimpleServerConfig{
		Host: "localhost",
		Port: 11211,
	})
	assert.Equal(t, nil, err)
	fmt.Println(client.GetMemUsage())

	assert.Equal(t, nil, client.Close())
}

type serverStatsTest struct {
	clients map[ServerID]*StatsClientMock
	stats   *SimpleServerStats

	newArgs []SimpleServerConfig
	newFunc func(conf SimpleServerConfig) (StatsClient, error)
}

func newServerStatsTest(t *testing.T) *serverStatsTest {
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

	s.newFunc = func(conf SimpleServerConfig) (StatsClient, error) {
		return s.clients[conf.ID], nil
	}

	var err error
	s.stats, err = NewSimpleServerStats[SimpleServerConfig]([]SimpleServerConfig{
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
	}, func(conf SimpleServerConfig) (StatsClient, error) {
		s.newArgs = append(s.newArgs, conf)
		return s.newFunc(conf)
	})

	assert.Equal(t, nil, err)

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
	}, s.newArgs)

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
		time.Sleep(10 * time.Millisecond)

		assert.Equal(t, float64(18000), s.stats.GetMemUsage(serverID1))

		getCalls = s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 2, len(getCalls))

		// Check Failed Again
		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))
	})

	t.Run("server-get-mem-error--is-server-failed", func(t *testing.T) {
		s := newServerStatsTest(t)

		getCalls := s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 1, len(getCalls))

		assert.Equal(t, float64(8000), s.stats.GetMemUsage(serverID1))
		assert.Equal(t, float64(9000), s.stats.GetMemUsage(serverID2))

		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		s.stubGetMem(serverID1, 0, errors.New("some error"))
		newClient := &StatsClientMock{
			CloseFunc: func() error { return nil },
		}
		s.newFunc = func(conf SimpleServerConfig) (StatsClient, error) {
			return newClient, nil
		}

		s.stats.NotifyServerFailed(serverID1)
		time.Sleep(10 * time.Millisecond)

		assert.Equal(t, float64(8000), s.stats.GetMemUsage(serverID1))

		// Check client calls
		getCalls = s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 2, len(getCalls))

		assert.Equal(t, 3, len(s.newArgs))
		assert.Equal(t, SimpleServerConfig{
			ID:   serverID1,
			Host: "localhost",
			Port: 11201,
		}, s.newArgs[2])

		// Check Failed Again
		assert.Equal(t, true, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		// Check Call After Shutdown
		s.stats.Shutdown()

		assert.Equal(t, 1, len(s.clients[serverID1].CloseCalls()))
		assert.Equal(t, 1, len(s.clients[serverID2].CloseCalls()))
		assert.Equal(t, 1, len(newClient.CloseCalls()))
	})

	t.Run("server-get-mem-error--do-close-and-new-client--but-returns-error", func(t *testing.T) {
		s := newServerStatsTest(t)

		getCalls := s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 1, len(getCalls))

		assert.Equal(t, float64(8000), s.stats.GetMemUsage(serverID1))
		assert.Equal(t, float64(9000), s.stats.GetMemUsage(serverID2))

		assert.Equal(t, false, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		s.stubGetMem(serverID1, 0, errors.New("some error"))
		s.newFunc = func(conf SimpleServerConfig) (StatsClient, error) {
			return nil, errors.New("new error")
		}

		s.stats.NotifyServerFailed(serverID1)
		time.Sleep(10 * time.Millisecond)

		assert.Equal(t, float64(8000), s.stats.GetMemUsage(serverID1))

		// Check client calls
		getCalls = s.clients[serverID1].GetMemUsageCalls()
		assert.Equal(t, 2, len(getCalls))

		assert.Equal(t, 3, len(s.newArgs))
		assert.Equal(t, SimpleServerConfig{
			ID:   serverID1,
			Host: "localhost",
			Port: 11201,
		}, s.newArgs[2])

		// Check Failed Again
		assert.Equal(t, true, s.stats.IsServerFailed(serverID1))
		assert.Equal(t, false, s.stats.IsServerFailed(serverID2))

		// Check Call After Shutdown
		s.stats.Shutdown()

		assert.Equal(t, 2, len(s.clients[serverID1].CloseCalls()))
		assert.Equal(t, 1, len(s.clients[serverID2].CloseCalls()))
	})
}
