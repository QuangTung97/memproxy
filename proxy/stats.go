package proxy

import (
	"fmt"
	mcstats "github.com/QuangTung97/go-memcache/memcache/stats"
	"sync"
	"sync/atomic"
	"time"
)

//go:generate moq -rm -out stats_mocks_test.go . StatsClient

type serverStatus struct {
	memory atomic.Uint64
	failed atomic.Bool
}

// SimpleServerStats ...
type SimpleServerStats struct {
	wg sync.WaitGroup

	//revive:disable-next-line:nested-structs
	clientSignals map[ServerID]chan struct{}

	statuses map[ServerID]*serverStatus

	newClientFunc func(server ServerID) (StatsClient, error)
}

// StatsClient ...
type StatsClient interface {
	// GetMemUsage get memory usage in bytes
	GetMemUsage() (uint64, error)

	// Close client
	Close() error
}

// NewSimpleServerStats ...
func NewSimpleServerStats[S ServerConfig](
	servers []S,
	factory func(conf S) (StatsClient, error),
) (*SimpleServerStats, error) {
	clients := map[ServerID]StatsClient{}
	clientSignals := map[ServerID]chan struct{}{}
	statuses := map[ServerID]*serverStatus{}
	confMap := map[ServerID]S{}

	for _, server := range servers {
		confMap[server.GetID()] = server

		client, err := factory(server)
		if err != nil {
			// TODO Close
			return nil, err
		}

		clients[server.GetID()] = client
		clientSignals[server.GetID()] = make(chan struct{}, 10)
		statuses[server.GetID()] = &serverStatus{}
	}

	s := &SimpleServerStats{
		clientSignals: clientSignals,
		statuses:      statuses,
		newClientFunc: func(server ServerID) (StatsClient, error) {
			return factory(confMap[server])
		},
	}

	for _, server := range servers {
		client := clients[server.GetID()]
		clients[server.GetID()] = s.clientGetMemory(server.GetID(), client)
	}

	s.wg.Add(len(servers))

	for _, server := range servers {
		serverID := server.GetID()

		client := clients[serverID]
		ch := clientSignals[serverID]

		go func() {
			defer s.wg.Done()

			s.handleClient(serverID, client, ch)
		}()
	}

	return s, nil
}

func (s *SimpleServerStats) clientGetMemory(server ServerID, client StatsClient) StatsClient {
	status := s.statuses[server]

	mem, err := client.GetMemUsage()
	if err != nil {
		// TODO log error
		status.failed.Store(true)

		_ = client.Close()
		newClient, err := s.newClientFunc(server)
		if err != nil {
			// TODO log error
			return client
		}
		return newClient
	}
	status.failed.Store(false)
	status.memory.Store(mem)
	return client
}

func (s *SimpleServerStats) handleClient(server ServerID, client StatsClient, signal <-chan struct{}) {
	for {
		select {
		case _, ok := <-signal:
			if !ok {
				_ = client.Close()
				return
			}
			client = s.clientGetMemory(server, client)

		case <-time.After(30 * time.Second): // TODO Config
			client = s.clientGetMemory(server, client)
		}
	}
}

// IsServerFailed check whether the server is currently not connected
func (s *SimpleServerStats) IsServerFailed(server ServerID) bool {
	return s.statuses[server].failed.Load()
}

// NotifyServerFailed ...
func (s *SimpleServerStats) NotifyServerFailed(server ServerID) {
	status := s.statuses[server]
	status.failed.Store(true)

	ch := s.clientSignals[server]
	select {
	case ch <- struct{}{}:
	default:
	}
}

// GetMemUsage returns memory usage in bytes
func (s *SimpleServerStats) GetMemUsage(server ServerID) float64 {
	status := s.statuses[server]
	return float64(status.memory.Load())
}

// Shutdown ...
func (s *SimpleServerStats) Shutdown() {
	for _, ch := range s.clientSignals {
		close(ch)
	}
	s.wg.Wait()
}

type simpleStatsClient struct {
	client *mcstats.Client
}

// NewSimpleStatsClient ...
func NewSimpleStatsClient(conf SimpleServerConfig) (StatsClient, error) {
	client, err := mcstats.New(fmt.Sprintf("%s:%d", conf.Host, conf.Port))
	if err != nil {
		return nil, err
	}
	return &simpleStatsClient{
		client: client,
	}, nil
}

// GetMemUsage ...
func (s *simpleStatsClient) GetMemUsage() (uint64, error) {
	slabs, err := s.client.GetSlabsStats()
	if err != nil {
		return 0, err
	}
	return slabs.TotalMalloced, nil
}

// Close ...
func (s *simpleStatsClient) Close() error {
	return s.client.Close()
}
