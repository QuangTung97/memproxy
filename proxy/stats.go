package proxy

import (
	"fmt"
	mcstats "github.com/QuangTung97/go-memcache/memcache/stats"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

//go:generate moq -rm -out stats_mocks_test.go . StatsClient

type serverStatus struct {
	memory atomic.Uint64
	failed atomic.Bool
}

type simpleStatsConfig struct {
	errorLogger   func(err error)
	memLogger     func(server ServerID, mem uint64, err error)
	checkDuration time.Duration
}

// SimpleStatsOption ...
type SimpleStatsOption func(conf *simpleStatsConfig)

// WithSimpleStatsErrorLogger ...
func WithSimpleStatsErrorLogger(logger func(err error)) SimpleStatsOption {
	return func(conf *simpleStatsConfig) {
		conf.errorLogger = logger
	}
}

// WithSimpleStatsMemLogger ...
func WithSimpleStatsMemLogger(memLogger func(server ServerID, mem uint64, err error)) SimpleStatsOption {
	return func(conf *simpleStatsConfig) {
		conf.memLogger = memLogger
	}
}

// WithSimpleStatsCheckDuration ...
func WithSimpleStatsCheckDuration(d time.Duration) SimpleStatsOption {
	return func(conf *simpleStatsConfig) {
		conf.checkDuration = d
	}
}

func computeSimpleStatsConfig(options ...SimpleStatsOption) *simpleStatsConfig {
	conf := &simpleStatsConfig{
		errorLogger: func(err error) {
			log.Println("[ERROR] SimpleServerStats:", err)
		},
		memLogger: func(server ServerID, mem uint64, err error) {
		},
		checkDuration: 30 * time.Second,
	}
	for _, option := range options {
		option(conf)
	}
	return conf
}

// SimpleServerStats ...
type SimpleServerStats struct {
	conf *simpleStatsConfig

	wg sync.WaitGroup

	//revive:disable-next-line:nested-structs
	clientSignals map[ServerID]chan struct{}

	statuses map[ServerID]*serverStatus

	newClientFunc func(server ServerID) StatsClient
}

// StatsClient ...
type StatsClient interface {
	// GetMemUsage get memory usage in bytes
	GetMemUsage() (uint64, error)

	// Close client
	Close() error
}

const signalChanSize = 128

// NewSimpleServerStats ...
func NewSimpleServerStats[S ServerConfig](
	servers []S,
	factory func(conf S) StatsClient,
	options ...SimpleStatsOption,
) *SimpleServerStats {
	conf := computeSimpleStatsConfig(options...)

	clients := map[ServerID]StatsClient{}
	clientSignals := map[ServerID]chan struct{}{}
	statuses := map[ServerID]*serverStatus{}
	confMap := map[ServerID]S{}

	for _, server := range servers {
		confMap[server.GetID()] = server

		client := factory(server)

		clients[server.GetID()] = client
		clientSignals[server.GetID()] = make(chan struct{}, signalChanSize)
		statuses[server.GetID()] = &serverStatus{}
	}

	s := &SimpleServerStats{
		conf: conf,

		clientSignals: clientSignals,
		statuses:      statuses,
		newClientFunc: func(server ServerID) StatsClient {
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

	return s
}

func (s *SimpleServerStats) clientGetMemory(server ServerID, client StatsClient) StatsClient {
	status := s.statuses[server]

	if status.failed.Load() {
		_ = client.Close()
		client = s.newClientFunc(server)
	}

	mem, err := client.GetMemUsage()
	s.conf.memLogger(server, mem, err)
	if err != nil {
		s.conf.errorLogger(err)
		status.failed.Store(true)
		return client
	}
	status.failed.Store(false)
	status.memory.Store(mem)
	return client
}

func drainSignal(signal <-chan struct{}) {
	for i := 0; i < signalChanSize-1; i++ {
		select {
		case <-signal:
		default:
		}
	}
}

func (s *SimpleServerStats) handleClient(server ServerID, client StatsClient, signal <-chan struct{}) {
	alreadySignaled := false
	timeAfter := time.After(s.conf.checkDuration)

	for {
		select {
		case _, ok := <-signal:
			if !ok {
				_ = client.Close()
				return
			}
			drainSignal(signal)

			if alreadySignaled {
				continue
			}
			alreadySignaled = true
			timeAfter = time.After(s.conf.checkDuration)

			client = s.clientGetMemory(server, client)

		case <-timeAfter:
			client = s.clientGetMemory(server, client)
			alreadySignaled = false
			timeAfter = time.After(s.conf.checkDuration)
		}
	}
}

// IsServerFailed check whether the server is currently not connected
func (s *SimpleServerStats) IsServerFailed(server ServerID) bool {
	return s.statuses[server].failed.Load()
}

// NotifyServerFailed ...
func (s *SimpleServerStats) NotifyServerFailed(server ServerID) {
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
func NewSimpleStatsClient(conf SimpleServerConfig) StatsClient {
	client := mcstats.New(fmt.Sprintf("%s:%d", conf.Host, conf.Port))
	return &simpleStatsClient{
		client: client,
	}
}

// GetMemUsage ...
func (s *simpleStatsClient) GetMemUsage() (uint64, error) {
	slabs, err := s.client.GetSlabsStats()
	if err != nil {
		return 0, err
	}

	mem := uint64(0)
	for _, slabID := range slabs.SlabIDs {
		slab := slabs.Slabs[slabID]
		mem += uint64(slab.ChunkSize) * slab.UsedChunks
	}

	return mem, nil
}

// Close ...
func (s *simpleStatsClient) Close() error {
	return s.client.Close()
}
