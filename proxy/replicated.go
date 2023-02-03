package proxy

import (
	"math/rand"
)

type replicatedRoute struct {
	configServers []ServerID

	conf  *replicatedRouteConfig
	stats ServerStats
}

// random from 0 => 999,999
const randomMaxValues uint64 = 1000000

type replicatedRouteConfig struct {
	// compute score from memory
	memScore func(mem float64) float64

	// random from 0 => n - 1
	randFunc func(n uint64) uint64
}

type replicatedRouteSelector struct {
	//revive:disable-next-line:nested-structs
	failedServers map[ServerID]struct{}

	remainingServers []ServerID

	route       *replicatedRoute
	weightAccum []float64

	alreadyChosen bool
	chosenServer  ServerID
}

var _ Route = &replicatedRoute{}

// ReplicatedRouteOption ...
type ReplicatedRouteOption func(conf *replicatedRouteConfig)

// WithRandFunc ...
func WithRandFunc(randFunc func(n uint64) uint64) ReplicatedRouteOption {
	return func(conf *replicatedRouteConfig) {
		conf.randFunc = randFunc
	}
}

// NewReplicatedRoute ...
func NewReplicatedRoute(
	servers []ServerID,
	stats ServerStats,
	options ...ReplicatedRouteOption,
) Route {
	conf := &replicatedRouteConfig{
		memScore: func(mem float64) float64 {
			return mem
		},
		randFunc: func(n uint64) uint64 {
			return uint64(rand.Intn(int(n)))
		},
	}

	for _, opt := range options {
		opt(conf)
	}

	return &replicatedRoute{
		configServers: servers,

		conf:  conf,
		stats: stats,
	}
}

// NewSelector ...
func (r *replicatedRoute) NewSelector() Selector {
	s := &replicatedRouteSelector{
		route: r,
	}
	s.remainingServers = s.computeRemainingServers()
	return s
}

// SetFailedServer ...
func (s *replicatedRouteSelector) SetFailedServer(server ServerID) {
	if s.failedServers == nil {
		s.failedServers = map[ServerID]struct{}{}
	}

	_, existed := s.failedServers[server]
	s.failedServers[server] = struct{}{}

	if !existed {
		s.Reset()
		s.remainingServers = s.computeRemainingServers()
	}
}

// HasNextAvailableServer check if next available server ready to be fallback to
func (s *replicatedRouteSelector) HasNextAvailableServer() bool {
	return len(s.failedServers) < len(s.route.configServers)
}

func (s *replicatedRouteSelector) computeRemainingServers() []ServerID {
	if s.failedServers == nil {
		return s.route.configServers
	}

	remainingServers := make([]ServerID, 0, len(s.route.configServers))
	for _, server := range s.route.configServers {
		_, existed := s.failedServers[server]
		if existed {
			continue
		}
		remainingServers = append(remainingServers, server)
	}

	if len(remainingServers) == 0 {
		return s.route.configServers
	}
	return remainingServers
}

// SelectServer choose a server id, will keep in this server id unless Reset is call or failed server added
func (s *replicatedRouteSelector) SelectServer(string) ServerID {
	if s.alreadyChosen {
		return s.chosenServer
	}

	sum := float64(0)

	for _, server := range s.remainingServers {
		w := s.route.conf.memScore(s.route.stats.GetMemUsage(server))
		sum += w

		s.weightAccum = append(s.weightAccum, sum)
	}

	n := s.route.conf.randFunc(randomMaxValues)

	chosenWeight := float64(n) / float64(randomMaxValues) * sum

	index := 0
	for i, w := range s.weightAccum {
		if chosenWeight < w {
			index = i
			break
		}
	}

	s.alreadyChosen = true
	s.chosenServer = s.remainingServers[index]
	return s.chosenServer
}

// SelectForDelete choose servers for deleting
func (s *replicatedRouteSelector) SelectForDelete(string) []ServerID {
	return s.remainingServers
}

// Reset the selection
func (s *replicatedRouteSelector) Reset() {
	s.alreadyChosen = false
	s.weightAccum = s.weightAccum[:0]
}
