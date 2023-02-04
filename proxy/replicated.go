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

	// default 1%
	minPercent float64
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

// WithMinPercentage minimum request percentage to memcached servers
func WithMinPercentage(percentage float64) ReplicatedRouteOption {
	return func(conf *replicatedRouteConfig) {
		conf.minPercent = percentage
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
		minPercent: 1.0, // 1%
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
		s.route.stats.NotifyServerFailed(server)
	}
}

// HasNextAvailableServer check if next available server ready to be fallback to
func (s *replicatedRouteSelector) HasNextAvailableServer() bool {
	return len(s.failedServers) < len(s.route.configServers)
}

func (s *replicatedRouteSelector) computeRemainingServers() []ServerID {
	remainingServers := make([]ServerID, 0, len(s.route.configServers))
	for _, server := range s.route.configServers {
		if s.route.stats.IsServerFailed(server) {
			s.failedServers[server] = struct{}{}
			continue
		}

		if s.failedServers != nil {
			_, existed := s.failedServers[server]
			if existed {
				continue
			}
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

	for _, server := range s.remainingServers {
		w := s.route.conf.memScore(s.route.stats.GetMemUsage(server))
		// current not accumulated
		s.weightAccum = append(s.weightAccum, w)
	}

	randVal := s.route.conf.randFunc(randomMaxValues)

	index, weights := computeChosenServer(s.weightAccum, s.route.conf.minPercent, randVal)
	s.weightAccum = weights

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

func computeWeightAccumWithMinPercent(
	weights []float64, minPercent float64,
) []float64 {
	sum := 0.0
	for i, w := range weights {
		if w < 1.0 {
			weights[i] = 1.0
			w = 1.0
		}
		sum += w
	}

	belowMinCount := 0
	belowMinWeightSum := float64(0)
	minWeight := minPercent * sum / 100.0

	for _, w := range weights {
		if w < minWeight {
			belowMinWeightSum += w
			belowMinCount++
		}
	}

	ratio := 100.0 / minPercent / float64(belowMinCount)
	newMinWeight := (sum - belowMinWeightSum) / (ratio - 1.0)
	for index, w := range weights {
		if w < newMinWeight {
			weights[index] = newMinWeight
		}
	}

	for i := 1; i < len(weights); i++ {
		weights[i] = weights[i] + weights[i-1]
	}
	return weights
}

func computeChosenServer(
	weights []float64,
	minPercent float64,
	randVal uint64,
) (int, []float64) {
	weights = computeWeightAccumWithMinPercent(weights, minPercent)
	sum := weights[len(weights)-1]

	chosenWeight := float64(randVal) / float64(randomMaxValues) * sum

	for i, w := range weights {
		if chosenWeight < w {
			return i, weights
		}
	}
	return 0, weights
}
