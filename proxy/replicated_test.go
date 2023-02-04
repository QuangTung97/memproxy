package proxy

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type replicatedRouteTest struct {
	stats    *ServerStatsMock
	randFunc func(n uint64) uint64
	randArgs []uint64

	route    Route
	selector Selector
}

func newReplicatedRouteTest(options ...ReplicatedRouteOption) *replicatedRouteTest {
	r := &replicatedRouteTest{}

	r.stats = &ServerStatsMock{
		NotifyServerFailedFunc: func(server ServerID) {
		},
	}

	randFunc := func(n uint64) uint64 {
		r.randArgs = append(r.randArgs, n)
		return r.randFunc(n)
	}

	r.stubServerFailed(false)

	opts := []ReplicatedRouteOption{
		WithRandFunc(randFunc),
	}
	opts = append(opts, options...)

	r.route = NewReplicatedRoute(
		[]ServerID{
			serverID1,
			serverID2,
		},
		r.stats,
		opts...,
	)
	r.selector = r.route.NewSelector()

	return r
}

func (r *replicatedRouteTest) stubGetMem(values ...float64) {
	r.stats.GetMemUsageFunc = func(server ServerID) float64 {
		index := len(r.stats.GetMemUsageCalls()) - 1
		return values[index]
	}
}

func (r *replicatedRouteTest) stubRand(val uint64) {
	r.randFunc = func(n uint64) uint64 {
		return val
	}
}

func (r *replicatedRouteTest) stubServerFailed(failed bool) {
	r.stats.IsServerFailedFunc = func(server ServerID) bool {
		return failed
	}
}

func TestReplicatedRoute(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		r := newReplicatedRouteTest()

		r.stubGetMem(
			50, 50,
			50, 50,
		)

		r.stubRand(499000)
		assert.Equal(t, serverID1, r.selector.SelectServer(""))
		assert.Equal(t, true, r.selector.HasNextAvailableServer())

		r.stubRand(500000)

		// Get Again
		assert.Equal(t, serverID1, r.selector.SelectServer(""))

		// Get Again after Reset
		r.selector.Reset()
		assert.Equal(t, serverID2, r.selector.SelectServer(""))

		assert.Equal(t, []uint64{randomMaxValues, randomMaxValues}, r.randArgs)

		getMemCalls := r.stats.GetMemUsageCalls()
		assert.Equal(t, 4, len(getMemCalls))

		assert.Equal(t, serverID1, getMemCalls[0].Server)
		assert.Equal(t, serverID2, getMemCalls[1].Server)

		assert.Equal(t, serverID1, getMemCalls[2].Server)
		assert.Equal(t, serverID2, getMemCalls[3].Server)

		getFailedCalls := r.stats.IsServerFailedCalls()
		assert.Equal(t, 2, len(getFailedCalls))
		assert.Equal(t, serverID1, getFailedCalls[0].Server)
		assert.Equal(t, serverID2, getFailedCalls[1].Server)
	})

	t.Run("weight-is-changed-in-between", func(t *testing.T) {
		r := newReplicatedRouteTest()

		r.stubGetMem(
			50, 50,
			60, 40,
		)

		r.stubRand(499000)
		assert.Equal(t, serverID1, r.selector.SelectServer(""))

		r.stubRand(500000)

		// Get Again
		assert.Equal(t, serverID1, r.selector.SelectServer(""))

		// Get Again after Reset
		r.selector.Reset()
		assert.Equal(t, serverID1, r.selector.SelectServer(""))

		assert.Equal(t, []ServerID{serverID1, serverID2}, r.selector.SelectForDelete(""))
	})

	t.Run("set-failed-server--fallback-to-another", func(t *testing.T) {
		r := newReplicatedRouteTest()

		r.stubGetMem(
			50, 50,
			50, 50,
		)

		r.stubRand(499000)
		assert.Equal(t, serverID1, r.selector.SelectServer(""))

		r.selector.SetFailedServer(serverID1)

		r.stubRand(499000)
		assert.Equal(t, serverID2, r.selector.SelectServer(""))
		assert.Equal(t, true, r.selector.HasNextAvailableServer())

		assert.Equal(t, []ServerID{serverID2}, r.selector.SelectForDelete(""))

		assert.Equal(t, 1, len(r.stats.NotifyServerFailedCalls()))
		assert.Equal(t, serverID1, r.stats.NotifyServerFailedCalls()[0].Server)
	})

	t.Run("all-servers-failed--use-normal-random", func(t *testing.T) {
		r := newReplicatedRouteTest()

		r.stubGetMem(
			50, 50,
			50, 50,
		)

		r.stubRand(499000)
		assert.Equal(t, serverID1, r.selector.SelectServer(""))
		assert.Equal(t, true, r.selector.HasNextAvailableServer())

		r.selector.SetFailedServer(serverID1)
		r.selector.SetFailedServer(serverID2)

		r.stubRand(499000)
		assert.Equal(t, serverID1, r.selector.SelectServer(""))
		assert.Equal(t, false, r.selector.HasNextAvailableServer())

		assert.Equal(t, []ServerID{serverID1, serverID2}, r.selector.SelectForDelete(""))
	})

	t.Run("set-failed-server--but-status-all-server-already-failed", func(t *testing.T) {
		r := newReplicatedRouteTest()

		r.stubGetMem(
			50, 50,
			50, 50,
		)

		r.stubServerFailed(true)
		r.selector.SetFailedServer(serverID1)

		r.stubRand(499000)
		assert.Equal(t, serverID1, r.selector.SelectServer(""))
		assert.Equal(t, false, r.selector.HasNextAvailableServer())

		assert.Equal(t, []ServerID{serverID1, serverID2}, r.selector.SelectForDelete(""))

		assert.Equal(t, 1, len(r.stats.NotifyServerFailedCalls()))
		assert.Equal(t, serverID1, r.stats.NotifyServerFailedCalls()[0].Server)
	})

	t.Run("with-mem-zero-use-default-1-percent-min", func(t *testing.T) {
		r := newReplicatedRouteTest()

		r.stubGetMem(
			0, 50,
		)

		r.stubRand(1000) // 1 / 1000
		assert.Equal(t, serverID1, r.selector.SelectServer(""))
		assert.Equal(t, true, r.selector.HasNextAvailableServer())

		assert.Equal(t, []ServerID{serverID1, serverID2}, r.selector.SelectForDelete(""))
	})

	t.Run("with-mem-zero-use-default-3-percent-min", func(t *testing.T) {
		r := newReplicatedRouteTest(WithMinPercentage(3.0))

		r.stubGetMem(
			0, 50,
			0, 50,
		)

		r.stubRand(30000)
		assert.Equal(t, serverID2, r.selector.SelectServer(""))
		assert.Equal(t, true, r.selector.HasNextAvailableServer())

		r.selector.Reset()

		r.stubRand(29000)
		assert.Equal(t, serverID1, r.selector.SelectServer(""))
		assert.Equal(t, true, r.selector.HasNextAvailableServer())

		assert.Equal(t, []ServerID{serverID1, serverID2}, r.selector.SelectForDelete(""))
	})
}

func TestReplicatedRoute_With_Real_Rand(*testing.T) {
	stats := &ServerStatsMock{
		GetMemUsageFunc: func(server ServerID) float64 {
			return 50
		},
		IsServerFailedFunc: func(server ServerID) bool {
			return false
		},
	}
	route := NewReplicatedRoute(
		[]ServerID{
			serverID1,
			serverID2,
		},
		stats,
	)
	selector := route.NewSelector()

	counters := map[ServerID]int{}
	for i := 0; i < 1000; i++ {
		server := selector.SelectServer("")

		counters[server]++

		selector.Reset()
	}

	fmt.Println(counters)
}

func TestComputeWeightAccumWithMinPercent(t *testing.T) {
	table := []struct {
		name       string
		weights    []float64
		minPercent float64

		newWeights []float64
	}{
		{
			name:       "empty",
			weights:    nil,
			minPercent: 1.0,
			newWeights: nil,
		},
		{
			name:       "no-min",
			weights:    []float64{10, 20, 30},
			minPercent: 1.0,
			newWeights: []float64{10, 30, 60},
		},
		{
			name:       "with-one-zero",
			weights:    []float64{10, 20, 0},
			minPercent: 1.0,
			newWeights: []float64{10, 30, 30 + 30.0/99.0},
		},
		{
			name:       "with-one-zero-in-middle",
			weights:    []float64{10, 20, 0, 30},
			minPercent: 1.0,
			newWeights: []float64{10, 30, 30 + 60.0/99.0, 60 + 60.0/99.0},
		},
		{
			name:       "with-one-zero-in-the-beginning",
			weights:    []float64{0, 10, 20, 30},
			minPercent: 1.0,
			newWeights: []float64{
				60.0 / 99.0,
				10 + 60.0/99.0,
				30 + 60.0/99.0,
				60 + 60.0/99.0,
			},
		},
		{
			name:       "with-two-zeros",
			weights:    []float64{0, 10, 0, 30},
			minPercent: 4.0,
			newWeights: []float64{
				40.0 / 11.5,
				10 + 40.0/11.5,
				10 + 80.0/11.5,
				40 + 80.0/11.5,
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			weights := computeWeightAccumWithMinPercent(e.weights, e.minPercent)
			assert.Equal(t, e.newWeights, weights)
		})
	}
}
