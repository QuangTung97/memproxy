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

func newReplicatedRouteTest() *replicatedRouteTest {
	r := &replicatedRouteTest{}

	r.stats = &ServerStatsMock{}

	randFunc := func(n uint64) uint64 {
		r.randArgs = append(r.randArgs, n)
		return r.randFunc(n)
	}

	r.route = NewReplicatedRoute(
		[]ServerID{
			serverID1,
			serverID2,
		},
		r.stats,
		WithRandFunc(randFunc),
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

		assert.Equal(t, []ServerID{serverID2}, r.selector.SelectForDelete(""))
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
}

func TestReplicatedRoute_With_Real_Rand(*testing.T) {
	stats := &ServerStatsMock{
		GetMemUsageFunc: func(server ServerID) float64 {
			return 50
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
