package proxy

import (
	"sync"
)

// ===============================
// pool of lease get state
// ===============================

var leaseGetStatePool = sync.Pool{
	New: func() any {
		return &leaseGetState{}
	},
}

func putLeaseGetState(s *leaseGetState) {
	*s = leaseGetState{}
	leaseGetStatePool.Put(s)
}

func getLeaseGetState() *leaseGetState {
	return leaseGetStatePool.Get().(*leaseGetState)
}
