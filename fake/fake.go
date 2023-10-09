package fake

import (
	"context"
	"sync"

	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/mocks"
)

// Entry ...
type Entry struct {
	Valid bool
	Data  []byte
	CAS   uint64
}

// Memcache fake memcached for testing purpose
type Memcache struct {
	sessProvider memproxy.SessionProvider

	mut     sync.Mutex
	cas     uint64
	entries map[string]Entry
}

var _ memproxy.Memcache = &Memcache{}

// New ...
func New() *Memcache {
	return &Memcache{
		sessProvider: memproxy.NewSessionProvider(),

		entries: map[string]Entry{},
	}
}

func (m *Memcache) nextCAS() uint64 {
	m.cas++
	return m.cas
}

// Pipeline returns a Fake Pipeline
//
//revive:disable-next-line:cognitive-complexity
func (m *Memcache) Pipeline(_ context.Context, _ ...memproxy.PipelineOption) memproxy.Pipeline {
	sess := m.sessProvider.New()
	var calls []func()
	doCalls := func() {
		for _, fn := range calls {
			fn()
		}
		calls = nil
	}

	pipe := &mocks.PipelineMock{}

	pipe.LeaseGetFunc = func(key string, options memproxy.LeaseGetOptions) memproxy.LeaseGetResult {
		var resp memproxy.LeaseGetResponse

		callFn := func() {
			m.mut.Lock()
			defer m.mut.Unlock()

			entry, ok := m.entries[key]

			if !ok {
				cas := m.nextCAS()
				m.entries[key] = Entry{
					CAS: cas,
				}
				resp = memproxy.LeaseGetResponse{
					Status: memproxy.LeaseGetStatusLeaseGranted,
					CAS:    cas,
				}
				return
			}

			if !entry.Valid {
				resp = memproxy.LeaseGetResponse{
					Status: memproxy.LeaseGetStatusLeaseRejected,
					CAS:    entry.CAS,
				}
				return
			}

			resp = memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusFound,
				CAS:    entry.CAS,
				Data:   entry.Data,
			}
		}

		calls = append(calls, callFn)

		return memproxy.LeaseGetResultFunc(func() (memproxy.LeaseGetResponse, error) {
			doCalls()
			return resp, nil
		})
	}

	pipe.LeaseSetFunc = func(
		key string, data []byte, cas uint64, options memproxy.LeaseSetOptions,
	) func() (memproxy.LeaseSetResponse, error) {
		status := memproxy.LeaseSetStatusNotStored

		callFn := func() {
			m.mut.Lock()
			defer m.mut.Unlock()

			entry, ok := m.entries[key]
			if !ok {
				return
			}

			if entry.CAS != cas {
				return
			}

			m.entries[key] = Entry{
				Valid: true,
				Data:  data,
				CAS:   cas,
			}
			status = memproxy.LeaseSetStatusStored
		}

		calls = append(calls, callFn)

		return func() (memproxy.LeaseSetResponse, error) {
			doCalls()
			return memproxy.LeaseSetResponse{
				Status: status,
			}, nil
		}
	}

	pipe.DeleteFunc = func(key string, options memproxy.DeleteOptions) func() (memproxy.DeleteResponse, error) {
		callFn := func() {
			m.mut.Lock()
			defer m.mut.Unlock()

			delete(m.entries, key)
		}

		calls = append(calls, callFn)

		return func() (memproxy.DeleteResponse, error) {
			doCalls()
			return memproxy.DeleteResponse{}, nil
		}
	}

	pipe.FinishFunc = func() {
		doCalls()
	}

	pipe.ExecuteFunc = func() {
		doCalls()
	}

	pipe.LowerSessionFunc = func() memproxy.Session {
		return sess
	}

	return pipe
}

// Close ...
func (*Memcache) Close() error {
	return nil
}
