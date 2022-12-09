package memproxy

import "time"

type sessionProviderImpl struct {
	nowFn   func() time.Time
	sleepFn func(d time.Duration)
}

var _ SessionProvider = &sessionProviderImpl{}

// NewSessionProvider ...
func NewSessionProvider(
	nowFn func() time.Time,
	sleepFn func(d time.Duration),
) SessionProvider {
	return &sessionProviderImpl{
		nowFn:   nowFn,
		sleepFn: sleepFn,
	}
}

// New ...
func (p *sessionProviderImpl) New() Session {
	return &sessionImpl{provider: p}
}

type sessionImpl struct {
	provider  *sessionProviderImpl
	nextCalls []func()
	heap      delayedCallHeap
}

type delayedCall struct {
	startedAt time.Time
	call      func()
}

var _ Session = &sessionImpl{}

// AddNextCall ...
func (s *sessionImpl) AddNextCall(fn func()) {
	s.nextCalls = append(s.nextCalls, fn)
}

// AddDelayedCall ...
func (s *sessionImpl) AddDelayedCall(d time.Duration, fn func()) {
	s.heap.push(delayedCall{
		startedAt: s.provider.nowFn().Add(d),
		call:      fn,
	})
}

// Execute ...
func (s *sessionImpl) Execute() {
	for {
		s.executeNextCalls()

		if s.heap.size() == 0 {
			return
		}

		s.executeDelayedCalls()
	}
}

func (s *sessionImpl) executeNextCalls() {
	for len(s.nextCalls) > 0 {
		nextCalls := s.nextCalls
		s.nextCalls = s.nextCalls[:0]
		for _, call := range nextCalls {
			call()
		}
	}
}

const deviationDuration = 100 * time.Microsecond

func (s *sessionImpl) executeDelayedCalls() {
MainLoop:
	for s.heap.size() > 0 {
		now := s.provider.nowFn()

		for s.heap.size() > 0 {
			top := s.heap.top()
			topStart := top.startedAt
			if topStart.Add(-deviationDuration).After(now) {
				duration := topStart.Sub(now)
				s.provider.sleepFn(duration)
				continue MainLoop
			}
			s.heap.pop()
			top.call()
		}
	}
}
