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
	return newSession(p, nil)
}

func newSession(provider *sessionProviderImpl, higher *sessionImpl) *sessionImpl {
	s := &sessionImpl{
		provider: provider,
		lower:    nil,
		higher:   higher,
	}
	if higher != nil {
		higher.lower = s
	}
	return s
}

type sessionImpl struct {
	provider  *sessionProviderImpl
	nextCalls []func()
	heap      delayedCallHeap

	isDirty bool // an optimization

	lower  *sessionImpl
	higher *sessionImpl
}

type delayedCall struct {
	startedAt time.Time
	call      func()
}

var _ Session = &sessionImpl{}

func (s *sessionImpl) isSessionDirty() bool {
	return s.isDirty || len(s.nextCalls) > 0 || s.heap.size() > 0
}

func setDirtyRecursive(s *sessionImpl) {
	for {
		s.isDirty = true
		if s.lower == nil {
			return
		}
		if s.lower.isSessionDirty() {
			return
		}
		s = s.lower
	}
}

// AddNextCall ...
func (s *sessionImpl) AddNextCall(fn func()) {
	setDirtyRecursive(s)
	s.nextCalls = append(s.nextCalls, fn)
}

// AddDelayedCall ...
func (s *sessionImpl) AddDelayedCall(d time.Duration, fn func()) {
	setDirtyRecursive(s)
	s.heap.push(delayedCall{
		startedAt: s.provider.nowFn().Add(d),
		call:      fn,
	})
}

// Execute ...
func (s *sessionImpl) Execute() {
	if s.higher != nil && s.higher.isDirty {
		s.higher.Execute()
	}

	for {
		s.executeNextCalls()

		if s.heap.size() == 0 {
			s.isDirty = false
			return
		}

		s.executeDelayedCalls()
	}
}

// GetLower get lower priority session
func (s *sessionImpl) GetLower() Session {
	if s.lower != nil {
		return s.lower
	}
	return newSession(s.provider, s)
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
