package memproxy

import "time"

type sessionProviderImpl struct {
}

var _ SessionProvider = &sessionProviderImpl{}

// New ...
func (p *sessionProviderImpl) New() Session {
	return &sessionImpl{}
}

type sessionImpl struct {
}

var _ Session = &sessionImpl{}

// AddNextCall ...
func (s *sessionImpl) AddNextCall(fn func()) {
}

// AddDelayedCall ...
func (s *sessionImpl) AddDelayedCall(d time.Duration, fn func()) {
}

// Execute ...
func (s *sessionImpl) Execute() {
}
