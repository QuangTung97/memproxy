package memproxy

import (
	"sync"
	"time"
)

type sessionProviderImpl struct {
	nowFn   func() time.Time
	sleepFn func(d time.Duration)
}

var _ SessionProvider = &sessionProviderImpl{}

type sessionProviderConf struct {
	nowFn   func() time.Time
	sleepFn func(d time.Duration)
}

// SessionProviderOption ...
type SessionProviderOption func(conf *sessionProviderConf)

// WithSessionNowFunc ...
func WithSessionNowFunc(nowFn func() time.Time) SessionProviderOption {
	return func(conf *sessionProviderConf) {
		conf.nowFn = nowFn
	}
}

// WithSessionSleepFunc ...
func WithSessionSleepFunc(sleepFn func(d time.Duration)) SessionProviderOption {
	return func(conf *sessionProviderConf) {
		conf.sleepFn = sleepFn
	}
}

// NewSessionProvider is THREAD SAFE
func NewSessionProvider(options ...SessionProviderOption) SessionProvider {
	conf := &sessionProviderConf{
		nowFn:   time.Now,
		sleepFn: time.Sleep,
	}

	for _, opt := range options {
		opt(conf)
	}

	return &sessionProviderImpl{
		nowFn:   conf.nowFn,
		sleepFn: conf.sleepFn,
	}
}

// New a Session, NOT a Thread Safe Object
func (p *sessionProviderImpl) New() Session {
	return newSession(p, nil)
}

func newSession(
	provider *sessionProviderImpl, higher *sessionImpl,
) *sessionImpl {
	s := &sessionImpl{
		provider: provider,
		lower:    nil,
		higher:   higher,
	}

	if higher != nil {
		higher.lower = s
		s.isDirty = higher.isDirty
	}
	return s
}

type sessionImpl struct {
	provider  *sessionProviderImpl
	nextCalls callbackList
	heap      delayedCallHeap

	isDirty bool // an optimization

	lower  *sessionImpl
	higher *sessionImpl
}

type delayedCall struct {
	startedAt time.Time
	call      CallbackFunc
}

var _ Session = &sessionImpl{}

func setDirtyRecursive(s *sessionImpl) {
	for !s.isDirty {
		s.isDirty = true
		if s.lower == nil {
			return
		}
		s = s.lower
	}
}

// AddNextCall ...
func (s *sessionImpl) AddNextCall(fn CallbackFunc) {
	setDirtyRecursive(s)
	s.nextCalls.append(fn)
}

// AddDelayedCall ...
func (s *sessionImpl) AddDelayedCall(d time.Duration, fn CallbackFunc) {
	setDirtyRecursive(s)
	s.heap.push(delayedCall{
		startedAt: s.provider.nowFn().Add(d),
		call:      fn,
	})
}

// Execute ...
func (s *sessionImpl) Execute() {
	if !s.isDirty {
		return
	}

	if s.higher != nil {
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
	for !s.nextCalls.isEmpty() {
		it := s.nextCalls.getIterator()

		for {
			fn, ok := it.getNext()
			if !ok {
				break
			}
			fn.Call()
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
			top.call.Call()
		}
	}
}

// ===============================
// callback list
// ===============================

type callbackList struct {
	head *callbackSegment
	tail *callbackSegment
}

type callbackSegment struct {
	next  *callbackSegment // linked list of callback
	size  int
	funcs [16]CallbackFunc
}

func (s *callbackList) append(fn CallbackFunc) {
	if s.tail == nil {
		s.head = getCallbackSegment()
		s.tail = s.head
	} else if s.tail.size >= len(s.tail.funcs) {
		newTail := getCallbackSegment()
		s.tail.next = newTail
		s.tail = newTail
	}

	n := s.tail
	n.funcs[n.size] = fn
	n.size++
}

func (s *callbackList) isEmpty() bool {
	return s.head == nil
}

type callbackListIterator struct {
	current *callbackSegment
	index   int
}

// getIterator also clears the list
func (s *callbackList) getIterator() callbackListIterator {
	it := callbackListIterator{
		current: s.head,
		index:   0,
	}

	s.head = nil
	s.tail = nil

	return it
}

func (it *callbackListIterator) getNext() (CallbackFunc, bool) {
	if it.current == nil {
		return CallbackFunc{}, false
	}

	if it.index >= it.current.size {
		prev := it.current
		it.current = it.current.next

		putCallbackSegment(prev)

		it.index = 0

		if it.current == nil {
			return CallbackFunc{}, false
		}
	}

	fn := it.current.funcs[it.index]
	it.index++
	return fn, true
}

// ===============================
// Pool of Callback Segments
// ===============================

var callbackSegmentPool = sync.Pool{
	New: func() any {
		return &callbackSegment{}
	},
}

func getCallbackSegment() *callbackSegment {
	return callbackSegmentPool.Get().(*callbackSegment)
}

func putCallbackSegment(s *callbackSegment) {
	s.next = nil
	for i := 0; i < s.size; i++ {
		s.funcs[i] = CallbackFunc{}
	}
	s.size = 0
	callbackSegmentPool.Put(s)
}
