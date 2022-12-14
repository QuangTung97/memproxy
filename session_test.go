package memproxy

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type sessionTest struct {
	now      time.Time
	nowCalls int
	nowFunc  func() time.Time

	sleepFunc  func(d time.Duration)
	sleepCalls []time.Duration

	sess Session
}

func newSessionTest() *sessionTest {
	s := &sessionTest{}

	s.now = newTime("2022-11-22T10:00:00+07:00")

	s.nowFunc = func() time.Time {
		return s.now
	}

	s.sleepFunc = func(d time.Duration) {
		s.now = s.now.Add(d)
		return
	}

	provider := NewSessionProvider(func() time.Time {
		s.nowCalls++
		return s.nowFunc()
	}, func(d time.Duration) {
		s.sleepCalls = append(s.sleepCalls, d)
		s.sleepFunc(d)
	})

	s.sess = provider.New()
	return s
}

type callMock struct {
	count int
	fn    func()
}

func newCallMock() *callMock {
	return &callMock{
		fn: func() {},
	}
}

func (m *callMock) get() func() {
	return func() {
		m.count++
		m.fn()
	}
}

func TestSessionAddNextCall(t *testing.T) {
	s := newSessionTest()

	fn1 := newCallMock()
	s.sess.AddNextCall(fn1.get())

	assert.Equal(t, 0, fn1.count)

	s.sess.Execute()

	assert.Equal(t, 1, fn1.count)
}

func TestSessionAddNextCall_Call_Chain(t *testing.T) {
	s := newSessionTest()

	fn1 := newCallMock()
	fn2 := &callMock{
		fn: func() {
			s.sess.AddNextCall(fn1.get())
		},
	}
	s.sess.AddNextCall(fn2.get())

	assert.Equal(t, 0, fn1.count)
	assert.Equal(t, 0, fn2.count)

	s.sess.Execute()

	assert.Equal(t, 1, fn1.count)
	assert.Equal(t, 1, fn2.count)
}

func TestSessionAddNextCall_Multiple_Calls__And_Form_A_Chain(t *testing.T) {
	s := newSessionTest()

	var calls []int

	fn1 := &callMock{
		fn: func() {
			calls = append(calls, 11)
		},
	}

	fn2 := &callMock{
		fn: func() {
			calls = append(calls, 12)
		},
	}

	fn3 := &callMock{
		fn: func() {
			calls = append(calls, 13)
			s.sess.AddNextCall(fn1.get())
		},
	}

	fn4 := &callMock{
		fn: func() {
			calls = append(calls, 14)
			s.sess.AddNextCall(fn2.get())
		},
	}

	s.sess.AddNextCall(fn3.get())
	s.sess.AddNextCall(fn4.get())

	s.sess.Execute()

	assert.Equal(t, []int{13, 14, 11, 12}, calls)
	assert.Equal(t, 0, s.nowCalls)
}

func TestSessionAddDelayedCall(t *testing.T) {
	s := newSessionTest()

	fn1 := newCallMock()

	s.sess.AddDelayedCall(7*time.Second, fn1.get())

	s.sess.Execute()

	assert.Equal(t, 3, s.nowCalls)
	assert.Equal(t, []time.Duration{7 * time.Second}, s.sleepCalls)
	assert.Equal(t, 1, fn1.count)
}

func TestSessionAddDelayedCall_Multi_Calls(t *testing.T) {
	s := newSessionTest()

	var calls []int
	fn1 := &callMock{
		fn: func() {
			calls = append(calls, 11)
		},
	}
	fn2 := &callMock{
		fn: func() {
			calls = append(calls, 12)
		},
	}

	s.sess.AddDelayedCall(13*time.Second, fn2.get())
	s.sess.AddDelayedCall(7*time.Second, fn1.get())

	s.sess.Execute()

	// 2 origin call + now() at the start + now() after sleep 7s + now() after sleep 6s
	assert.Equal(t, 5, s.nowCalls)
	assert.Equal(t, []time.Duration{7 * time.Second, 6 * time.Second}, s.sleepCalls)
	assert.Equal(t, []int{11, 12}, calls)
}

func TestSessionAddDelayedCall_Multi_Calls_Same_Duration(t *testing.T) {
	s := newSessionTest()

	var calls []int
	fn1 := &callMock{
		fn: func() {
			calls = append(calls, 11)
		},
	}
	fn2 := &callMock{
		fn: func() {
			calls = append(calls, 12)
		},
	}

	s.sess.AddDelayedCall(7*time.Second, fn1.get())
	s.sess.AddDelayedCall(7*time.Second, fn2.get())

	s.sess.Execute()

	// 2 + 2
	assert.Equal(t, 4, s.nowCalls)
	assert.Equal(t, []time.Duration{7 * time.Second}, s.sleepCalls)
	assert.Equal(t, []int{11, 12}, calls)
}

func TestSessionAddDelayedCall_NextCalls__And_Then_Delayed_Calls(t *testing.T) {
	s := newSessionTest()

	var calls []int
	fn1 := &callMock{
		fn: func() {
			calls = append(calls, 11)
		},
	}

	fn2 := &callMock{
		fn: func() {
			calls = append(calls, 12)
			s.sess.AddNextCall(fn1.get())
		},
	}

	fn3 := &callMock{
		fn: func() {
			calls = append(calls, 13)
		},
	}

	fn4 := &callMock{
		fn: func() {
			calls = append(calls, 14)
			s.sess.AddNextCall(fn3.get())
		},
	}

	s.sess.AddNextCall(fn2.get())

	s.sess.AddDelayedCall(7*time.Second, fn4.get())

	s.sess.Execute()

	// 1 first + 1 now at the start + 1 after sleep
	assert.Equal(t, 3, s.nowCalls)
	assert.Equal(t, []time.Duration{7 * time.Second}, s.sleepCalls)
	assert.Equal(t, []int{12, 11, 14, 13}, calls)
}

func TestSessionAddDelayedCall_Delay_Calls__Chain_To_Delay_Calls(t *testing.T) {
	s := newSessionTest()

	var calls []int
	fn1 := &callMock{
		fn: func() {
			calls = append(calls, 11)
		},
	}

	fn2 := &callMock{
		fn: func() {
			calls = append(calls, 12)
			s.sess.AddDelayedCall(11*time.Second, fn1.get())
		},
	}

	s.sess.AddDelayedCall(7*time.Second, fn2.get())

	s.sess.Execute()

	// 2 calls + 1 now at the start + 1 after sleep + 1 after sleep
	assert.Equal(t, 5, s.nowCalls)
	assert.Equal(t, []time.Duration{7 * time.Second, 11 * time.Second}, s.sleepCalls)
	assert.Equal(t, []int{12, 11}, calls)
}

func TestSessionAddDelayedCall_Delay_Call__Chain_To_Delay_Call__Negative_Duration(t *testing.T) {
	s := newSessionTest()

	var calls []int
	fn1 := &callMock{
		fn: func() {
			calls = append(calls, 11)
		},
	}

	fn2 := &callMock{
		fn: func() {
			calls = append(calls, 12)
			s.sess.AddDelayedCall(-3*time.Second, fn1.get())
		},
	}

	s.sess.AddDelayedCall(7*time.Second, fn2.get())

	s.sess.Execute()

	// 2 calls + 1 now at the start + 1 after sleep
	assert.Equal(t, 4, s.nowCalls)
	assert.Equal(t, []time.Duration{7 * time.Second}, s.sleepCalls)
	assert.Equal(t, []int{12, 11}, calls)
}

func TestSessionAddDelayedCall_Delay_Call_Negative_Duration(t *testing.T) {
	s := newSessionTest()

	var calls []int
	fn1 := &callMock{
		fn: func() {
			calls = append(calls, 11)
		},
	}

	s.sess.AddDelayedCall(-4*time.Second, fn1.get())

	s.sess.Execute()

	// 1 calls + 1 now at the start
	assert.Equal(t, 2, s.nowCalls)
	assert.Equal(t, []time.Duration(nil), s.sleepCalls)
	assert.Equal(t, []int{11}, calls)
}
