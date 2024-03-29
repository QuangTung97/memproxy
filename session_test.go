package memproxy

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
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

	provider := NewSessionProvider(
		WithSessionNowFunc(func() time.Time {
			s.nowCalls++
			return s.nowFunc()
		}),
		WithSessionSleepFunc(func(d time.Duration) {
			s.sleepCalls = append(s.sleepCalls, d)
			s.sleepFunc(d)
		}),
	)

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

func (m *callMock) get() CallbackFunc {
	return NewEmptyCallback(func() {
		m.count++
		m.fn()
	})
}

func TestSessionAddNextCall(t *testing.T) {
	s := newSessionTest()

	fn1 := newCallMock()
	s.sess.AddNextCall(fn1.get())

	assert.Equal(t, 0, fn1.count)

	s.sess.Execute()

	assert.Equal(t, 1, fn1.count)
}

func TestSessionAddNextCall__AddSession_Multiple_Next_Calls__Inside_Execute(t *testing.T) {
	s := newSessionTest()

	var calls []string

	fn1 := &callMock{
		fn: func() {
			calls = append(calls, "fn1")
		},
	}

	fn2 := &callMock{
		fn: func() {
			calls = append(calls, "fn2")
		},
	}

	fn3 := &callMock{
		fn: func() {
			calls = append(calls, "fn3")
		},
	}

	fn4 := &callMock{
		fn: func() {
			calls = append(calls, "fn4")
			s.sess.AddNextCall(fn1.get())
			s.sess.AddNextCall(fn2.get())
		},
	}
	fn5 := &callMock{
		fn: func() {
			calls = append(calls, "fn5")
			s.sess.AddNextCall(fn3.get())
		},
	}

	s.sess.AddNextCall(fn4.get())
	s.sess.AddNextCall(fn5.get())

	assert.Equal(t, 0, fn1.count)
	assert.Equal(t, 0, fn4.count)

	s.sess.Execute()

	assert.Equal(t, 1, fn1.count)
	assert.Equal(t, 1, fn2.count)
	assert.Equal(t, 1, fn3.count)
	assert.Equal(t, 1, fn4.count)
	assert.Equal(t, 1, fn5.count)

	assert.Equal(t, []string{"fn4", "fn5", "fn1", "fn2", "fn3"}, calls)
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

func TestSession_Lower_Priority(t *testing.T) {
	s := newSessionTest()

	var calls []int

	newCall := func(n int) *callMock {
		return &callMock{
			fn: func() {
				calls = append(calls, n)
			},
		}
	}

	fn1 := newCall(11)
	fn2 := newCall(12)
	fn3 := newCall(13)
	fn4 := newCall(14)

	s.sess.AddNextCall(fn1.get())

	lower := s.sess.GetLower()
	lower.AddNextCall(fn2.get())

	s.sess.AddNextCall(fn3.get())

	lower.AddNextCall(fn4.get())

	lower.Execute()

	assert.Equal(t, []int{11, 13, 12, 14}, calls)
}

func TestSession_Not_New_Lower__When_Already_Init(t *testing.T) {
	s := newSessionTest()

	lower1 := s.sess.GetLower()
	lower2 := s.sess.GetLower()

	assert.Same(t, lower1, lower2)
}

func TestSession_Lower_Priority__After_AddNextCall(t *testing.T) {
	s := newSessionTest()

	var calls []int

	newCall := func(n int) *callMock {
		return &callMock{
			fn: func() {
				calls = append(calls, n)
			},
		}
	}

	fn1 := newCall(11)
	fn2 := newCall(12)
	fn3 := newCall(13)
	fn4 := newCall(14)
	fn5 := newCall(15)

	s.sess.AddNextCall(fn1.get())

	lower := s.sess.GetLower()
	lower.AddNextCall(fn2.get())

	s.sess.AddNextCall(fn3.get())

	lower.AddNextCall(fn4.get())

	lower2 := lower.GetLower()
	lower2.AddNextCall(fn5.get())

	lower2.Execute()

	assert.Equal(t, []int{11, 13, 12, 14, 15}, calls)
}

func TestSession_Call_Execute_Only_On_Middle_Priority(t *testing.T) {
	s := newSessionTest()

	var calls []int

	newCall := func(n int) *callMock {
		return &callMock{
			fn: func() {
				calls = append(calls, n)
			},
		}
	}

	fn1 := newCall(11)
	fn2 := newCall(12)
	fn3 := newCall(13)
	fn4 := newCall(14)
	fn5 := newCall(15)

	s.sess.AddNextCall(fn1.get())

	lower := s.sess.GetLower()
	lower.AddNextCall(fn2.get())

	s.sess.AddNextCall(fn3.get())

	lower.AddNextCall(fn4.get())

	lower2 := lower.GetLower()
	lower2.AddNextCall(fn5.get())

	lower.Execute()
	assert.Equal(t, []int{11, 13, 12, 14}, calls)

	calls = nil
	lower2.Execute()
	assert.Equal(t, []int{15}, calls)
}

func TestSession_Call_Execute_Only_On_Highest_Priority(t *testing.T) {
	s := newSessionTest()

	var calls []int

	newCall := func(n int) *callMock {
		return &callMock{
			fn: func() {
				calls = append(calls, n)
			},
		}
	}

	fn1 := newCall(11)
	fn2 := newCall(12)
	fn3 := newCall(13)
	fn4 := newCall(14)
	fn5 := newCall(15)

	s.sess.AddNextCall(fn1.get())

	lower := s.sess.GetLower()
	lower.AddNextCall(fn2.get())

	s.sess.AddNextCall(fn3.get())

	lower.AddNextCall(fn4.get())

	lower2 := lower.GetLower()
	lower2.AddNextCall(fn5.get())

	s.sess.Execute()
	assert.Equal(t, []int{11, 13}, calls)

	calls = nil
	lower.Execute()
	assert.Equal(t, []int{12, 14}, calls)

	calls = nil
	lower.Execute()
	assert.Equal(t, []int(nil), calls)

	calls = nil
	lower2.Execute()
	assert.Equal(t, []int{15}, calls)
}

func TestSession_Lower_Priority__Before_AddNextCall__And_After_Execute__Multi_Levels(t *testing.T) {
	s := newSessionTest()

	var calls []int

	newCall := func(n int) *callMock {
		return &callMock{
			fn: func() {
				calls = append(calls, n)
			},
		}
	}

	fn1 := newCall(11)
	fn2 := newCall(12)
	fn3 := newCall(13)
	fn4 := newCall(14)
	fn5 := newCall(15)

	s.sess.AddNextCall(fn1.get())

	lower := s.sess.GetLower()
	lower.AddNextCall(fn2.get())

	s.sess.AddNextCall(fn3.get())

	lower2 := lower.GetLower()
	lower2.AddNextCall(fn5.get())

	s.sess.AddNextCall(fn4.get())

	lower2.Execute()

	assert.Equal(t, []int{11, 13, 14, 12, 15}, calls)

	calls = []int{}
	s.sess.AddNextCall(fn4.get())
	lower2.Execute()
	assert.Equal(t, []int{14}, calls)
}

func TestSession_Lower_Priority__Delayed_Call(t *testing.T) {
	s := newSessionTest()

	var calls []int

	newCall := func(n int) *callMock {
		return &callMock{
			fn: func() {
				calls = append(calls, n)
			},
		}
	}

	fn1 := newCall(11)
	fn2 := newCall(12)
	fn3 := newCall(13)

	s.sess.AddDelayedCall(10*time.Millisecond, fn1.get())

	lower := s.sess.GetLower()
	lower.AddDelayedCall(5*time.Millisecond, fn2.get())

	s.sess.AddDelayedCall(12*time.Millisecond, fn3.get())

	lower.Execute()

	assert.Equal(t, []int{11, 13, 12}, calls)

	// 2 calls + 1 now at the start + 1 after sleep + 1 after sleep for the higher
	// 1 calls + 1 now at the start
	assert.Equal(t, 7, s.nowCalls)
	assert.Equal(t, []time.Duration{10 * time.Millisecond, 2 * time.Millisecond}, s.sleepCalls)
	assert.Equal(t, 1, fn1.count)
}

func TestSession_Lower_Priority__Delayed_Call__Sleep_Again_In_Lower_Session(t *testing.T) {
	s := newSessionTest()

	var calls []int

	newCall := func(n int) *callMock {
		return &callMock{
			fn: func() {
				calls = append(calls, n)
			},
		}
	}

	fn1 := newCall(11)
	fn2 := newCall(12)
	fn3 := newCall(13)

	s.sess.AddDelayedCall(10*time.Millisecond, fn1.get())

	lower := s.sess.GetLower()
	lower.AddDelayedCall(15*time.Millisecond, fn2.get())

	s.sess.AddDelayedCall(12*time.Millisecond, fn3.get())

	lower.Execute()

	assert.Equal(t, []int{11, 13, 12}, calls)

	// 2 calls + 1 now at the start + 1 after sleep + 1 after sleep -- for the higher
	// 1 calls + 1 now at the start + 1 after sleep
	assert.Equal(t, 8, s.nowCalls)
	assert.Equal(t, []time.Duration{
		10 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
	}, s.sleepCalls)
	assert.Equal(t, 1, fn1.count)
}

func TestSession_Lower_Priority_AddNextCall__Run_On_Lowest_Priority(t *testing.T) {
	s := newSessionTest()

	var calls []int

	newCall := func(n int) *callMock {
		return &callMock{
			fn: func() {
				calls = append(calls, n)
			},
		}
	}

	fn1 := newCall(11)

	s.sess.AddNextCall(fn1.get())

	lower1 := s.sess.GetLower()
	lower2 := lower1.GetLower()

	lower2.Execute()

	assert.Equal(t, []int{11}, calls)
}

func TestEmpty(t *testing.T) {
	calls := 0
	fn := LeaseGetResultFunc(func() (LeaseGetResponse, error) {
		calls++
		return LeaseGetResponse{
			CAS: 123,
		}, nil
	})

	resp, err := fn.Result()

	assert.Equal(t, nil, err)
	assert.Equal(t, LeaseGetResponse{
		CAS: 123,
	}, resp)

	assert.Equal(t, 1, calls)
}

func iterateCallbackSegment(l *callbackList) {
	it := l.getIterator()
	for {
		fn, ok := it.getNext()
		if !ok {
			break
		}
		fn.Call()
	}
}

func newCallbackListTest() *callbackList {
	var l callbackList
	return &l
}

func TestCallbackSegment(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		l := newCallbackListTest()
		assert.Equal(t, true, l.isEmpty())
		iterateCallbackSegment(l)
	})

	t.Run("single", func(t *testing.T) {
		l := newCallbackListTest()

		var values []string

		fn1 := &callMock{
			fn: func() {
				values = append(values, "fn1")
			},
		}
		l.append(fn1.get())

		assert.Equal(t, false, l.isEmpty())
		iterateCallbackSegment(l)
		assert.Equal(t, true, l.isEmpty())

		assert.Equal(t, 1, fn1.count)
		assert.Equal(t, []string{"fn1"}, values)
	})

	t.Run("multiple", func(t *testing.T) {
		l := newCallbackListTest()

		var values []string

		fn1 := &callMock{
			fn: func() {
				values = append(values, "fn1")
			},
		}
		fn2 := &callMock{
			fn: func() {
				values = append(values, "fn2")
			},
		}
		fn3 := &callMock{
			fn: func() {
				values = append(values, "fn3")
			},
		}
		l.append(fn1.get())
		l.append(fn2.get())
		l.append(fn3.get())

		iterateCallbackSegment(l)

		assert.Equal(t, 1, fn1.count)
		assert.Equal(t, 1, fn2.count)
		assert.Equal(t, 1, fn3.count)

		assert.Equal(t, []string{"fn1", "fn2", "fn3"}, values)
	})

	t.Run("multiples of 16", func(t *testing.T) {
		l := newCallbackListTest()

		var values []string

		for i := 0; i < 16*3; i++ {
			index := i
			fn := &callMock{
				fn: func() {
					values = append(values, fmt.Sprintf("fn%02d", index))
				},
			}
			l.append(fn.get())
		}

		iterateCallbackSegment(l)

		expected := make([]string, 16*3)
		for i := range expected {
			expected[i] = fmt.Sprintf("fn%02d", i)
		}

		assert.Equal(t, 16*3, len(values))
		assert.Equal(t, expected, values)
	})
}

func TestCallbackSegmentPool(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		x := getCallbackSegment()
		x.size = 3
		x.next = x

		num1 := 10
		num2 := 10

		x.funcs[0] = CallbackFunc{
			Object: unsafe.Pointer(&num1),
		}
		x.funcs[2] = CallbackFunc{
			Object: unsafe.Pointer(&num2),
		}
		x.funcs[3] = CallbackFunc{
			Object: unsafe.Pointer(&num2),
		}

		oldPtr := unsafe.Pointer(x)

		putCallbackSegment(x)

		x = getCallbackSegment()
		assert.Equal(t, 0, x.size)
		assert.Nil(t, x.next)
		assert.Nil(t, x.funcs[0].Object)
		assert.Nil(t, x.funcs[2].Object)

		fmt.Println("POINTERS EQUAL:", oldPtr == unsafe.Pointer(x))
		fmt.Println("SHOULD NOT NIL:", x.funcs[3].Object)
	})
}
