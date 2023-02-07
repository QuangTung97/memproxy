package item

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type userValue struct {
	Tenant string `json:"tenant"`
	Name   string `json:"name"`
	Age    int64  `json:"age"`
}

type userKey struct {
	Tenant string
	Name   string
}

func (u userValue) GetKey() userKey {
	return userKey{
		Tenant: u.Tenant,
		Name:   u.Name,
	}
}

var _ Value = userValue{}

func (u userValue) Marshal() ([]byte, error) {
	return json.Marshal(u)
}

func unmarshalUser(data []byte) (userValue, error) {
	var user userValue
	err := json.Unmarshal(data, &user)
	return user, err
}

func (k userKey) String() string {
	return k.Tenant + ":" + k.Name
}

type testKeyType struct{}

func newContext() context.Context {
	return context.WithValue(context.Background(), testKeyType{}, "some-value")
}

func newTestError() error {
	return errors.New("test error")
}

type itemTest struct {
	sess *mocks.SessionMock
	pipe *mocks.PipelineMock

	fillCalls int
	fillKeys  []userKey
	fillFunc  Filler[userValue, userKey]

	item *Item[userValue, userKey]

	delayCalls []time.Duration

	actions []string
}

func leaseGetAction(key string) string {
	return "lease-get: " + key
}

func leaseGetFuncAction(key string) string {
	return "lease-get-func: " + key
}

func leaseSetAction(key string) string {
	return "lease-set: " + key
}

func fillAction(key string) string {
	return "fill: " + key
}

func fillFuncAction(key string) string {
	return "fill-func: " + key
}

func newItemTest(options ...Option) *itemTest {
	return newItemTestWithSleepDurations(DefaultSleepDurations(), options...)
}

func newItemTestWithSleepDurations(
	sleepDurations []time.Duration,
	options ...Option,
) *itemTest {
	sess := &mocks.SessionMock{}
	pipe := &mocks.PipelineMock{}

	i := &itemTest{
		fillFunc: func(ctx context.Context, key userKey) func() (userValue, error) {
			return func() (userValue, error) {
				return userValue{}, nil
			}
		},
	}

	var calls []func()

	sess.AddNextCallFunc = func(fn func()) {
		calls = append(calls, fn)
	}
	sess.AddDelayedCallFunc = func(d time.Duration, fn func()) {
		i.delayCalls = append(i.delayCalls, d)
		calls = append(calls, fn)
	}
	sess.ExecuteFunc = func() {
		for len(calls) > 0 {
			nextCalls := calls
			calls = nil
			for _, fn := range nextCalls {
				fn()
			}
		}
	}

	pipe.LowerSessionFunc = func() memproxy.Session {
		return sess
	}

	i.sess = sess
	i.pipe = pipe

	var userFiller Filler[userValue, userKey] = func(ctx context.Context, key userKey) func() (userValue, error) {
		i.fillCalls++
		i.fillKeys = append(i.fillKeys, key)
		return i.fillFunc(ctx, key)
	}

	options = append(options, WithSleepDurations(sleepDurations...))
	i.item = New(pipe, unmarshalUser, userFiller, options...)

	// stubbing
	i.stubLeaseSet()

	return i
}

func (i *itemTest) appendAction(action string) {
	i.actions = append(i.actions, action)
}

func (i *itemTest) stubLeaseGet(resp memproxy.LeaseGetResponse, err error) {
	i.pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		i.appendAction(leaseGetAction(key))
		return func() (memproxy.LeaseGetResponse, error) {
			i.appendAction(leaseGetFuncAction(key))
			return resp, err
		}
	}
}

func (i *itemTest) stubLeaseGetMulti(respList ...memproxy.LeaseGetResponse) {
	i.pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		i.appendAction(leaseGetAction(key))
		index := len(i.pipe.LeaseGetCalls()) - 1

		return func() (memproxy.LeaseGetResponse, error) {
			i.appendAction(leaseGetFuncAction(key))
			return respList[index], nil
		}
	}
}

func (i *itemTest) stubLeaseSet() {
	i.pipe.LeaseSetFunc = func(
		key string, data []byte, cas uint64, options memproxy.LeaseSetOptions,
	) func() (memproxy.LeaseSetResponse, error) {
		i.appendAction(leaseSetAction(key))
		return func() (memproxy.LeaseSetResponse, error) {
			return memproxy.LeaseSetResponse{}, nil
		}
	}
}

func (i *itemTest) stubFillMulti(users ...userValue) {
	i.fillFunc = func(ctx context.Context, key userKey) func() (userValue, error) {
		i.appendAction(fillAction(key.String()))
		index := i.fillCalls - 1
		return func() (userValue, error) {
			i.appendAction(fillFuncAction(key.String()))
			return users[index], nil
		}
	}
}

func mustMarshalUser(u userValue) []byte {
	data, err := json.Marshal(u)
	if err != nil {
		panic(err)
	}
	return data
}

func TestItem(t *testing.T) {
	t.Run("call-lease-get", func(t *testing.T) {
		i := newItemTest()

		user := userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}

		i.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			Data:   mustMarshalUser(user),
		}, nil)

		i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		calls := i.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)
	})

	t.Run("lease-get-found-returns-data", func(t *testing.T) {
		i := newItemTest()

		user := userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}

		i.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			Data:   mustMarshalUser(user),
		}, nil)

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		resp, err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, user, resp)
	})

	t.Run("lease-get-with-error-returns-error", func(t *testing.T) {
		i := newItemTest()

		i.stubLeaseGet(memproxy.LeaseGetResponse{}, newTestError())

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		resp, err := fn()
		assert.Equal(t, newTestError(), err)
		assert.Equal(t, userValue{}, resp)
	})

	t.Run("lease-get-lease-granted", func(t *testing.T) {
		i := newItemTest()

		const cas = 8231

		i.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    cas,
		}, nil)

		user := userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}

		i.fillFunc = func(ctx context.Context, key userKey) func() (userValue, error) {
			return func() (userValue, error) {
				return user, nil
			}
		}

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		resp, err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, user, resp)

		// Check Calls
		assert.Equal(t, 1, i.fillCalls)
		assert.Equal(t, []userKey{
			user.GetKey(),
		}, i.fillKeys)

		// Check Sets
		setCalls := i.pipe.LeaseSetCalls()
		assert.Equal(t, 1, len(setCalls))
		assert.Equal(t, "TENANT01:USER01", setCalls[0].Key)
		assert.Equal(t, uint64(cas), setCalls[0].Cas)
		assert.Equal(t, mustMarshalUser(user), setCalls[0].Data)
	})

	t.Run("lease-get-lease-granted--fill-with-error-not-found", func(t *testing.T) {
		i := newItemTest()

		const cas = 8231

		i.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    cas,
		}, nil)

		// stub delete
		i.pipe.DeleteFunc = func(key string, options memproxy.DeleteOptions) func() (memproxy.DeleteResponse, error) {
			return func() (memproxy.DeleteResponse, error) {
				return memproxy.DeleteResponse{}, nil
			}
		}

		user := userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}

		i.fillFunc = func(ctx context.Context, key userKey) func() (userValue, error) {
			return func() (userValue, error) {
				return userValue{Age: 11}, ErrNotFound
			}
		}

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		resp, err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, userValue{Age: 11}, resp)

		// Check Calls
		assert.Equal(t, 1, i.fillCalls)
		assert.Equal(t, []userKey{
			user.GetKey(),
		}, i.fillKeys)

		// Check Set
		setCalls := i.pipe.LeaseSetCalls()
		assert.Equal(t, 0, len(setCalls))

		// Check Delete
		assert.Equal(t, 1, len(i.pipe.DeleteCalls()))
		assert.Equal(t, "TENANT01:USER01", i.pipe.DeleteCalls()[0].Key)
	})
}

func TestItem__LeaseRejected__Do_Sleep(t *testing.T) {
	t.Run("lease-rejected-second-lease-get-found", func(t *testing.T) {
		i := newItemTestWithSleepDurations([]time.Duration{
			3 * time.Millisecond,
			7 * time.Millisecond,
			13 * time.Millisecond,
		})

		user := userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}

		i.stubLeaseGetMulti(
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusFound,
				Data:   mustMarshalUser(user),
			},
		)

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})
		result, err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, user, result)

		calls := i.pipe.LeaseGetCalls()

		assert.Equal(t, 2, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)
		assert.Equal(t, "TENANT01:USER01", calls[1].Key)

		assert.Equal(t, []time.Duration{
			3 * time.Millisecond,
		}, i.delayCalls)
	})

	t.Run("lease-rejected-multi-times", func(t *testing.T) {
		i := newItemTestWithSleepDurations([]time.Duration{
			3 * time.Millisecond,
			7 * time.Millisecond,
			13 * time.Millisecond,
		})

		user := userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}

		i.stubLeaseGetMulti(
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusFound,
				Data:   mustMarshalUser(user),
			},
		)

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		result, err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, user, result)

		calls := i.pipe.LeaseGetCalls()

		assert.Equal(t, 4, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)
		assert.Equal(t, "TENANT01:USER01", calls[1].Key)
		assert.Equal(t, "TENANT01:USER01", calls[3].Key)

		assert.Equal(t, []time.Duration{
			3 * time.Millisecond,
			7 * time.Millisecond,
			13 * time.Millisecond,
		}, i.delayCalls)
	})

	t.Run("lease-rejected-exceed-max-number-of-times--returns-error", func(t *testing.T) {
		i := newItemTestWithSleepDurations([]time.Duration{
			3 * time.Millisecond,
			7 * time.Millisecond,
			13 * time.Millisecond,
		}, WithEnableErrorOnExceedRetryLimit(true))

		i.stubLeaseGetMulti(
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
			},
		)

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		result, err := fn()
		assert.Equal(t, ErrExceededRejectRetryLimit, err)
		assert.Equal(t, userValue{}, result)

		calls := i.pipe.LeaseGetCalls()

		assert.Equal(t, 4, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)
		assert.Equal(t, "TENANT01:USER01", calls[1].Key)
		assert.Equal(t, "TENANT01:USER01", calls[3].Key)

		assert.Equal(t, []time.Duration{
			3 * time.Millisecond,
			7 * time.Millisecond,
			13 * time.Millisecond,
		}, i.delayCalls)
	})

	t.Run("error-when-lease-get-status-invalid", func(t *testing.T) {
		i := newItemTest()

		i.stubLeaseGetMulti(
			memproxy.LeaseGetResponse{},
		)

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		result, err := fn()
		assert.Equal(t, ErrInvalidLeaseGetStatus, err)
		assert.Equal(t, userValue{}, result)

		calls := i.pipe.LeaseGetCalls()

		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)

		assert.Equal(t, 0, len(i.delayCalls))
	})

	t.Run("lease-rejected-exceed-max-number-of-times--still-do-fill", func(t *testing.T) {
		i := newItemTestWithSleepDurations([]time.Duration{
			3 * time.Millisecond,
			7 * time.Millisecond,
			13 * time.Millisecond,
		})

		const cas = 78112

		i.stubLeaseGetMulti(
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
				CAS:    cas,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
				CAS:    cas,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
				CAS:    cas,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseRejected,
				CAS:    cas,
			},
		)

		user := userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}

		i.fillFunc = func(ctx context.Context, key userKey) func() (userValue, error) {
			return func() (userValue, error) {
				return user, nil
			}
		}

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		result, err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, user, result)

		calls := i.pipe.LeaseGetCalls()

		assert.Equal(t, 4, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)
		assert.Equal(t, "TENANT01:USER01", calls[1].Key)
		assert.Equal(t, "TENANT01:USER01", calls[3].Key)

		assert.Equal(t, []time.Duration{
			3 * time.Millisecond,
			7 * time.Millisecond,
			13 * time.Millisecond,
		}, i.delayCalls)

		assert.Equal(t, 1, len(i.pipe.LeaseSetCalls()))
		assert.Equal(t, "TENANT01:USER01", i.pipe.LeaseSetCalls()[0].Key)
		assert.Equal(t, uint64(cas), i.pipe.LeaseSetCalls()[0].Cas)
		assert.Equal(t, mustMarshalUser(user), i.pipe.LeaseSetCalls()[0].Data)
	})

	t.Run("error-when-lease-get-return-error", func(t *testing.T) {
		i := newItemTest()

		i.stubLeaseGet(
			memproxy.LeaseGetResponse{},
			errors.New("lease get error"),
		)

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		result, err := fn()
		assert.Equal(t, errors.New("lease get error"), err)
		assert.Equal(t, userValue{}, result)

		calls := i.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)
	})

	t.Run("continuing-get-from-db---when-lease-get-return-error", func(t *testing.T) {
		i := newItemTest(WithEnableFillingOnCacheError(true))

		i.stubLeaseGet(
			memproxy.LeaseGetResponse{
				CAS: 9988,
			},
			errors.New("lease get error"),
		)

		user := userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}

		i.fillFunc = func(ctx context.Context, key userKey) func() (userValue, error) {
			return func() (userValue, error) {
				return user, nil
			}
		}

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		result, err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, user, result)

		calls := i.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)

		assert.Equal(t, 0, len(i.pipe.LeaseSetCalls()))
	})

	t.Run("error-when-fill-error---after-lease-get-return-error", func(t *testing.T) {
		i := newItemTest(WithEnableFillingOnCacheError(true))

		i.stubLeaseGet(
			memproxy.LeaseGetResponse{
				CAS: 9988,
			},
			errors.New("lease get error"),
		)

		i.fillFunc = func(ctx context.Context, key userKey) func() (userValue, error) {
			return func() (userValue, error) {
				return userValue{}, errors.New("fill error")
			}
		}

		fn := i.item.Get(newContext(), userKey{
			Tenant: "TENANT01",
			Name:   "USER01",
		})

		result, err := fn()
		assert.Equal(t, errors.New("fill error"), err)
		assert.Equal(t, userValue{}, result)

		calls := i.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)

		assert.Equal(t, 0, len(i.pipe.LeaseSetCalls()))
	})
}

func TestItem__Multi(t *testing.T) {
	t.Run("get-multi-different-keys", func(t *testing.T) {
		i := newItemTest()

		i.stubLeaseGetMulti(
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseGranted,
				CAS:    1101,
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseGranted,
				CAS:    1102,
			},
		)

		user1 := userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}

		user2 := userValue{
			Tenant: "TENANT02",
			Name:   "USER02",
			Age:    89,
		}

		i.stubFillMulti(user1, user2)

		fn1 := i.item.Get(newContext(), user1.GetKey())
		fn2 := i.item.Get(newContext(), user2.GetKey())

		result, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, user1, result)

		result, err = fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, user2, result)

		calls := i.pipe.LeaseGetCalls()

		assert.Equal(t, 2, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)
		assert.Equal(t, "TENANT02:USER02", calls[1].Key)

		assert.Equal(t, []userKey{
			user1.GetKey(),
			user2.GetKey(),
		}, i.fillKeys)

		assert.Equal(t, []string{
			leaseGetAction(user1.GetKey().String()),
			leaseGetAction(user2.GetKey().String()),

			leaseGetFuncAction(user1.GetKey().String()),
			fillAction(user1.GetKey().String()),

			leaseGetFuncAction(user2.GetKey().String()),
			fillAction(user2.GetKey().String()),

			fillFuncAction(user1.GetKey().String()),
			leaseSetAction(user1.GetKey().String()),

			fillFuncAction(user2.GetKey().String()),
			leaseSetAction(user2.GetKey().String()),
		}, i.actions)
	})

	t.Run("get-multi-same-key", func(t *testing.T) {
		i := newItemTest()

		i.stubLeaseGetMulti(
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseGranted,
				CAS:    1101,
			},
		)

		user1 := userValue{
			Tenant: "TENANT01",
			Name:   "USER01",
			Age:    88,
		}

		i.stubFillMulti(user1)

		fn1 := i.item.Get(newContext(), user1.GetKey())
		fn2 := i.item.Get(newContext(), user1.GetKey())

		result, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, user1, result)

		result, err = fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, user1, result)

		calls := i.pipe.LeaseGetCalls()

		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "TENANT01:USER01", calls[0].Key)

		assert.Equal(t, []userKey{
			user1.GetKey(),
		}, i.fillKeys)

		assert.Equal(t, []string{
			leaseGetAction(user1.GetKey().String()),

			leaseGetFuncAction(user1.GetKey().String()),
			fillAction(user1.GetKey().String()),

			fillFuncAction(user1.GetKey().String()),
			leaseSetAction(user1.GetKey().String()),
		}, i.actions)
	})
}
