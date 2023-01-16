package item

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/QuangTung97/memproxy"
	"github.com/stretchr/testify/assert"
	"testing"
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
	pipe *memproxy.PipelineMock

	fillCalls int
	fillKeys  []userKey
	fillFunc  Filler[userValue, userKey]

	item *Item[userValue, userKey]
}

func newItemTest() *itemTest {
	sess := &memproxy.SessionMock{}
	pipe := &memproxy.PipelineMock{}

	var calls []func()
	sess.AddNextCallFunc = func(fn func()) {
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

	i := &itemTest{
		pipe: pipe,
		fillFunc: func(ctx context.Context, key userKey) func() (userValue, error) {
			return func() (userValue, error) {
				return userValue{}, nil
			}
		},
	}

	var userFiller Filler[userValue, userKey] = func(ctx context.Context, key userKey) func() (userValue, error) {
		i.fillCalls++
		i.fillKeys = append(i.fillKeys, key)
		return i.fillFunc(ctx, key)
	}

	i.item = New(pipe, unmarshalUser, userFiller)

	// stubbing
	i.stubLeaseSet()

	return i
}

func (i *itemTest) stubLeaseGet(resp memproxy.LeaseGetResponse, err error) {
	i.pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		return func() (memproxy.LeaseGetResponse, error) {
			return resp, err
		}
	}
}

func (i *itemTest) stubLeaseSet() {
	i.pipe.LeaseSetFunc = func(
		key string, data []byte, cas uint64, options memproxy.LeaseSetOptions,
	) func() (memproxy.LeaseSetResponse, error) {
		return func() (memproxy.LeaseSetResponse, error) {
			return memproxy.LeaseSetResponse{}, nil
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

		i.stubLeaseGet(memproxy.LeaseGetResponse{}, nil)

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
}
