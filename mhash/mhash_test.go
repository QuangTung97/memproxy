package mhash

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/QuangTung97/memproxy"
	"github.com/stretchr/testify/assert"
	"testing"
)

type customerUsage struct {
	Tenant     string `json:"tenant"`
	CampaignID int64  `json:"campaignID"`

	Phone    string `json:"phone"`
	TermCode string `json:"termCode"`
	Hash     uint64 `json:"hash"`

	Usage int64 `json:"usage"`
	Age   int64 `json:"age"`
}

func (u customerUsage) Marshal() ([]byte, error) {
	return json.Marshal(u)
}

func (u customerUsage) getKey() customerUsageKey {
	return customerUsageKey{
		Phone:    u.Phone,
		TermCode: u.TermCode,
	}
}

func unmarshalCustomerUsage(data []byte) (customerUsage, error) {
	var u customerUsage
	err := json.Unmarshal(data, &u)
	return u, err
}

type customerUsageRootKey struct {
	Tenant     string
	CampaignID int64
}

func (r customerUsageRootKey) String() string {
	return fmt.Sprintf("%s:%d", r.Tenant, r.CampaignID)
}

type customerUsageKey struct {
	Phone    string
	TermCode string
	hash     uint64
}

func (c customerUsageKey) Hash() uint64 {
	return c.hash
}

type hashTest struct {
	pipe *memproxy.PipelineMock
	hash *Hash[customerUsage, customerUsageRootKey, customerUsageKey]
}

func newHashTest() *hashTest {
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

	h := &hashTest{
		pipe: pipe,
	}

	var filler Filler[customerUsage, customerUsageRootKey] = func(
		ctx context.Context, rootKey customerUsageRootKey, hash uint64,
	) func() ([]byte, error) {
		return func() ([]byte, error) {
			return nil, nil
		}
	}

	h.hash = New[customerUsage, customerUsageRootKey, customerUsageKey](
		sess, pipe, customerUsage.getKey, unmarshalCustomerUsage, filler,
	)

	return h
}

func (h *hashTest) stubLeaseGet(resp memproxy.LeaseGetResponse, err error) {
	h.pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		return func() (memproxy.LeaseGetResponse, error) {
			return resp, err
		}
	}
}

type testKeyType struct{}

func newContext() context.Context {
	return context.WithValue(context.Background(), testKeyType{}, "some-value")
}

func mustMarshalBucket(b Bucket[customerUsage]) []byte {
	data, err := b.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func TestHash(t *testing.T) {
	t.Run("get-from-cache", func(t *testing.T) {
		h := newHashTest()

		usage := customerUsage{
			Tenant:     "TENANT01",
			CampaignID: 41,
			Phone:      "0987000111",
			TermCode:   "TERM01",
			Hash:       2233,

			Usage: 12,
			Age:   22,
		}

		h.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    5566,
			Data: mustMarshalBucket(Bucket[customerUsage]{
				Items: []customerUsage{
					usage,
				},
				Bitset: BitSet{77, 88},
			}),
		}, nil)

		fn := h.hash.Get(newContext(),
			customerUsageRootKey{
				Tenant:     "TENANT01",
				CampaignID: 41,
			}, customerUsageKey{
				Phone:    "0987000111",
				TermCode: "TERM01",
			},
		)

		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, Null[customerUsage]{
			Valid: true,
			Data:  usage,
		}, resp)

		getCalls := h.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(getCalls))
		assert.Equal(t, "TENANT01:41:00", getCalls[0].Key)
	})
}
