package mhash

import (
	"context"
	"encoding/json"
	"errors"
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
		hash:     u.Hash,
	}
}

func (u customerUsage) getRootKey() customerUsageRootKey {
	return customerUsageRootKey{
		Tenant:     u.Tenant,
		CampaignID: u.CampaignID,
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

	fillerFunc     Filler[customerUsage, customerUsageRootKey]
	fillerRootKeys []customerUsageRootKey
	fillerHashList []uint64
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
		h.fillerRootKeys = append(h.fillerRootKeys, rootKey)
		h.fillerHashList = append(h.fillerHashList, hash)

		if h.fillerFunc == nil {
			panic("fillerFunc is nil")
		}

		return h.fillerFunc(ctx, rootKey, hash)
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

func (h *hashTest) stubLeaseGetMulti(respList ...memproxy.LeaseGetResponse) {
	h.pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		index := len(h.pipe.LeaseGetCalls()) - 1
		return func() (memproxy.LeaseGetResponse, error) {
			return respList[index], nil
		}
	}
}

func (h *hashTest) stubFill(data []byte, err error) {
	h.fillerFunc = func(ctx context.Context, rootKey customerUsageRootKey, hash uint64) func() ([]byte, error) {
		return func() ([]byte, error) {
			return data, err
		}
	}
}

func (h *hashTest) stubLeaseSet() {
	h.pipe.LeaseSetFunc = func(
		key string, data []byte, cas uint64, options memproxy.LeaseSetOptions,
	) func() (memproxy.LeaseSetResponse, error) {
		return func() (memproxy.LeaseSetResponse, error) {
			return memproxy.LeaseSetResponse{}, nil
		}
	}
}

type testKeyType struct{}

func newContext() context.Context {
	return context.WithValue(context.Background(), testKeyType{}, "some-value")
}

func newBitSet(posList ...int) BitSet {
	b := BitSet{}
	for _, pos := range posList {
		b.SetBit(pos)
	}
	return b
}

func mustMarshalBucket(b Bucket[customerUsage]) []byte {
	data, err := b.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func newLeaseResp(pos int) memproxy.LeaseGetResponse {
	return memproxy.LeaseGetResponse{
		Status: memproxy.LeaseGetStatusFound,
		CAS:    5501,
		Data: mustMarshalBucket(Bucket[customerUsage]{
			Items:  []customerUsage{},
			Bitset: newBitSet(pos),
		}),
	}
}

func TestHash(t *testing.T) {
	t.Run("get-from-cache", func(t *testing.T) {
		h := newHashTest()

		const keyHash = 2233

		usage := customerUsage{
			Tenant:     "TENANT01",
			CampaignID: 41,
			Phone:      "0987000111",
			TermCode:   "TERM01",
			Hash:       keyHash,

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
			}),
		}, nil)

		fn := h.hash.Get(newContext(),
			customerUsageRootKey{
				Tenant:     "TENANT01",
				CampaignID: 41,
			}, customerUsageKey{
				Phone:    "0987000111",
				TermCode: "TERM01",
				hash:     keyHash,
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
		assert.Equal(t, "TENANT01:41:", getCalls[0].Key)
	})

	t.Run("get-from-cache-not-found", func(t *testing.T) {
		h := newHashTest()

		const keyHash = 2233

		h.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    5566,
			Data: mustMarshalBucket(Bucket[customerUsage]{
				Items: []customerUsage{},
			}),
		}, nil)

		fn := h.hash.Get(newContext(),
			customerUsageRootKey{
				Tenant:     "TENANT01",
				CampaignID: 41,
			}, customerUsageKey{
				Phone:    "0987000111",
				TermCode: "TERM01",
				hash:     keyHash,
			},
		)

		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, Null[customerUsage]{}, resp)

		getCalls := h.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(getCalls))
		assert.Equal(t, "TENANT01:41:", getCalls[0].Key)
	})

	t.Run("get-first-level-returns-with-bit-set--continue-on-next-level", func(t *testing.T) {
		h := newHashTest()

		const keyHash = 0x2233 << (64 - 16)

		usage := customerUsage{
			Tenant:     "TENANT01",
			CampaignID: 41,
			Phone:      "0987000111",
			TermCode:   "TERM01",
			Hash:       keyHash,

			Usage: 12,
			Age:   22,
		}

		h.stubLeaseGetMulti(
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusFound,
				CAS:    5566,
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items:  []customerUsage{},
					Bitset: newBitSet(0x22),
				}),
			},
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusFound,
				CAS:    5566,
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items: []customerUsage{
						usage,
					},
				}),
			},
		)

		fn := h.hash.Get(newContext(),
			customerUsageRootKey{
				Tenant:     "TENANT01",
				CampaignID: 41,
			}, customerUsageKey{
				Phone:    "0987000111",
				TermCode: "TERM01",
				hash:     keyHash,
			},
		)

		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, Null[customerUsage]{
			Valid: true,
			Data:  usage,
		}, resp)

		getCalls := h.pipe.LeaseGetCalls()
		assert.Equal(t, 2, len(getCalls))
		assert.Equal(t, "TENANT01:41:", getCalls[0].Key)
		assert.Equal(t, "TENANT01:41:22", getCalls[1].Key)
	})

	t.Run("total-4-levels", func(t *testing.T) {
		h := newHashTest()

		const keyHash = 0x16273849 << (64 - 4*8)

		usage := customerUsage{
			Tenant:     "TENANT01",
			CampaignID: 41,
			Phone:      "0987000111",
			TermCode:   "TERM01",
			Hash:       keyHash,

			Usage: 12,
			Age:   22,
		}

		h.stubLeaseGetMulti(
			newLeaseResp(0x16),
			newLeaseResp(0x27),
			newLeaseResp(0x38),
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusFound,
				CAS:    5566,
				Data: mustMarshalBucket(Bucket[customerUsage]{
					Items: []customerUsage{
						usage,
					},
				}),
			},
		)

		fn := h.hash.Get(newContext(),
			customerUsageRootKey{
				Tenant:     "TENANT01",
				CampaignID: 41,
			}, customerUsageKey{
				Phone:    "0987000111",
				TermCode: "TERM01",
				hash:     keyHash,
			},
		)

		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, Null[customerUsage]{
			Valid: true,
			Data:  usage,
		}, resp)

		getCalls := h.pipe.LeaseGetCalls()
		assert.Equal(t, 4, len(getCalls))
		assert.Equal(t, "TENANT01:41:", getCalls[0].Key)
		assert.Equal(t, "TENANT01:41:16", getCalls[1].Key)
		assert.Equal(t, "TENANT01:41:1627", getCalls[2].Key)
		assert.Equal(t, "TENANT01:41:162738", getCalls[3].Key)
	})

	t.Run("error-when-over-5-level", func(t *testing.T) {
		h := newHashTest()

		const keyHash = 0x1627384950 << (64 - 5*8)

		h.stubLeaseGetMulti(
			newLeaseResp(0x16),
			newLeaseResp(0x27),
			newLeaseResp(0x38),
			newLeaseResp(0x49),
			newLeaseResp(0x50),
		)

		fn := h.hash.Get(newContext(),
			customerUsageRootKey{
				Tenant:     "TENANT01",
				CampaignID: 41,
			}, customerUsageKey{
				Phone:    "0987000111",
				TermCode: "TERM01",
				hash:     keyHash,
			},
		)

		resp, err := fn()

		assert.Equal(t, ErrHashTooDeep, err)
		assert.Equal(t, Null[customerUsage]{}, resp)

		getCalls := h.pipe.LeaseGetCalls()
		assert.Equal(t, 5, len(getCalls))
		assert.Equal(t, "TENANT01:41:", getCalls[0].Key)
		assert.Equal(t, "TENANT01:41:16", getCalls[1].Key)
		assert.Equal(t, "TENANT01:41:1627", getCalls[2].Key)
		assert.Equal(t, "TENANT01:41:162738", getCalls[3].Key)
		assert.Equal(t, "TENANT01:41:16273849", getCalls[4].Key)
	})

	t.Run("get-from-cache-returns-error", func(t *testing.T) {
		h := newHashTest()

		const keyHash = 2233

		someErr := errors.New("some error")
		h.stubLeaseGet(memproxy.LeaseGetResponse{}, someErr)

		fn := h.hash.Get(newContext(),
			customerUsageRootKey{
				Tenant:     "TENANT01",
				CampaignID: 41,
			}, customerUsageKey{
				Phone:    "0987000111",
				TermCode: "TERM01",
				hash:     keyHash,
			},
		)

		resp, err := fn()

		assert.Equal(t, someErr, err)
		assert.Equal(t, Null[customerUsage]{}, resp)

		getCalls := h.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(getCalls))
		assert.Equal(t, "TENANT01:41:", getCalls[0].Key)
	})

	t.Run("cache-lease-granted--do-fill", func(t *testing.T) {
		h := newHashTest()

		const keyHash = 2233 << (64 - 8*2)
		const cas = 120033

		usage := customerUsage{
			Tenant:     "TENANT01",
			CampaignID: 41,
			Phone:      "0987000111",
			TermCode:   "TERM01",
			Hash:       keyHash,

			Usage: 12,
			Age:   22,
		}

		h.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    cas,
		}, nil)

		data := mustMarshalBucket(Bucket[customerUsage]{
			Items: []customerUsage{
				usage,
			},
		})
		h.stubFill(data, nil)

		h.stubLeaseSet()

		rootKey := customerUsageRootKey{
			Tenant:     "TENANT01",
			CampaignID: 41,
		}
		usageKey := customerUsageKey{
			Phone:    "0987000111",
			TermCode: "TERM01",
			hash:     keyHash,
		}

		fn := h.hash.Get(newContext(),
			rootKey,
			usageKey,
		)

		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, Null[customerUsage]{
			Valid: true,
			Data:  usage,
		}, resp)

		getCalls := h.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(getCalls))
		assert.Equal(t, "TENANT01:41:", getCalls[0].Key)

		setCalls := h.pipe.LeaseSetCalls()
		assert.Equal(t, 1, len(setCalls))
		assert.Equal(t, "TENANT01:41:", setCalls[0].Key)
		assert.Equal(t, uint64(cas), setCalls[0].Cas)
		assert.Equal(t, data, setCalls[0].Data)

		assert.Equal(t, 1, len(h.fillerRootKeys))
		assert.Equal(t, rootKey, h.fillerRootKeys[0])
		assert.Equal(t, []uint64{0x00}, h.fillerHashList)
	})

	t.Run("cache-lease-granted--do-fill-return-errors", func(t *testing.T) {
		h := newHashTest()

		const keyHash = 2233 << (64 - 8*2)
		const cas = 120033

		h.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    cas,
		}, nil)

		someErr := errors.New("some fill error")
		h.stubFill(nil, someErr)

		h.stubLeaseSet()

		rootKey := customerUsageRootKey{
			Tenant:     "TENANT01",
			CampaignID: 41,
		}
		usageKey := customerUsageKey{
			Phone:    "0987000111",
			TermCode: "TERM01",
			hash:     keyHash,
		}

		fn := h.hash.Get(newContext(),
			rootKey,
			usageKey,
		)

		resp, err := fn()

		assert.Equal(t, someErr, err)
		assert.Equal(t, Null[customerUsage]{}, resp)

		getCalls := h.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(getCalls))
		assert.Equal(t, "TENANT01:41:", getCalls[0].Key)

		setCalls := h.pipe.LeaseSetCalls()
		assert.Equal(t, 0, len(setCalls))

		assert.Equal(t, 1, len(h.fillerRootKeys))
		assert.Equal(t, rootKey, h.fillerRootKeys[0])
		assert.Equal(t, []uint64{0x0}, h.fillerHashList)
	})

	t.Run("cache-lease-granted-at-level-3--do-fill", func(t *testing.T) {
		h := newHashTest()

		const keyHash = 0x1223ff << (64 - 8*3)
		const cas = 120033

		usage := customerUsage{
			Tenant:     "TENANT01",
			CampaignID: 41,
			Phone:      "0987000111",
			TermCode:   "TERM01",
			Hash:       keyHash,

			Usage: 88,
			Age:   99,
		}

		h.stubLeaseGetMulti(
			newLeaseResp(0x12),
			newLeaseResp(0x23),
			memproxy.LeaseGetResponse{
				Status: memproxy.LeaseGetStatusLeaseGranted,
				CAS:    cas,
			},
		)

		data := mustMarshalBucket(Bucket[customerUsage]{
			Items: []customerUsage{
				usage,
			},
		})
		h.stubFill(data, nil)

		h.stubLeaseSet()

		rootKey := customerUsageRootKey{
			Tenant:     "TENANT01",
			CampaignID: 41,
		}
		usageKey := customerUsageKey{
			Phone:    "0987000111",
			TermCode: "TERM01",
			hash:     keyHash,
		}

		fn := h.hash.Get(newContext(),
			rootKey,
			usageKey,
		)

		resp, err := fn()

		assert.Equal(t, nil, err)
		assert.Equal(t, Null[customerUsage]{
			Valid: true,
			Data:  usage,
		}, resp)

		getCalls := h.pipe.LeaseGetCalls()
		assert.Equal(t, 3, len(getCalls))
		assert.Equal(t, "TENANT01:41:", getCalls[0].Key)
		assert.Equal(t, "TENANT01:41:12", getCalls[1].Key)
		assert.Equal(t, "TENANT01:41:1223", getCalls[2].Key)

		setCalls := h.pipe.LeaseSetCalls()
		assert.Equal(t, 1, len(setCalls))
		assert.Equal(t, "TENANT01:41:1223", setCalls[0].Key)
		assert.Equal(t, uint64(cas), setCalls[0].Cas)
		assert.Equal(t, data, setCalls[0].Data)

		assert.Equal(t, 1, len(h.fillerRootKeys))
		assert.Equal(t, rootKey, h.fillerRootKeys[0])
		assert.Equal(t, []uint64{0x1223 << (64 - 8*2)}, h.fillerHashList)
	})
}
