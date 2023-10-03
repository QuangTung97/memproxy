package mmap

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/mocks"
)

type stockLocationRootKey struct {
	sku string
}

func (k stockLocationRootKey) String() string {
	return fmt.Sprintf("p/stocks/%s", k.sku)
}

func (stockLocationRootKey) AvgBucketSizeLog() uint8 {
	return 2
}

type stockLocationKey struct {
	loc  string
	hash uint64
}

func (k stockLocationKey) Hash() uint64 {
	return k.hash
}

type stockLocation struct {
	Sku      string  `json:"sku"`
	Location string  `json:"location"`
	Hash     uint64  `json:"hash"`
	Quantity float64 `json:"quantity"`
}

func (s stockLocation) getKey() stockLocationKey {
	return stockLocationKey{
		loc:  s.Location,
		hash: s.Hash,
	}
}

func (s stockLocation) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func unmarshalStockLocation(data []byte) (stockLocation, error) {
	var s stockLocation
	err := json.Unmarshal(data, &s)
	return s, err
}

type mapTest struct {
	pipe *mocks.PipelineMock
	mmap *Map[stockLocation, stockLocationRootKey, stockLocationKey]

	fillRootKeys   []stockLocationRootKey
	fillHashRanges []HashRange
	fillFunc       Filler[stockLocation, stockLocationRootKey]
}

func newMapTest() *mapTest {
	sess := memproxy.NewSessionProvider().New()

	m := &mapTest{
		pipe: &mocks.PipelineMock{
			LowerSessionFunc: func() memproxy.Session {
				return sess
			},
		},
	}

	m.mmap = New[stockLocation, stockLocationRootKey, stockLocationKey](
		m.pipe,
		unmarshalStockLocation,
		func(ctx context.Context, rootKey stockLocationRootKey, hashRange HashRange) func() ([]stockLocation, error) {
			m.fillRootKeys = append(m.fillRootKeys, rootKey)
			m.fillHashRanges = append(m.fillHashRanges, hashRange)

			return m.fillFunc(ctx, rootKey, hashRange)
		},
		stockLocation.getKey,
	)
	return m
}

func (m *mapTest) stubLeaseGet(resp memproxy.LeaseGetResponse) {
	m.pipe.LeaseGetFunc = func(
		key string, options memproxy.LeaseGetOptions,
	) func() (memproxy.LeaseGetResponse, error) {
		return func() (memproxy.LeaseGetResponse, error) {
			return resp, nil
		}
	}
}

func mustMarshalStocks(b Bucket[stockLocation]) []byte {
	data, err := b.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func TestMap(t *testing.T) {
	const sku1 = "SKU01"

	const loc1 = "LOC01"
	const loc2 = "LOC02"

	t.Run("with single bucket elem count, check lease get call", func(t *testing.T) {
		m := newMapTest()

		hash1 := newHash(0x1122, 2)
		stock1 := stockLocation{
			Sku:      sku1,
			Location: loc1,
			Hash:     hash1,
			Quantity: 41,
		}

		bucket := Bucket[stockLocation]{
			Values: []stockLocation{stock1},
		}

		m.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    3300,
			Data:   mustMarshalStocks(bucket),
		})

		fn := m.mmap.Get(context.Background(), 3,
			stockLocationRootKey{
				sku: sku1,
			},
			stockLocationKey{
				loc:  loc1,
				hash: hash1,
			},
		)

		result, err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, Option[stockLocation]{
			Valid: true,
			Data:  stock1,
		}, result)

		calls := m.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "p/stocks/SKU01:0:", calls[0].Key)
	})

	t.Run("with single bucket elem count, not found", func(t *testing.T) {
		m := newMapTest()

		hash1 := newHash(0x1122, 2)
		stock1 := stockLocation{
			Sku:      sku1,
			Location: loc1,
			Hash:     hash1,
			Quantity: 41,
		}

		bucket := Bucket[stockLocation]{
			Values: []stockLocation{stock1},
		}

		m.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    3300,
			Data:   mustMarshalStocks(bucket),
		})

		fn := m.mmap.Get(context.Background(), 3,
			stockLocationRootKey{
				sku: sku1,
			},
			stockLocationKey{
				loc:  loc2,
				hash: hash1,
			},
		)

		result, err := fn()
		assert.Equal(t, nil, err)
		assert.Equal(t, Option[stockLocation]{}, result)

		calls := m.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "p/stocks/SKU01:0:", calls[0].Key)
	})

	t.Run("with elem count more than 4, hash in the first range", func(t *testing.T) {
		m := newMapTest()

		hash1 := newHash(0x1122, 2)
		stock1 := stockLocation{
			Sku:      sku1,
			Location: loc1,
			Hash:     hash1,
			Quantity: 41,
		}

		bucket := Bucket[stockLocation]{
			Values: []stockLocation{stock1},
		}

		m.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    3300,
			Data:   mustMarshalStocks(bucket),
		})

		m.mmap.Get(context.Background(), 5,
			stockLocationRootKey{
				sku: sku1,
			},
			stockLocationKey{
				loc:  loc1,
				hash: hash1,
			},
		)

		calls := m.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "p/stocks/SKU01:1:0", calls[0].Key)
	})

	t.Run("with elem count more than 4, hash in the second range", func(t *testing.T) {
		m := newMapTest()

		hash1 := newHash(0x8122, 2)
		stock1 := stockLocation{
			Sku:      sku1,
			Location: loc1,
			Hash:     hash1,
			Quantity: 41,
		}

		bucket := Bucket[stockLocation]{
			Values: []stockLocation{stock1},
		}

		m.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    3300,
			Data:   mustMarshalStocks(bucket),
		})

		m.mmap.Get(context.Background(), 5,
			stockLocationRootKey{
				sku: sku1,
			},
			stockLocationKey{
				loc:  loc1,
				hash: hash1,
			},
		)

		calls := m.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "p/stocks/SKU01:0:", calls[0].Key)
	})

	t.Run("with elem count = 37, hash in the first part", func(t *testing.T) {
		m := newMapTest()

		hash1 := newHash(0x2fff, 2)

		bucket := Bucket[stockLocation]{}

		m.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    3300,
			Data:   mustMarshalStocks(bucket),
		})

		m.mmap.Get(context.Background(), 37,
			stockLocationRootKey{
				sku: sku1,
			},
			stockLocationKey{
				loc:  loc1,
				hash: hash1,
			},
		)

		calls := m.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "p/stocks/SKU01:4:2", calls[0].Key)
	})

	t.Run("with elem count = 37, hash in the second part", func(t *testing.T) {
		m := newMapTest()

		hash1 := newHash(0x3000, 2)

		bucket := Bucket[stockLocation]{}

		m.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusFound,
			CAS:    3300,
			Data:   mustMarshalStocks(bucket),
		})

		m.mmap.Get(context.Background(), 37,
			stockLocationRootKey{
				sku: sku1,
			},
			stockLocationKey{
				loc:  loc1,
				hash: hash1,
			},
		)

		calls := m.pipe.LeaseGetCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, "p/stocks/SKU01:3:2", calls[0].Key)
	})
}

func TestComputeSizeLog(t *testing.T) {
	t.Run("smaller than avg", func(t *testing.T) {
		sizeLog := computeSizeLog(2, 3, newHash(0x1234, 2))
		assert.Equal(t, uint8(0), sizeLog)
	})

	t.Run("equal avg", func(t *testing.T) {
		sizeLog := computeSizeLog(2, 4, newHash(0x1234, 2))
		assert.Equal(t, uint8(0), sizeLog)
	})

	t.Run("bigger than avg", func(t *testing.T) {
		sizeLog := computeSizeLog(2, 5, newHash(0x1234, 2))
		assert.Equal(t, uint8(1), sizeLog)
	})

	t.Run("bigger than avg, but in the upper part", func(t *testing.T) {
		sizeLog := computeSizeLog(2, 5, newHash(0x8123, 2))
		assert.Equal(t, uint8(0), sizeLog)
	})

	t.Run("end of size log = 1", func(t *testing.T) {
		sizeLog := computeSizeLog(2, 8, newHash(0xf234, 2))
		assert.Equal(t, uint8(1), sizeLog)
	})

	t.Run("size log = 3", func(t *testing.T) {
		sizeLog := computeSizeLog(2, 32, newHash(0x1234, 2))
		assert.Equal(t, uint8(3), sizeLog)

		sizeLog = computeSizeLog(2, 32, newHash(0xf234, 2))
		assert.Equal(t, uint8(3), sizeLog)
	})

	t.Run("right after size log = 3", func(t *testing.T) {
		sizeLog := computeSizeLog(2, 33, newHash(0x0f00, 2))
		assert.Equal(t, uint8(4), sizeLog)

		sizeLog = computeSizeLog(2, 33, newHash(0x0fff, 2))
		assert.Equal(t, uint8(4), sizeLog)

		sizeLog = computeSizeLog(2, 34, newHash(0x0fff, 2))
		assert.Equal(t, uint8(4), sizeLog)

		sizeLog = computeSizeLog(2, 33, newHash(0x1000, 2))
		assert.Equal(t, uint8(3), sizeLog)

		sizeLog = computeSizeLog(2, 34, newHash(0x1000, 2))
		assert.Equal(t, uint8(3), sizeLog)
	})

	t.Run("middle of size log = 3", func(t *testing.T) {
		sizeLog := computeSizeLog(2, 37, newHash(0x2fff, 2))
		assert.Equal(t, uint8(4), sizeLog)

		sizeLog = computeSizeLog(2, 37, newHash(0x3000, 2))
		assert.Equal(t, uint8(3), sizeLog)

		sizeLog = computeSizeLog(2, 38, newHash(0x2fff, 2))
		assert.Equal(t, uint8(4), sizeLog)

		sizeLog = computeSizeLog(2, 38, newHash(0x3000, 2))
		assert.Equal(t, uint8(3), sizeLog)
	})

	t.Run("end of size log = 3", func(t *testing.T) {
		sizeLog := computeSizeLog(2, 63, newHash(0xffff, 2))
		assert.Equal(t, uint8(4), sizeLog)

		sizeLog = computeSizeLog(2, 64, newHash(0xffff, 2))
		assert.Equal(t, uint8(4), sizeLog)
	})
}

func TestComputeBucketKeyString(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		s := ComputeBucketKeyString(
			70,
			stockLocationRootKey{
				sku: "SKU01",
			},
			stockLocationKey{
				hash: newHash(0xffff, 2),
			},
			"/",
		)
		assert.Equal(t, "p/stocks/SKU01/4/f", s)
	})

	t.Run("at bound of the first part", func(t *testing.T) {
		s := ComputeBucketKeyString(
			70,
			stockLocationRootKey{
				sku: "SKU01",
			},
			stockLocationKey{
				hash: newHash(0b0001_0111, 1),
			},
			"/",
		)
		assert.Equal(t, "p/stocks/SKU01/5/10", s)

		s = ComputeBucketKeyString(
			70,
			stockLocationRootKey{
				sku: "SKU01",
			},
			stockLocationKey{
				hash: newHash(0b0001_1000, 1),
			},
			"/",
		)
		assert.Equal(t, "p/stocks/SKU01/4/1", s)
	})
}
