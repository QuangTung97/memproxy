package mmap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
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

func (s stockLocation) getRootKey() stockLocationRootKey {
	return stockLocationRootKey{
		sku: s.Sku,
	}
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

func newMapTest(options ...MapOption) *mapTest {
	sess := memproxy.NewSessionProvider().New()

	m := &mapTest{
		pipe: &mocks.PipelineMock{
			LowerSessionFunc: func() memproxy.Session {
				return sess
			},
			ExecuteFunc: func() {},
		},
	}

	m.mmap = New[stockLocation, stockLocationRootKey, stockLocationKey](
		m.pipe,
		unmarshalStockLocation,
		func(ctx context.Context, rootKey stockLocationRootKey, hashRange HashRange) func() ([]stockLocation, error) {
			m.fillRootKeys = append(m.fillRootKeys, rootKey)
			m.fillHashRanges = append(m.fillHashRanges, hashRange)

			if m.fillFunc == nil {
				panic("fillFunc is nil")
			}
			return m.fillFunc(ctx, rootKey, hashRange)
		},
		stockLocation.getKey,
		options...,
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

func (m *mapTest) stubFillFunc(stocks ...stockLocation) {
	m.fillFunc = func(
		ctx context.Context, rootKey stockLocationRootKey, hashRange HashRange,
	) func() ([]stockLocation, error) {
		return func() ([]stockLocation, error) {
			return stocks, nil
		}
	}
}

func (m *mapTest) stubLeaseSet() {
	m.pipe.LeaseSetFunc = func(
		key string, data []byte, cas uint64, options memproxy.LeaseSetOptions,
	) func() (memproxy.LeaseSetResponse, error) {
		return func() (memproxy.LeaseSetResponse, error) {
			return memproxy.LeaseSetResponse{
				Status: memproxy.LeaseSetStatusStored,
			}, nil
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

const sku1 = "SKU01"
const sku2 = "SKU02"
const sku3 = "SKU03"

const loc1 = "LOC01"
const loc2 = "LOC02"
const loc3 = "LOC03"

func TestMap(t *testing.T) {
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

	t.Run("single key, with lease granted, do fill from cache", func(t *testing.T) {
		m := newMapTest()

		hash1 := newHash(0x7122, 2)

		m.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    3300,
		})

		stock1 := stockLocation{
			Sku:      sku1,
			Location: loc1,
			Hash:     hash1,
			Quantity: 41,
		}

		m.stubFillFunc(stock1)
		m.stubLeaseSet()

		fn := m.mmap.Get(context.Background(), 110,
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
		assert.Equal(t, "p/stocks/SKU01:5:70", calls[0].Key)

		assert.Equal(t, []stockLocationRootKey{
			{sku: sku1},
		}, m.fillRootKeys)
		assert.Equal(t, []HashRange{
			{
				Begin: 0x7000_0000_0000_0000,
				End:   0x77ff_ffff_ffff_ffff,
			},
		}, m.fillHashRanges)

		setCalls := m.pipe.LeaseSetCalls()
		assert.Equal(t, 1, len(setCalls))
		assert.Equal(t, "p/stocks/SKU01:5:70", setCalls[0].Key)
		assert.Equal(t, uint64(3300), setCalls[0].Cas)

		unmarshaler := NewBucketUnmarshaler(unmarshalStockLocation)
		bucket, err := unmarshaler(setCalls[0].Data)
		assert.Equal(t, nil, err)
		assert.Equal(t, Bucket[stockLocation]{
			Values: []stockLocation{stock1},
		}, bucket)
	})

	t.Run("single key, with options, with lease get error, do fill from cache", func(t *testing.T) {
		m := newMapTest(
			WithItemOptions(item.WithEnableFillingOnCacheError(true)),
			WithSeparator("/"),
		)

		hash1 := newHash(0x7122, 2)

		// lease get error
		m.pipe.LeaseGetFunc = func(
			key string, options memproxy.LeaseGetOptions,
		) func() (memproxy.LeaseGetResponse, error) {
			return func() (memproxy.LeaseGetResponse, error) {
				return memproxy.LeaseGetResponse{}, errors.New("lease get error")
			}
		}

		stock1 := stockLocation{
			Sku:      sku1,
			Location: loc1,
			Hash:     hash1,
			Quantity: 41,
		}

		m.stubFillFunc(stock1)
		m.stubLeaseSet()

		fn := m.mmap.Get(context.Background(), 110,
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
		assert.Equal(t, "p/stocks/SKU01/5/70", calls[0].Key)

		assert.Equal(t, []stockLocationRootKey{
			{sku: sku1},
		}, m.fillRootKeys)
		assert.Equal(t, []HashRange{
			{
				Begin: 0x7000_0000_0000_0000,
				End:   0x77ff_ffff_ffff_ffff,
			},
		}, m.fillHashRanges)

		setCalls := m.pipe.LeaseSetCalls()
		assert.Equal(t, 0, len(setCalls))
	})

	t.Run("fill error, should return error", func(t *testing.T) {
		m := newMapTest()

		hash1 := newHash(0x7122, 2)

		m.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    3300,
		})

		m.fillFunc = func(
			ctx context.Context, rootKey stockLocationRootKey, hashRange HashRange,
		) func() ([]stockLocation, error) {
			return func() ([]stockLocation, error) {
				return nil, errors.New("fill error")
			}
		}

		fn := m.mmap.Get(context.Background(), 110,
			stockLocationRootKey{
				sku: sku1,
			},
			stockLocationKey{
				loc:  loc1,
				hash: hash1,
			},
		)

		result, err := fn()
		assert.Equal(t, errors.New("fill error"), err)
		assert.Equal(t, Option[stockLocation]{}, result)
	})

	t.Run("multiple keys", func(t *testing.T) {
		m := newMapTest()

		hash1 := newHash(0x7122, 2)
		hash2 := newHash(0x9122, 2)
		hash3 := newHash(0xaf22, 2)

		m.stubLeaseGet(memproxy.LeaseGetResponse{
			Status: memproxy.LeaseGetStatusLeaseGranted,
			CAS:    3300,
		})

		stock1 := stockLocation{
			Sku:      sku1,
			Location: loc1,
			Hash:     hash1,
			Quantity: 41,
		}

		stock2 := stockLocation{
			Sku:      sku2,
			Location: loc2,
			Hash:     hash2,
			Quantity: 41,
		}

		stocks := [][]stockLocation{
			{stock1},
			{stock2},
			{},
		}

		m.fillFunc = func(
			ctx context.Context, rootKey stockLocationRootKey, hashRange HashRange,
		) func() ([]stockLocation, error) {
			index := len(m.fillRootKeys) - 1
			return func() ([]stockLocation, error) {
				return stocks[index], nil
			}
		}

		m.stubLeaseSet()

		fn1 := m.mmap.Get(context.Background(), 110,
			stockLocationRootKey{
				sku: sku1,
			},
			stockLocationKey{
				loc:  loc1,
				hash: hash1,
			},
		)
		fn2 := m.mmap.Get(context.Background(), 110,
			stockLocationRootKey{
				sku: sku2,
			},
			stockLocationKey{
				loc:  loc2,
				hash: hash2,
			},
		)
		fn3 := m.mmap.Get(context.Background(), 110,
			stockLocationRootKey{
				sku: sku2,
			},
			stockLocationKey{
				loc:  loc3,
				hash: hash3,
			},
		)

		result, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, Option[stockLocation]{
			Valid: true,
			Data:  stock1,
		}, result)

		result, err = fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, Option[stockLocation]{
			Valid: true,
			Data:  stock2,
		}, result)

		result, err = fn3()
		assert.Equal(t, nil, err)
		assert.Equal(t, Option[stockLocation]{}, result)

		calls := m.pipe.LeaseGetCalls()
		assert.Equal(t, 3, len(calls))
		assert.Equal(t, "p/stocks/SKU01:5:70", calls[0].Key)
		assert.Equal(t, "p/stocks/SKU02:5:90", calls[1].Key)
		assert.Equal(t, "p/stocks/SKU02:5:a8", calls[2].Key)

		assert.Equal(t, []stockLocationRootKey{
			{sku: sku1},
			{sku: sku2},
			{sku: sku2},
		}, m.fillRootKeys)

		assert.Equal(t, []HashRange{
			{
				Begin: 0x7000_0000_0000_0000,
				End:   0x77ff_ffff_ffff_ffff,
			},
			{
				Begin: 0x9000_0000_0000_0000,
				End:   0x97ff_ffff_ffff_ffff,
			},
			{
				Begin: 0xa800_0000_0000_0000,
				End:   0xafff_ffff_ffff_ffff,
			},
		}, m.fillHashRanges)

		setCalls := m.pipe.LeaseSetCalls()
		assert.Equal(t, 3, len(setCalls))
		assert.Equal(t, "p/stocks/SKU01:5:70", setCalls[0].Key)
		assert.Equal(t, "p/stocks/SKU02:5:90", setCalls[1].Key)
		assert.Equal(t, "p/stocks/SKU02:5:a8", setCalls[2].Key)

		assert.Equal(t, uint64(3300), setCalls[0].Cas)

		unmarshaler := NewBucketUnmarshaler(unmarshalStockLocation)

		bucket, err := unmarshaler(setCalls[0].Data)
		assert.Equal(t, nil, err)
		assert.Equal(t, Bucket[stockLocation]{
			Values: []stockLocation{stock1},
		}, bucket)

		bucket, err = unmarshaler(setCalls[1].Data)
		assert.Equal(t, nil, err)
		assert.Equal(t, Bucket[stockLocation]{
			Values: []stockLocation{stock2},
		}, bucket)
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

	t.Run("with bucket size log = 0", func(t *testing.T) {
		sizeLog := computeSizeLog(0, 0, newHash(0x0fff, 2))
		assert.Equal(t, uint8(0), sizeLog)

		sizeLog = computeSizeLog(0, 1, newHash(0x0fff, 2))
		assert.Equal(t, uint8(0), sizeLog)

		sizeLog = computeSizeLog(0, 2, newHash(0x0fff, 2))
		assert.Equal(t, uint8(1), sizeLog)

		sizeLog = computeSizeLog(0, 3, newHash(0x0fff, 2))
		assert.Equal(t, uint8(2), sizeLog)

		sizeLog = computeSizeLog(0, 3, newHash(0xffff, 2))
		assert.Equal(t, uint8(1), sizeLog)

		sizeLog = computeSizeLog(0, 8, newHash(0b1111_1111, 1))
		assert.Equal(t, uint8(3), sizeLog)

		sizeLog = computeSizeLog(0, 9, newHash(0b0001_1111, 1))
		assert.Equal(t, uint8(4), sizeLog)

		sizeLog = computeSizeLog(0, 9, newHash(0b0010_0000, 1))
		assert.Equal(t, uint8(3), sizeLog)
	})

	t.Run("with bucket size log = 0, bigger count", func(t *testing.T) {
		// count = 32
		sizeLog := computeSizeLog(0, 32, newHash(0b1111_1111, 1))
		assert.Equal(t, uint8(5), sizeLog)

		sizeLog = computeSizeLog(0, 32, newHash(0b0000_0000, 1))
		assert.Equal(t, uint8(5), sizeLog)

		// count = 33
		sizeLog = computeSizeLog(0, 33, newHash(0b0000_0111, 1))
		assert.Equal(t, uint8(6), sizeLog)

		sizeLog = computeSizeLog(0, 33, newHash(0b0000_1000, 1))
		assert.Equal(t, uint8(5), sizeLog)

		sizeLog = computeSizeLog(0, 33, newHash(0b1111_1111, 1))
		assert.Equal(t, uint8(5), sizeLog)

		// count = 40
		sizeLog = computeSizeLog(0, 40, newHash(0b0011_1111, 1))
		assert.Equal(t, uint8(6), sizeLog)

		sizeLog = computeSizeLog(0, 40, newHash(0b0100_0000, 1))
		assert.Equal(t, uint8(5), sizeLog)
	})

	t.Run("with bucket size log = 4", func(t *testing.T) {
		sizeLog := computeSizeLog(4, 0, newHash(0x0000, 2))
		assert.Equal(t, uint8(0), sizeLog)

		sizeLog = computeSizeLog(4, 16, newHash(0x0000, 2))
		assert.Equal(t, uint8(0), sizeLog)

		// count = 17
		sizeLog = computeSizeLog(4, 17, newHash(0b0000_0000, 1))
		assert.Equal(t, uint8(1), sizeLog)

		sizeLog = computeSizeLog(4, 17, newHash(0b0111_1111, 1))
		assert.Equal(t, uint8(1), sizeLog)

		sizeLog = computeSizeLog(4, 17, newHash(0b1000_0000, 1))
		assert.Equal(t, uint8(0), sizeLog)

		// count = 128
		sizeLog = computeSizeLog(4, 128, newHash(0b0000_0000, 1))
		assert.Equal(t, uint8(3), sizeLog)

		sizeLog = computeSizeLog(4, 128, newHash(0b1111_1111, 1))
		assert.Equal(t, uint8(3), sizeLog)

		// count = 129
		sizeLog = computeSizeLog(4, 129, newHash(0b0000_0000, 1))
		assert.Equal(t, uint8(4), sizeLog)

		sizeLog = computeSizeLog(4, 129, newHash(0b0000_1111, 1))
		assert.Equal(t, uint8(4), sizeLog)

		sizeLog = computeSizeLog(4, 129, newHash(0b0001_0000, 1))
		assert.Equal(t, uint8(3), sizeLog)

		sizeLog = computeSizeLog(4, 129, newHash(0b1111_1111, 1))
		assert.Equal(t, uint8(3), sizeLog)

		// count = 136
		sizeLog = computeSizeLog(4, 136, newHash(0b0000_1111, 1))
		assert.Equal(t, uint8(4), sizeLog)

		sizeLog = computeSizeLog(4, 136, newHash(0b0001_0000, 1))
		assert.Equal(t, uint8(3), sizeLog)
	})
}

func TestComputeBucketKeyString(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		s := ComputeBucketKeyStringWithSeparator(
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
		s := ComputeBucketKeyStringWithSeparator(
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

		s = ComputeBucketKeyStringWithSeparator(
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

	t.Run("middle of size log = 4", func(t *testing.T) {
		const elemCount = 96

		s := ComputeBucketKeyStringWithSeparator(
			elemCount,
			stockLocationRootKey{
				sku: "SKU01",
			},
			stockLocationKey{
				hash: newHash(0b0111_1111, 1),
			},
			"/",
		)
		assert.Equal(t, "p/stocks/SKU01/5/78", s)

		s = ComputeBucketKeyStringWithSeparator(
			elemCount,
			stockLocationRootKey{
				sku: "SKU01",
			},
			stockLocationKey{
				hash: newHash(0b1000_0000, 1),
			},
			"/",
		)
		assert.Equal(t, "p/stocks/SKU01/4/8", s)
	})
}

func TestComputeBucketKey_Hash_Diff_After_Size_Log(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		key1 := ComputeBucketKey[stockLocationRootKey, stockLocationKey](
			4*8,
			stockLocationRootKey{sku: sku1},
			stockLocationKey{
				loc:  loc1,
				hash: newHash(0b1010_0000, 1),
			},
			":",
		)
		key2 := ComputeBucketKey[stockLocationRootKey, stockLocationKey](
			4*8,
			stockLocationRootKey{sku: sku1},
			stockLocationKey{
				loc:  loc1,
				hash: newHash(0b1011_1111, 1),
			},
			":",
		)
		assert.Equal(t, newHash(0b1010_0000, 1), key2.Hash)
		assert.Equal(t, key1.Hash, key2.Hash)
		assert.Equal(t, true, key1 == key2)
	})
}
