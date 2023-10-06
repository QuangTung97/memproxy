package mmap

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/google/btree"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/proxy"
)

type primaryKey struct {
	rootKey stockLocationRootKey
	loc     string
}

type indexKey struct {
	rootKey stockLocationRootKey
	hash    uint64
	loc     string
}

type mapPropertyTest struct {
	mc memproxy.Memcache

	mut sync.Mutex

	stockMap      map[primaryKey]stockLocation
	stockCounters map[string]uint64
	stockIndex    *btree.BTreeG[indexKey]
}

func indexKeyLess(a, b indexKey) bool {
	if a.rootKey.sku < b.rootKey.sku {
		return true
	}
	if a.rootKey.sku > b.rootKey.sku {
		return false
	}
	if a.hash < b.hash {
		return true
	}
	if a.hash > b.hash {
		return false
	}
	return a.loc < b.loc
}

func newMapPropertyTest(
	t *testing.T,
) *mapPropertyTest {
	return &mapPropertyTest{
		mc: newMemcacheWithProxy(t),

		stockMap:      make(map[primaryKey]stockLocation),
		stockCounters: map[string]uint64{},
		stockIndex:    btree.NewG[indexKey](3, indexKeyLess),
	}
}

func (s stockLocation) getPrimaryKey() primaryKey {
	return primaryKey{
		rootKey: s.getRootKey(),
		loc:     s.Location,
	}
}

func resetStockHash(s *stockLocation) {
	s.Hash = murmur3.Sum64([]byte(s.Location))
}

func (m *mapPropertyTest) putStock(stock stockLocation) {
	resetStockHash(&stock)

	m.mut.Lock()

	primary := stock.getPrimaryKey()
	prev, ok := m.stockMap[primary]
	if ok {
		prevIndex := indexKey{
			rootKey: prev.getRootKey(),
			hash:    prev.Hash,
			loc:     prev.Location,
		}
		m.stockIndex.Delete(prevIndex)
	} else {
		m.stockCounters[primary.rootKey.sku] = m.stockCounters[primary.rootKey.sku] + 1
	}

	index := indexKey{
		rootKey: stock.getRootKey(),
		hash:    stock.Hash,
		loc:     stock.Location,
	}
	m.stockIndex.ReplaceOrInsert(index)

	m.stockMap[primary] = stock

	newCounter := m.stockCounters[primary.rootKey.sku]

	m.mut.Unlock()

	pipe := m.mc.Pipeline(context.Background())
	defer pipe.Finish()

	cacheKey := ComputeBucketKeyString[stockLocationRootKey, stockLocationKey](
		newCounter,
		stock.getRootKey(),
		stock.getKey(),
	)
	fmt.Println(cacheKey)

	fn := pipe.Delete(cacheKey, memproxy.DeleteOptions{})
	_, err := fn()
	if err != nil {
		panic(err)
	}
}

func (m *mapPropertyTest) getCounter(rootKey stockLocationRootKey) uint64 {
	m.mut.Lock()
	defer m.mut.Unlock()

	return m.stockCounters[rootKey.sku]
}

func (m *mapPropertyTest) getStocksByHashes(
	_ context.Context, keys []FillKey[stockLocationRootKey],
) ([]stockLocation, error) {
	m.mut.Lock()
	defer m.mut.Unlock()

	var result []stockLocation
	for _, k := range keys {
		beginKey := indexKey{
			rootKey: k.RootKey,
			hash:    k.Range.Begin,
		}
		endKey := indexKey{
			rootKey: k.RootKey,
			hash:    k.Range.End + 1,
		}

		collectFn := func(item indexKey) bool {
			result = append(result, m.stockMap[primaryKey{
				rootKey: item.rootKey,
				loc:     item.loc,
			}])
			return true
		}

		if k.Range.End == math.MaxUint64 {
			endKey.rootKey.sku = k.RootKey.sku + "\000"
		}
		m.stockIndex.AscendRange(beginKey, endKey, collectFn)
	}
	return result, nil
}

func (m *mapPropertyTest) newMap(pipe memproxy.Pipeline) *Map[stockLocation, stockLocationRootKey, stockLocationKey] {
	filler := NewMultiGetFiller(m.getStocksByHashes, stockLocation.getRootKey, stockLocation.getKey)
	return New[stockLocation, stockLocationRootKey, stockLocationKey](
		pipe,
		unmarshalStockLocation,
		filler,
		stockLocation.getKey,
	)
}

func clearMemcache() {
	client, err := memcache.New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	pipe := client.Pipeline()
	defer pipe.Finish()

	err = pipe.FlushAll()()
	if err != nil {
		panic(err)
	}
}

func newMemcacheWithProxy(t *testing.T) memproxy.Memcache {
	clearMemcache()

	server1 := proxy.SimpleServerConfig{
		ID:   1,
		Host: "localhost",
		Port: 11211,
	}

	servers := []proxy.SimpleServerConfig{server1}
	mc, closeFunc, err := proxy.NewSimpleReplicatedMemcache(servers, 1, proxy.NewSimpleStats(servers))
	if err != nil {
		panic(err)
	}
	t.Cleanup(closeFunc)

	if err != nil {
		panic(err)
	}

	return mc
}

func TestMapPropertyTest_PutAndGetStocks(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		m := newMapPropertyTest(t)

		result, _ := m.getStocksByHashes(context.Background(), []FillKey[stockLocationRootKey]{
			{
				RootKey: stockLocationRootKey{
					sku: "SKU01",
				},
				Range: HashRange{
					Begin: newHash(0x00, 1),
					End:   math.MaxUint64,
				},
			},
		})
		assert.Equal(t, 0, len(result))
	})

	t.Run("multi", func(t *testing.T) {
		m := newMapPropertyTest(t)

		stock1 := stockLocation{
			Sku:      "SKU01",
			Location: "LOC01",
			Quantity: 41,
		}
		stock2 := stockLocation{
			Sku:      "SKU02",
			Location: "LOC02",
			Quantity: 42,
		}
		stock3 := stockLocation{
			Sku:      "SKU01",
			Location: "LOC03",
			Quantity: 43,
		}

		m.putStock(stock1)
		m.putStock(stock2)
		m.putStock(stock3)

		assert.Equal(t, map[string]uint64{
			"SKU01": 2,
			"SKU02": 1,
		}, m.stockCounters)

		resetStockHash(&stock1)
		resetStockHash(&stock2)
		resetStockHash(&stock3)

		result, _ := m.getStocksByHashes(context.Background(), []FillKey[stockLocationRootKey]{
			{
				RootKey: stockLocationRootKey{
					sku: "SKU01",
				},
				Range: HashRange{
					Begin: newHash(0x00, 1),
					End:   math.MaxUint64,
				},
			},
		})
		assert.Equal(t, []stockLocation{
			stock3,
			stock1,
		}, result)

		// SKU02
		result, _ = m.getStocksByHashes(context.Background(), []FillKey[stockLocationRootKey]{
			{
				RootKey: stockLocationRootKey{
					sku: "SKU02",
				},
				Range: HashRange{
					Begin: newHash(0x00, 1),
					End:   math.MaxUint64,
				},
			},
		})
		assert.Equal(t, []stockLocation{
			stock2,
		}, result)

		// SKU03
		result, _ = m.getStocksByHashes(context.Background(), []FillKey[stockLocationRootKey]{
			{
				RootKey: stockLocationRootKey{
					sku: "SKU03",
				},
				Range: HashRange{
					Begin: newHash(0x00, 1),
					End:   math.MaxUint64,
				},
			},
		})
		assert.Equal(t, 0, len(result))

		// SKU01 with hash range
		result, _ = m.getStocksByHashes(context.Background(), []FillKey[stockLocationRootKey]{
			{
				RootKey: stockLocationRootKey{
					sku: "SKU01",
				},
				Range: HashRange{
					Begin: stock3.Hash,
					End:   stock3.Hash,
				},
			},
		})
		assert.Equal(t, []stockLocation{
			stock3,
		}, result)

		// SKU01 with hash range to end
		result, _ = m.getStocksByHashes(context.Background(), []FillKey[stockLocationRootKey]{
			{
				RootKey: stockLocationRootKey{
					sku: "SKU01",
				},
				Range: HashRange{
					Begin: stock3.Hash + 1,
					End:   stock1.Hash,
				},
			},
		})
		assert.Equal(t, []stockLocation{
			stock1,
		}, result)
	})
}

func TestMapPropertyTest_PutStock(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		m := newMapPropertyTest(t)
		assert.Equal(t, 0, len(m.stockMap))
		assert.Equal(t, 0, m.stockIndex.Len())
		assert.Equal(t, 0, len(m.stockCounters))
	})

	t.Run("single", func(t *testing.T) {
		m := newMapPropertyTest(t)

		stock1 := stockLocation{
			Sku:      "SKU01",
			Location: "LOC01",
			Quantity: 41,
		}
		m.putStock(stock1)

		resetStockHash(&stock1)

		assert.Equal(t, map[primaryKey]stockLocation{
			primaryKey{
				rootKey: stockLocationRootKey{
					sku: "SKU01",
				},
				loc: "LOC01",
			}: stock1,
		}, m.stockMap)

		assert.Equal(t, map[string]uint64{
			"SKU01": 1,
		}, m.stockCounters)

		assert.Equal(t, 1, m.stockIndex.Len())

		var indexKeys []indexKey
		m.stockIndex.Ascend(func(item indexKey) bool {
			indexKeys = append(indexKeys, item)
			return true
		})
		assert.Equal(t, []indexKey{
			{
				rootKey: stock1.getRootKey(),
				hash:    stock1.Hash,
				loc:     stock1.Location,
			},
		}, indexKeys)

		// Do update
		stock1.Quantity = 42
		m.putStock(stock1)

		// check again
		assert.Equal(t, map[primaryKey]stockLocation{
			primaryKey{
				rootKey: stockLocationRootKey{
					sku: "SKU01",
				},
				loc: "LOC01",
			}: stock1,
		}, m.stockMap)

		assert.Equal(t, map[string]uint64{
			"SKU01": 1,
		}, m.stockCounters)

		assert.Equal(t, 1, m.stockIndex.Len())

		indexKeys = nil
		m.stockIndex.Ascend(func(item indexKey) bool {
			indexKeys = append(indexKeys, item)
			return true
		})
		assert.Equal(t, []indexKey{
			{
				rootKey: stock1.getRootKey(),
				hash:    stock1.Hash,
				loc:     stock1.Location,
			},
		}, indexKeys)
	})
}

func TestIndexKeyLess(t *testing.T) {
	t.Run("all", func(t *testing.T) {
		a := indexKey{
			rootKey: stockLocationRootKey{
				sku: "SKU01",
			},
			hash: 11,
			loc:  "LOC01",
		}
		b := indexKey{
			rootKey: stockLocationRootKey{
				sku: "SKU02",
			},
			hash: 12,
			loc:  "LOC02",
		}

		assert.Equal(t, true, indexKeyLess(a, b))
		assert.Equal(t, false, indexKeyLess(b, a))
	})

	t.Run("same sku", func(t *testing.T) {
		a := indexKey{
			rootKey: stockLocationRootKey{
				sku: "SKU01",
			},
			hash: 11,
			loc:  "LOC02",
		}
		b := indexKey{
			rootKey: stockLocationRootKey{
				sku: "SKU01",
			},
			hash: 12,
			loc:  "LOC01",
		}

		assert.Equal(t, true, indexKeyLess(a, b))
		assert.Equal(t, false, indexKeyLess(b, a))
	})

	t.Run("same sku and hash", func(t *testing.T) {
		a := indexKey{
			rootKey: stockLocationRootKey{
				sku: "SKU01",
			},
			hash: 12,
			loc:  "LOC01",
		}
		b := indexKey{
			rootKey: stockLocationRootKey{
				sku: "SKU01",
			},
			hash: 12,
			loc:  "LOC02",
		}

		assert.Equal(t, true, indexKeyLess(a, b))
		assert.Equal(t, false, indexKeyLess(b, a))
	})

	t.Run("same all", func(t *testing.T) {
		a := indexKey{
			rootKey: stockLocationRootKey{
				sku: "SKU01",
			},
			hash: 12,
			loc:  "LOC01",
		}

		assert.Equal(t, false, indexKeyLess(a, a))
	})
}
