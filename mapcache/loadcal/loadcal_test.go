package loadcal

import (
	"github.com/QuangTung97/memproxy/mapcache/loadcal/prob"
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestSizeOfMapCacheBuckets(t *testing.T) {
	n := unsafe.Sizeof(mapCacheLRUEntry{})
	assert.Equal(t, 312, int(n))

	n = unsafe.Sizeof(mapCacheStats{})
	assert.Equal(t, 160, int(n))
}

func newStats() *mapCacheStats {
	stats := &mapCacheStats{}
	initMapCacheStats(stats)
	return stats
}

func TestMapCacheStats(t *testing.T) {
	s := newStats()

	sizeLog := SizeLog{
		Value:   7,
		Version: 61,
	}
	s.addEntry(sizeLog, 0, 3)

	assert.Equal(t, 1, s.countedBuckets)
	assert.Equal(t, 3, s.totalEntries)
	assert.Equal(t, sizeLog, s.sizeLog)

	// Add New with Another Hash
	s.addEntry(sizeLog, 1, 4)
	assert.Equal(t, 2, s.countedBuckets)
	assert.Equal(t, 7, s.totalEntries)
	assert.Equal(t, sizeLog, s.sizeLog)

	// Add Entry with The Same Hash
	s.addEntry(sizeLog, 0, 2)
	assert.Equal(t, 2, s.countedBuckets)
	assert.Equal(t, 7, s.totalEntries)

	// Add Entry with The Highest Slot
	s.addEntry(sizeLog, 255, 7)
	assert.Equal(t, 3, s.countedBuckets)
	assert.Equal(t, 14, s.totalEntries)

	// Add Entry with Slot Wrap Around
	s.addEntry(sizeLog, 256, 5)
	assert.Equal(t, 3, s.countedBuckets)
	assert.Equal(t, 14, s.totalEntries)

	// Add Entry with The Middle Slot
	s.addEntry(sizeLog, 127, 3)
	assert.Equal(t, 4, s.countedBuckets)
	assert.Equal(t, 17, s.totalEntries)
	assert.Equal(t, sizeLog, s.sizeLog)

	assert.Equal(t, float64(17)/4, s.estimatedLoad())
}

func TestMapCacheStats__NeedNewSizeLog(_ *testing.T) {
	s := newStats()

	sizeLog := SizeLog{
		Value:   1,
		Version: 61,
	}
	s.addEntry(sizeLog, 0, 3)
}

func TestLowerAndUpperBound(t *testing.T) {
	var bound prob.BucketSizeBound
	bound = lowerAndUpperBound(SizeLog{
		Value: 8,
	})
	assert.Equal(t, prob.ComputeLowerAndUpperBound(256), bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 7,
	})
	assert.Equal(t, prob.ComputeLowerAndUpperBound(128), bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 2,
	})
	assert.Equal(t, prob.ComputeLowerAndUpperBound(4), bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 1,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 2,
		Lower:    1.0,
		Upper:    4.0,
	}, bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 0,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 1,
		Lower:    1.0,
		Upper:    4.0,
	}, bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 10,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 648,
		Upper:    4.341911300652749,
		Lower:    0.8362045709310009,
	}, bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 11,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 648,
		Upper:    4.341911300652749,
		Lower:    0.8362045709310009,
	}, bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 12,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 648,
		Upper:    4.341911300652749,
		Lower:    0.8362045709310009,
	}, bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 5,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 23,
		Lower:    0.71875,
		Upper:    5.565217391304348,
	}, bound)
}
