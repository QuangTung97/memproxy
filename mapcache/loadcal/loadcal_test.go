package loadcal

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestSizeOfMapCacheBuckets(t *testing.T) {
	n := unsafe.Sizeof(mapCacheLRUEntry{})
	assert.Equal(t, 216, int(n))

	n = unsafe.Sizeof(mapCacheStats{})
	assert.Equal(t, 64, int(n))
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

func TestComputeCountedResetBound(t *testing.T) {
	bound := computeCountedResetBound(SizeLog{
		Value: 10,
	})
	assert.Equal(t, 162, bound)

	bound = computeCountedResetBound(SizeLog{
		Value: 7,
	})
	assert.Equal(t, 81, bound)
}
