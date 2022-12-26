package loadcal

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestSizeOfMapCacheBuckets(t *testing.T) {
	n := unsafe.Sizeof(mapCacheLRUEntry{})
	assert.Equal(t, 320, int(n))

	n = unsafe.Sizeof(mapCacheStats{})
	assert.Equal(t, 168, int(n))
}

type mapCacheStatsTest struct {
	stats   *mapCacheStats
	checker *BoundCheckerMock
}

func newStatsTest() *mapCacheStatsTest {
	stats := &mapCacheStats{}
	initMapCacheStats(stats)

	checker := &BoundCheckerMock{
		CheckFunc: func(input CheckBoundInput) CheckBoundOutput {
			return CheckBoundOutput{
				NeedReset: false,
			}
		},
	}

	return &mapCacheStatsTest{
		stats:   stats,
		checker: checker,
	}
}

func (s *mapCacheStatsTest) addEntry(sizeLog uint64, bucketIndex uint64, entries int) {
	s.stats.addEntry(AddEntryInput{
		SizeLog: SizeLog{
			Value:   sizeLog,
			Version: 71,
		},
		BucketIndex:   bucketIndex,
		BucketEntries: entries,
		Checker:       s.checker,
	})
}

func (s *mapCacheStatsTest) addEntryVersion(
	sizeLog SizeLog,
	bucketIndex uint64, entries int,
) {
	s.stats.addEntry(AddEntryInput{
		SizeLog:       sizeLog,
		BucketIndex:   bucketIndex,
		BucketEntries: entries,
		Checker:       s.checker,
	})
}

func TestMapCacheStats(t *testing.T) {
	s := newStatsTest()

	sizeLog := uint64(7)

	s.addEntry(sizeLog, 0, 3)

	assert.Equal(t, 1, s.stats.countedBuckets)
	assert.Equal(t, 3, s.stats.totalEntries)
	assert.Equal(t, sizeLog, s.stats.sizeLog.Value)

	// Add New with Another Hash
	s.addEntry(sizeLog, 1, 4)
	assert.Equal(t, 2, s.stats.countedBuckets)
	assert.Equal(t, 7, s.stats.totalEntries)
	assert.Equal(t, sizeLog, s.stats.sizeLog.Value)

	// Add Entry with The Same Hash
	s.addEntry(sizeLog, 0, 2)
	assert.Equal(t, 2, s.stats.countedBuckets)
	assert.Equal(t, 7, s.stats.totalEntries)

	// Add Entry with The Highest Slot
	s.addEntry(sizeLog, 1023, 7)
	assert.Equal(t, 3, s.stats.countedBuckets)
	assert.Equal(t, 14, s.stats.totalEntries)

	// Add Entry with Slot Wrap Around
	s.addEntry(sizeLog, 1024, 5)
	assert.Equal(t, 3, s.stats.countedBuckets)
	assert.Equal(t, 14, s.stats.totalEntries)

	// Add Entry with The Middle Slot
	s.addEntry(sizeLog, 512, 3)
	assert.Equal(t, 4, s.stats.countedBuckets)
	assert.Equal(t, 17, s.stats.totalEntries)
	assert.Equal(t, sizeLog, s.stats.sizeLog.Value)
}

func TestMapCacheStats__ResetSizeLog(t *testing.T) {
	s := newStatsTest()

	s.checker.CheckFunc = func(input CheckBoundInput) CheckBoundOutput {
		calls := s.checker.CheckCalls()
		if len(calls) > 1 {
			return CheckBoundOutput{
				NeedReset: true,
			}
		}
		return CheckBoundOutput{
			NeedReset: false,
		}
	}

	sizeLog := SizeLog{
		Value:   1,
		Version: 61,
	}
	s.addEntryVersion(sizeLog, 0, 6)
	s.addEntryVersion(sizeLog, 1, 7)

	checkCalls := s.checker.CheckCalls()

	assert.Equal(t, 2, len(checkCalls))

	assert.Equal(t, CheckBoundInput{
		TotalChecked:   1,
		TotalEntries:   6,
		CountedBuckets: 1,
	}, checkCalls[0].Input)

	assert.Equal(t, CheckBoundInput{
		TotalChecked:   2,
		TotalEntries:   13,
		CountedBuckets: 2,
	}, checkCalls[1].Input)

	s.addEntryVersion(sizeLog, 2, 3)

	checkCalls = s.checker.CheckCalls()
	assert.Equal(t, 3, len(checkCalls))
	assert.Equal(t, CheckBoundInput{
		TotalChecked:   1,
		TotalEntries:   3,
		CountedBuckets: 1,
	}, checkCalls[2].Input)
}

func TestMapCacheStats__Call_Check_After_Same_Bucket_Index(t *testing.T) {
	s := newStatsTest()

	sizeLog := SizeLog{
		Value:   1,
		Version: 61,
	}
	s.addEntryVersion(sizeLog, 0, 6)
	s.addEntryVersion(sizeLog, 1, 7)
	s.addEntryVersion(sizeLog, 1, 3)

	checkCalls := s.checker.CheckCalls()

	assert.Equal(t, 3, len(checkCalls))

	assert.Equal(t, CheckBoundInput{
		TotalChecked:   3,
		TotalEntries:   13,
		CountedBuckets: 2,
	}, checkCalls[2].Input)
}
