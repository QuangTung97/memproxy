package loadcal

import "github.com/QuangTung97/memproxy/mapcache/loadcal/prob"

// listHead for circular double linked list
type listHead struct {
	next int // = -1 => refer to root
	prev int // = -1 => refer to root
}

type mapCacheStats struct {
	countedSet [16]uint64 // 256 / 64 bit

	countedBuckets int // count number of buckets used
	totalEntries   int // count number of bucket entries

	sizeLog SizeLog
}

// mapCacheLRUEntry for maintaining stats in a LRU manner
type mapCacheLRUEntry struct {
	hashNext int // single linked list of hash table

	lru listHead

	keyLen  uint8
	rootKey [127]byte

	stats mapCacheStats
}

// LoadCalculator for automatically calculating size log of map caches
type LoadCalculator struct {
	hashTable []int // store pointers to all lru entry, a single linked list by hashNext field
	lruRoot   listHead
	statsPool []mapCacheStats
}

func initMapCacheStats(stats *mapCacheStats) {
	stats.countedSet = [16]uint64{}
	stats.countedBuckets = 0
	stats.totalEntries = 0
	stats.sizeLog = SizeLog{}
}

// SizeLog ...
type SizeLog struct {
	Value   uint64
	Version uint64
}

func computeIndexAndMask(slot uint64) (int, uint64) {
	index := int(slot >> 6)  // divide to 64
	shift := slot & 0b111111 // mod to 64
	mask := uint64(1 << shift)
	return index, mask
}

func (s *mapCacheStats) alreadyCounted(slot uint64) bool {
	index, mask := computeIndexAndMask(slot)
	return (s.countedSet[index] & mask) != 0
}

func (s *mapCacheStats) setCounted(slot uint64) {
	index, mask := computeIndexAndMask(slot)
	s.countedSet[index] |= mask
}

func (s *mapCacheStats) addEntry(sizeLog SizeLog, bucketIndex uint64, bucketEntries int) {
	slot := bucketIndex & 0xff

	if s.alreadyCounted(slot) {
		return
	}
	s.setCounted(slot)

	s.countedBuckets++
	s.totalEntries += bucketEntries
	s.sizeLog = sizeLog
}

func (s *mapCacheStats) estimatedLoad() float64 {
	return float64(s.totalEntries) / float64(s.countedBuckets)
}

func (*mapCacheStats) needNewSizeLog() bool {
	return false
}

const maxSizeLog = 10

func initBucketSizeBounds() []prob.BucketSizeBound {
	result := make([]prob.BucketSizeBound, 0, maxSizeLog+1)

	bound := prob.BucketSizeBound{
		MaxCount: 1,
		Lower:    1.0,
		Upper:    4.0,
	}
	result = append(result, bound)

	bound.MaxCount = 2
	result = append(result, bound)

	for i := 2; i <= 10; i++ {
		result = append(result, prob.ComputeLowerAndUpperBound(1<<i))
	}
	return result
}

var bucketBounds = initBucketSizeBounds()

func lowerAndUpperBound(sizeLog SizeLog) prob.BucketSizeBound {
	if sizeLog.Value > maxSizeLog {
		return bucketBounds[maxSizeLog]
	}
	return bucketBounds[sizeLog.Value]
}
