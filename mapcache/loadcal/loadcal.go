package loadcal

// listHead for circular double linked list
type listHead struct {
	next int // = -1 => refer to root
	prev int // = -1 => refer to root
}

type mapCacheStats struct {
	countedSet [4]uint64 // 256 / 64 bit

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
	stats.countedSet = [4]uint64{}
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

var boundTable = []int{
	1,  // 0
	2,  // 1
	4,  // 2
	7,  // 3
	11, // 4
	21, // 5
	41, // 6
	81, // 7
}

func computeCountedResetBound(sizeLog SizeLog) int {
	if sizeLog.Value >= 8 {
		return 162 // 256 * (1 - 1/e) ~ 161.82286306
	}
	return boundTable[sizeLog.Value]
}
