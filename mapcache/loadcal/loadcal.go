package loadcal

// CheckBoundInput ...
type CheckBoundInput struct {
	TotalEntries   int // total number of entries in all buckets
	CountedBuckets int // number of buckets already counted
	TotalChecked   int // total number of checks
}

// CheckBoundOutput ...
type CheckBoundOutput struct {
	NeedReset bool
}

// AddEntryInput ...
type AddEntryInput struct {
	SizeLog       SizeLog
	BucketIndex   uint64
	BucketEntries int // number of entries in the bucket
	Checker       BoundChecker
}

//go:generate moq -rm -out loadcal_mocks_test.go . BoundChecker

// BoundChecker for checking load need increasing / decreasing
type BoundChecker interface {
	Check(input CheckBoundInput) CheckBoundOutput
}

// listHead for circular double linked list
type listHead struct {
	next int // = -1 => refer to root
	prev int // = -1 => refer to root
}

type mapCacheStats struct {
	countedSet [16]uint64 // 256 / 64 bit

	countedBuckets int // count number of buckets used
	totalEntries   int // count number of bucket entries
	totalChecked   int // number of checks

	sizeLog SizeLog
}

// mapCacheLRUEntry for maintaining stats in the LRU manner
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
	stats.totalChecked = 0
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

func (s *mapCacheStats) addEntry(entry AddEntryInput) {
	slot := entry.BucketIndex & 0x3ff

	s.sizeLog = entry.SizeLog
	s.totalChecked++

	if !s.alreadyCounted(slot) {
		s.setCounted(slot)

		s.countedBuckets++
		s.totalEntries += entry.BucketEntries
	}

	output := entry.Checker.Check(CheckBoundInput{
		TotalEntries:   s.totalEntries,
		CountedBuckets: s.countedBuckets,
		TotalChecked:   s.totalChecked,
	})
	if output.NeedReset {
		initMapCacheStats(s)
	}
}
