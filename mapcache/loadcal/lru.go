package loadcal

// listHead for circular double linked list
type listHead struct {
	next int // = -1 => refer to root
	prev int // = -1 => refer to root
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
