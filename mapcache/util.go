package mapcache

import (
	"encoding/binary"
	"encoding/hex"
	"github.com/spaolacci/murmur3"
	"math"
)

func hashFunc(key string) uint64 {
	return murmur3.Sum64([]byte(key))
}

func computeBucketKey(key string, sizeLog uint64) string {
	mask := uint64(math.MaxUint64) << (64 - sizeLog)

	h := hashFunc(key)
	h = h & mask

	removeTrailing := ((sizeLog+3)>>2)&0b1 == 1
	if removeTrailing {
		h >>= 4
	}

	numBytes := (sizeLog + 7) / 8

	var data [8]byte
	binary.BigEndian.PutUint64(data[:], h)
	result := hex.EncodeToString(data[:numBytes])
	if removeTrailing {
		return result[1:]
	}
	return result
}
