package mapcache

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"github.com/spaolacci/murmur3"
	"math"
)

func hashFunc(key string) uint64 {
	return murmur3.Sum64([]byte(key))
}

func computeMask(sizeLog uint64) uint64 {
	return uint64(math.MaxUint64) << (64 - sizeLog)
}

func computeBucketKey(keyHash uint64, sizeLog uint64) string {
	mask := computeMask(sizeLog)

	h := keyHash
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

func computeHashRange(hash uint64, sizeLog uint64) HashRange {
	mask := computeMask(sizeLog)
	reverseMask := ^mask

	begin := hash & mask
	return HashRange{
		Begin: begin,
		End:   begin | (math.MaxUint64 & reverseMask),
	}
}

func putIntNumber(buf *bytes.Buffer, num int) {
	var dataLen [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(dataLen[:], uint64(num))
	buf.Write(dataLen[:size])
}

func parseIntNumber(data []byte) ([]byte, int, error) {
	num, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, 0, ErrMissingLength
	}
	return data[n:], int(num), nil
}

const version = 1

func marshalCacheBucket(bucket CacheBucketContent) []byte {
	var buf bytes.Buffer
	buf.WriteByte(version)

	var origin [8]byte
	binary.LittleEndian.PutUint64(origin[:], bucket.OriginSizeLogVersion)
	buf.Write(origin[:])

	putIntNumber(&buf, len(bucket.Entries))

	for _, entry := range bucket.Entries {
		keyBytes := []byte(entry.Key)
		putIntNumber(&buf, len(keyBytes))
		buf.Write(keyBytes)

		putIntNumber(&buf, len(entry.Data))
		buf.Write(entry.Data)
	}

	return buf.Bytes()
}

func cloneBytes(data []byte) []byte {
	result := make([]byte, len(data))
	copy(result, data)
	return result
}

func unmarshalCacheBucket(data []byte) (CacheBucketContent, error) {
	if len(data) == 0 {
		return CacheBucketContent{}, ErrMissingBucketContent
	}

	if data[0] != version {
		return CacheBucketContent{}, ErrInvalidBucketContentVersion
	}
	data = data[1:]

	if len(data) < 8 {
		return CacheBucketContent{}, ErrMissingSizeLogOrigin
	}

	origin := binary.LittleEndian.Uint64(data)
	data = data[8:]

	data, numEntries, err := parseIntNumber(data)
	if err != nil {
		return CacheBucketContent{}, err
	}

	entries := make([]Entry, 0, numEntries)
	for i := 0; i < numEntries; i++ {
		var entry Entry

		var keyLen int
		data, keyLen, err = parseIntNumber(data)
		if err != nil {
			return CacheBucketContent{}, err
		}

		entry.Key = string(data[:keyLen])
		data = data[keyLen:]

		var dataLen int
		data, dataLen, err = parseIntNumber(data)
		if err != nil {
			return CacheBucketContent{}, err
		}

		entry.Data = cloneBytes(data[:dataLen])
		data = data[dataLen:]

		entries = append(entries, entry)
	}

	return CacheBucketContent{
		OriginSizeLogVersion: origin,
		Entries:              entries,
	}, nil
}
