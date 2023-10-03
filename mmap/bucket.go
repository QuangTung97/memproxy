package mmap

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"strconv"
	"strings"

	"github.com/QuangTung97/memproxy/item"
)

// BucketKey ...
type BucketKey[R RootKey] struct {
	RootKey R
	SizeLog uint8
	Hash    uint64
	Sep     string // separator
}

// String ...
func (k BucketKey[R]) String() string {
	var buf strings.Builder

	buf.WriteString(k.RootKey.String())
	buf.WriteString(k.Sep)
	buf.WriteString(strconv.FormatInt(int64(k.SizeLog), 10))
	buf.WriteString(k.Sep)

	hash := k.Hash & (math.MaxUint64 << (64 - k.SizeLog))

	var data [8]byte
	binary.BigEndian.PutUint64(data[:], hash)

	numBytes := (k.SizeLog + 7) >> 3
	hexStr := hex.EncodeToString(data[:numBytes])

	numDigits := (k.SizeLog + 3) >> 2
	if numDigits&0b1 != 0 {
		hexStr = hexStr[:len(hexStr)-1]
	}

	buf.WriteString(hexStr)

	return buf.String()
}

// GetHashRange ...
func (k BucketKey[R]) GetHashRange() HashRange {
	return HashRange{}
}

// Bucket ...
type Bucket[T Value] struct {
	Values []T
}

// Marshal ...
func (b Bucket[T]) Marshal() ([]byte, error) {
	return nil, nil
}

// NewBucketUnmarshaler ...
func NewBucketUnmarshaler[T Value](
	unmarshaler item.Unmarshaler[T],
) func(data []byte) (Bucket[T], error) {
	return func(data []byte) (Bucket[T], error) {
		return Bucket[T]{}, nil
	}
}
