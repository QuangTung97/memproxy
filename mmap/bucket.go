package mmap

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
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

	_, _ = buf.WriteString(k.RootKey.String())
	_, _ = buf.WriteString(k.Sep)
	_, _ = buf.WriteString(strconv.FormatInt(int64(k.SizeLog), 10))
	_, _ = buf.WriteString(k.Sep)

	hash := k.Hash & (math.MaxUint64 << (64 - k.SizeLog))

	var data [8]byte
	binary.BigEndian.PutUint64(data[:], hash)

	numBytes := (k.SizeLog + 7) >> 3
	hexStr := hex.EncodeToString(data[:numBytes])

	numDigits := (k.SizeLog + 3) >> 2
	if numDigits&0b1 != 0 {
		hexStr = hexStr[:len(hexStr)-1]
	}

	_, _ = buf.WriteString(hexStr)

	return buf.String()
}

// GetHashRange ...
func (k BucketKey[R]) GetHashRange() HashRange {
	mask := uint64(math.MaxUint64) << (64 - k.SizeLog)

	begin := k.Hash & mask
	return HashRange{
		Begin: begin,
		End:   begin | ^mask,
	}
}

// Bucket ...
type Bucket[T Value] struct {
	Values []T
}

func putLength(buf *bytes.Buffer, length int) {
	var lenBytes [binary.MaxVarintLen64]byte

	n := binary.PutUvarint(lenBytes[:], uint64(length))
	_, _ = buf.Write(lenBytes[:n])
}

// Marshal ...
func (b Bucket[T]) Marshal() ([]byte, error) {
	var buf bytes.Buffer

	putLength(&buf, len(b.Values))

	for _, v := range b.Values {
		data, err := v.Marshal()
		if err != nil {
			return nil, err
		}

		putLength(&buf, len(data))
		_, _ = buf.Write(data)
	}

	return buf.Bytes(), nil
}

// NewBucketUnmarshaler ...
func NewBucketUnmarshaler[T Value](
	unmarshaler item.Unmarshaler[T],
) func(data []byte) (Bucket[T], error) {
	return func(data []byte) (Bucket[T], error) {
		numValues, n := binary.Uvarint(data)
		if n <= 0 {
			return Bucket[T]{}, errors.New("mmap bucket: invalid number of values")
		}
		data = data[n:]

		values := make([]T, 0, numValues)

		for i := uint64(0); i < numValues; i++ {
			numBytes, n := binary.Uvarint(data)
			if n <= 0 {
				return Bucket[T]{}, errors.New("mmap bucket: invalid length number of data")
			}
			data = data[n:]

			if len(data) < int(numBytes) {
				return Bucket[T]{}, errors.New("mmap bucket: invalid data")
			}

			value, err := unmarshaler(data[:numBytes])
			if err != nil {
				return Bucket[T]{}, err
			}
			values = append(values, value)

			data = data[numBytes:]
		}

		return Bucket[T]{
			Values: values,
		}, nil
	}
}
