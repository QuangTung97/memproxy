package mhash

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/QuangTung97/memproxy/item"
)

const binaryVersion = 1

// Marshal ...
func (b Bucket[T]) Marshal() ([]byte, error) {
	var buf bytes.Buffer

	_ = buf.WriteByte(binaryVersion)

	_ = buf.WriteByte(b.NextLevel)

	var prefix [8]byte
	binary.LittleEndian.PutUint64(prefix[:], b.NextLevelPrefix)
	_, _ = buf.Write(prefix[:])

	var itemLenBytes [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(itemLenBytes[:], uint64(len(b.Items)))

	_, _ = buf.Write(itemLenBytes[:n])

	var dataLen [binary.MaxVarintLen64]byte

	for _, bucketItem := range b.Items {
		data, err := bucketItem.Marshal()
		if err != nil {
			return nil, err
		}

		n = binary.PutUvarint(dataLen[:], uint64(len(data)))
		_, _ = buf.Write(dataLen[:n])

		_, _ = buf.Write(data)
	}

	_, _ = buf.Write(b.Bitset[:])

	return buf.Bytes(), nil
}

func unmarshalerError(err string) error {
	return errors.New("mhash unmarshaler: " + err)
}

// BucketUnmarshalerFromItem ...
//
//revive:disable-next-line:cognitive-complexity
func BucketUnmarshalerFromItem[T item.Value](unmarshaler item.Unmarshaler[T]) item.Unmarshaler[Bucket[T]] {
	return func(data []byte) (Bucket[T], error) {
		if len(data) == 0 {
			return Bucket[T]{}, nil
		}

		v := data[0]
		if v > binaryVersion {
			return Bucket[T]{}, unmarshalerError("version too big")
		}
		data = data[1:]

		if len(data) == 0 {
			return Bucket[T]{}, unmarshalerError("missing next level byte")
		}
		nextLevel := data[0]
		data = data[1:]

		if len(data) < 8 {
			return Bucket[T]{}, unmarshalerError("missing next level prefix")
		}
		nextLevelPrefix := binary.LittleEndian.Uint64(data)
		data = data[8:]

		itemLen, n := binary.Uvarint(data)
		if n <= 0 {
			return Bucket[T]{}, unmarshalerError("invalid item len")
		}
		data = data[n:]

		items := make([]T, 0, itemLen)
		for i := uint64(0); i < itemLen; i++ {
			dataLen, n := binary.Uvarint(data)
			if n <= 0 {
				return Bucket[T]{}, unmarshalerError("invalid data len")
			}
			data = data[n:]

			if uint64(len(data)) < dataLen {
				return Bucket[T]{}, unmarshalerError("invalid data bytes length")
			}
			itemVal, err := unmarshaler(data[:dataLen])
			if err != nil {
				return Bucket[T]{}, err
			}
			items = append(items, itemVal)

			data = data[dataLen:]
		}

		if len(data) < bitSetBytes {
			return Bucket[T]{}, unmarshalerError("missing bitset data")
		}
		var bitSet BitSet
		copy(bitSet[:], data)

		return Bucket[T]{
			NextLevel:       nextLevel,
			NextLevelPrefix: nextLevelPrefix,

			Items:  items,
			Bitset: bitSet,
		}, nil
	}
}
