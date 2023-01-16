package mhash

import (
	"bytes"
	"encoding/binary"
	"github.com/QuangTung97/memproxy/item"
)

// Marshal ...
func (b Bucket[T]) Marshal() ([]byte, error) {
	var buf bytes.Buffer

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

// BucketUnmarshalerFromItem ...
func BucketUnmarshalerFromItem[T item.Value](unmarshaler item.Unmarshaler[T]) item.Unmarshaler[Bucket[T]] {
	return func(data []byte) (Bucket[T], error) {
		if len(data) == 0 {
			return Bucket[T]{}, nil
		}

		nextLevel := data[0]
		data = data[1:]

		// TODO Check data len

		nextLevelPrefix := binary.LittleEndian.Uint64(data)
		data = data[8:]

		itemLen, n := binary.Uvarint(data)
		// TODO check n
		data = data[n:]

		items := make([]T, 0, itemLen)
		for i := uint64(0); i < itemLen; i++ {
			dataLen, n := binary.Uvarint(data)
			// TODO Check n
			data = data[n:]

			// TODO Check data with enough len
			itemVal, err := unmarshaler(data[:dataLen])
			if err != nil {
				return Bucket[T]{}, err
			}
			items = append(items, itemVal)

			data = data[dataLen:]
		}

		// TODO Check len still enough
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
