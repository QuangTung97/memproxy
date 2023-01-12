package mhash

import (
	"bytes"
	"encoding/binary"
	"github.com/QuangTung97/memproxy/item"
)

// Marshal ...
func (b Bucket[T]) Marshal() ([]byte, error) {
	var buf bytes.Buffer

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
			Items:  items,
			Bitset: bitSet,
		}, nil
	}
}
