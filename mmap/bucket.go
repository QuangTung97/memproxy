package mmap

import (
	"fmt"

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
	return fmt.Sprintf("%s%s%d%s%d", k.RootKey.String(), k.Sep, k.SizeLog, k.Sep, k.Hash)
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
