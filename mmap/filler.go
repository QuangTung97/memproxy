package mmap

import (
	"context"
)

// FillKey ...
type FillKey[R comparable] struct {
	RootKey R
	Range   HashRange
}

type multiGetState[T any, R comparable] struct {
	keys   []FillKey[R]
	result map[R][]T
	err    error
}

// NewMultiGetFiller converts from function often using SELECT WHERE IN
// into a Filler[T, R] that allow to be passed to New
func NewMultiGetFiller[T any, R comparable](
	multiGetFunc func(ctx context.Context, keys []FillKey[R]) ([]T, error),
	getRootKey func(v T) R,
) Filler[T, R] {
	var state *multiGetState[T, R]

	return func(ctx context.Context, rootKey R, hashRange HashRange) func() ([]T, error) {
		if state == nil {
			state = &multiGetState[T, R]{
				result: map[R][]T{},
			}
		}
		s := state
		s.keys = append(s.keys, FillKey[R]{
			RootKey: rootKey,
			Range:   hashRange,
		})

		return func() ([]T, error) {
			if state != nil {
				state = nil

				values, err := multiGetFunc(ctx, s.keys)
				if err != nil {
					s.err = err
				} else {
					for _, v := range values {
						rootKey := getRootKey(v)
						prev := s.result[rootKey]
						s.result[rootKey] = append(prev, v)
					}
				}
			}

			if s.err != nil {
				return nil, s.err
			}

			result := s.result[rootKey]
			return result, nil
		}
	}
}
