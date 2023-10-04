package mmap

import (
	"context"
	"sort"
)

// FillKey ...
type FillKey[R comparable] struct {
	RootKey R
	Range   HashRange
}

// NewMultiGetFiller converts from function often using SELECT WHERE IN
// into a Filler[T, R] that allow to be passed to New
func NewMultiGetFiller[T any, R comparable, K Key](
	multiGetFunc func(ctx context.Context, keys []FillKey[R]) ([]T, error),
	getRootKey func(v T) R,
	getKey func(v T) K,
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
					collectStateValues(s, values, getRootKey, getKey)
				}
			}

			if s.err != nil {
				return nil, s.err
			}

			valuesByRootKey := s.result[rootKey]
			lowerBound := findLowerBound(valuesByRootKey, getKey, hashRange.Begin)

			return computeValuesInHashRange(valuesByRootKey, lowerBound, hashRange, getKey), nil
		}
	}
}

type multiGetState[T any, R comparable] struct {
	keys   []FillKey[R]
	result map[R][]T
	err    error
}

func findLowerBound[T any, K Key](
	values []T,
	getKey func(v T) K,
	lowerBound uint64,
) int {
	// similar to std::lower_bound of C++
	first := 0
	last := len(values)
	for first != last {
		mid := (first + last) / 2

		val := values[mid]
		if getKey(val).Hash() >= lowerBound {
			last = mid
		} else {
			first = mid + 1
		}
	}
	return first
}

func computeValuesInHashRange[T any, K Key](
	values []T,
	lowerBound int,
	hashRange HashRange,
	getKey func(T) K,
) []T {
	var result []T
	for i := lowerBound; i < len(values); i++ {
		v := values[i]
		if getKey(v).Hash() > hashRange.End {
			break
		}
		result = append(result, v)
	}
	return result
}

func collectStateValues[T any, R comparable, K Key](
	s *multiGetState[T, R],
	values []T,
	getRootKey func(T) R,
	getKey func(T) K,
) {
	for _, v := range values {
		rootKey := getRootKey(v)
		prev := s.result[rootKey]
		s.result[rootKey] = append(prev, v)
	}

	// sort by hash
	for _, v := range s.result {
		sort.Slice(v, func(i, j int) bool {
			return getKey(v[i]).Hash() < getKey(v[j]).Hash()
		})
	}
}
