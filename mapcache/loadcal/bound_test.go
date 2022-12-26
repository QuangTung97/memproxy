package loadcal

import (
	"github.com/QuangTung97/memproxy/mapcache/loadcal/prob"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLowerAndUpperBound(t *testing.T) {
	var bound prob.BucketSizeBound
	bound = lowerAndUpperBound(SizeLog{
		Value: 8,
	})
	assert.Equal(t, prob.ComputeLowerAndUpperBound(256), bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 7,
	})
	assert.Equal(t, prob.ComputeLowerAndUpperBound(128), bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 2,
	})
	assert.Equal(t, prob.ComputeLowerAndUpperBound(4), bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 1,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 2,
		Lower:    1.0,
		Upper:    4.0,
	}, bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 0,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 1,
		Lower:    1.0,
		Upper:    4.0,
	}, bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 10,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 648,
		Upper:    4.341911300652749,
		Lower:    0.8362045709310009,
	}, bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 11,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 648,
		Upper:    4.341911300652749,
		Lower:    0.8362045709310009,
	}, bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 12,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 648,
		Upper:    4.341911300652749,
		Lower:    0.8362045709310009,
	}, bound)

	bound = lowerAndUpperBound(SizeLog{
		Value: 5,
	})
	assert.Equal(t, prob.BucketSizeBound{
		MaxCount: 23,
		Lower:    0.71875,
		Upper:    5.565217391304348,
	}, bound)
}
