package loadcal

import "github.com/QuangTung97/memproxy/mapcache/loadcal/prob"

const maxSizeLog = 10

func initBucketSizeBounds() []prob.BucketSizeBound {
	result := make([]prob.BucketSizeBound, 0, maxSizeLog+1)

	bound := prob.BucketSizeBound{
		MaxCount: 1,
		Lower:    1.0,
		Upper:    4.0,
	}
	result = append(result, bound)

	bound.MaxCount = 2
	result = append(result, bound)

	for i := 2; i <= 10; i++ {
		result = append(result, prob.ComputeLowerAndUpperBound(1<<i))
	}
	return result
}

var bucketBounds = initBucketSizeBounds()

func lowerAndUpperBound(sizeLog SizeLog) prob.BucketSizeBound {
	if sizeLog.Value > maxSizeLog {
		return bucketBounds[maxSizeLog]
	}
	return bucketBounds[sizeLog.Value]
}
