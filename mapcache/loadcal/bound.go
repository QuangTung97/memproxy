package loadcal

import "github.com/QuangTung97/memproxy/mapcache/loadcal/prob"

const maxSizeLog = 10

func initBucketSizeBounds() []prob.BucketSizeBound {
	result := make([]prob.BucketSizeBound, 0, maxSizeLog+1)

	bound := prob.BucketSizeBound{
		MaxCount: 1,
		Lower:    0.0,
		Upper:    4.0,
	}
	result = append(result, bound)

	bound.MaxCount = 2
	bound.Lower = 1.0
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

type boundChecker struct {
	updater SizeLogUpdater
}

var _ BoundChecker = &boundChecker{}

// NewBoundChecker ...
func NewBoundChecker(updater SizeLogUpdater) BoundChecker {
	return &boundChecker{
		updater: updater,
	}
}

func (c *boundChecker) updateIfLoadOutOfBounds(input CheckBoundInput, bounds prob.BucketSizeBound) {
	load := input.TotalEntries / float64(input.CountedBuckets)

	if load >= bounds.Upper {
		newSizeLog := SizeLog{
			Value:   input.CurrentSizeLog.Value + 1,
			Version: input.CurrentSizeLog.Version + 1,
		}
		c.updater.Update(input.Key, newSizeLog, UpdateOptions{})
		return
	}

	if load < bounds.Lower {
		newSizeLog := SizeLog{
			Value:   input.CurrentSizeLog.Value - 1,
			Version: input.CurrentSizeLog.Version + 1,
		}
		c.updater.Update(input.Key, newSizeLog, UpdateOptions{})
		return
	}
}

func (c *boundChecker) Check(input CheckBoundInput) CheckBoundOutput {
	needReset := false
	bounds := lowerAndUpperBound(input.CurrentSizeLog)

	if input.CountedBuckets >= bounds.MaxCount {
		needReset = true
		c.updateIfLoadOutOfBounds(input, bounds)
	}

	return CheckBoundOutput{
		NeedReset: needReset,
	}
}
