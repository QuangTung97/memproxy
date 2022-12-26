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
		Lower:    0.0,
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

type boundCheckerTest struct {
	updater *SizeLogUpdaterMock
	checker BoundChecker
}

func newBoundCheckerTest() *boundCheckerTest {
	updater := &SizeLogUpdaterMock{
		UpdateFunc: func(key string, sizeLog SizeLog, options UpdateOptions) {
		},
	}
	return &boundCheckerTest{
		updater: updater,
		checker: NewBoundChecker(updater),
	}
}

func TestBoundChecker__Call_Update_SizeLog__When_Reach_Buckets_Count_And_Load(t *testing.T) {
	c := newBoundCheckerTest()

	const key = "KEY01"

	const upper = 5.566

	output := c.checker.Check(CheckBoundInput{
		Key:            key,
		TotalEntries:   upper * 23,
		CountedBuckets: 23,
		TotalChecked:   30,
		CurrentSizeLog: SizeLog{
			Value:   5,
			Version: 41,
		},
	})
	assert.Equal(t, true, output.NeedReset)

	updateCalls := c.updater.UpdateCalls()
	assert.Equal(t, 1, len(updateCalls))
	assert.Equal(t, key, updateCalls[0].Key)
	assert.Equal(t, SizeLog{
		Value:   6,
		Version: 42,
	}, updateCalls[0].SizeLog)

	// Not Call When not enough counted buckets
	output = c.checker.Check(CheckBoundInput{
		Key:            key,
		TotalEntries:   upper * 22,
		CountedBuckets: 22,
		TotalChecked:   30,
		CurrentSizeLog: SizeLog{
			Value:   5,
			Version: 41,
		},
	})
	assert.Equal(t, false, output.NeedReset)

	updateCalls = c.updater.UpdateCalls()
	assert.Equal(t, 1, len(updateCalls))

	// Not Call When not reach limit
	output = c.checker.Check(CheckBoundInput{
		Key:            key,
		TotalEntries:   (upper - 0.001) * 23,
		CountedBuckets: 23,
		TotalChecked:   30,
		CurrentSizeLog: SizeLog{
			Value:   5,
			Version: 41,
		},
	})
	assert.Equal(t, true, output.NeedReset)

	updateCalls = c.updater.UpdateCalls()
	assert.Equal(t, 1, len(updateCalls))
}

func TestBoundChecker__Call_Update_SizeLog__When_Reach_Lower_Buckets_Count_And_Load(t *testing.T) {
	c := newBoundCheckerTest()

	const key = "KEY01"

	const lower = 0.7186

	output := c.checker.Check(CheckBoundInput{
		Key:            key,
		TotalEntries:   lower * 23,
		CountedBuckets: 23,
		TotalChecked:   30,
		CurrentSizeLog: SizeLog{
			Value:   5,
			Version: 41,
		},
	})
	assert.Equal(t, true, output.NeedReset)

	updateCalls := c.updater.UpdateCalls()
	assert.Equal(t, 1, len(updateCalls))
	assert.Equal(t, key, updateCalls[0].Key)
	assert.Equal(t, SizeLog{
		Value:   4,
		Version: 42,
	}, updateCalls[0].SizeLog)

	// Not Call When not reach lower limit
	output = c.checker.Check(CheckBoundInput{
		Key:            key,
		TotalEntries:   (lower + 0.001) * 23,
		CountedBuckets: 23,
		TotalChecked:   30,
		CurrentSizeLog: SizeLog{
			Value:   5,
			Version: 41,
		},
	})
	assert.Equal(t, true, output.NeedReset)

	updateCalls = c.updater.UpdateCalls()
	assert.Equal(t, 1, len(updateCalls))
}
