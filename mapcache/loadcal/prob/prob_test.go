package prob

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestCombination(t *testing.T) {
	c := NewCombinationCalculator(1)
	assert.Equal(t, 1, c.Get(0))
	assert.Equal(t, 1, c.Get(1))

	c = NewCombinationCalculator(2)
	assert.Equal(t, 1, c.Get(0))
	assert.Equal(t, 2, c.Get(1))
	assert.Equal(t, 1, c.Get(2))

	c = NewCombinationCalculator(4)
	assert.Equal(t, 1, c.Get(0))
	assert.Equal(t, 4, c.Get(1))
	assert.Equal(t, 6, c.Get(2))
	assert.Equal(t, 4, c.Get(3))
	assert.Equal(t, 1, c.Get(4))

	c = NewCombinationCalculator(5)
	assert.Equal(t, 1, c.Get(0))
	assert.Equal(t, 5, c.Get(1))
	assert.Equal(t, 10, c.Get(2))
	assert.Equal(t, 10, c.Get(3))
	assert.Equal(t, 5, c.Get(4))
	assert.Equal(t, 1, c.Get(5))
}

func BenchmarkCombinationCalculator(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_ = NewCombinationCalculator(10000)
	}
}

const epsilon = 0.000001

func TestBinomial(t *testing.T) {
	c := NewBinomialCalculator(5, 0.2)
	assert.InEpsilon(t, 0.32768, c.Get(0), epsilon)
	assert.InEpsilon(t, 0.4096, c.Get(1), epsilon)
	assert.InEpsilon(t, 0.2048, c.Get(2), epsilon)
	assert.InEpsilon(t, 0.0512, c.Get(3), epsilon)
	assert.InEpsilon(t, 0.0512, c.Get(3), epsilon)
	assert.InEpsilon(t, 0.00032, c.Get(5), epsilon)
}

func TestBinomial_Greater_Or_Equal(t *testing.T) {
	c := NewBinomialCalculator(5, 0.2)
	assert.InEpsilon(t, 1.0, c.GreaterOrEqual(0), epsilon)
	assert.InEpsilon(t, 1.0-0.32768, c.GreaterOrEqual(1), epsilon)
	assert.InEpsilon(t, 0.00032, c.GreaterOrEqual(5), epsilon)
}

func TestBinomial_Less_Or_Equal(t *testing.T) {
	c := NewBinomialCalculator(5, 0.2)
	assert.InEpsilon(t, 1.0, c.LessOrEqual(5), epsilon)
	assert.InEpsilon(t, 0.32768, c.LessOrEqual(0), epsilon)
	assert.InEpsilon(t, 0.32768+0.4096+0.2048, c.LessOrEqual(2), epsilon)
}

func BenchmarkBinomialCalculator(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_ = NewBinomialCalculator(10000, 0.2)
	}
}

func TestCouponCollectorExpectation(t *testing.T) {
	result := couponCollectorExpectation(256, 162)
	assert.Equal(t, 255.62234821580253, result)

	result = couponCollectorExpectation(256, 163)
	assert.Equal(t, 258.34575247112167, result)

	result = couponCollectorExpectation(256, 161)
	assert.Equal(t, 252.92761137369726, result)

	result = couponCollectorExpectation(256, 256)
	assert.Equal(t, 1567.8323104812232, result)

	assert.Equal(t, 1567.8326360011679, nTimesHarmonic(256))

	result = couponCollectorExpectation(128, 81)
	assert.Equal(t, 127.38345610987352, result)

	result = couponCollectorExpectation(128, 82)
	assert.Equal(t, 130.10686036519266, result)

	result = couponCollectorExpectation(70, 44)
	assert.Equal(t, 68.48919289961187, result)

	result = couponCollectorExpectation(70, 45)
	assert.Equal(t, 71.18150059191956, result)

	result = couponCollectorExpectation(32, 21)
	assert.Equal(t, 33.23577121789361, result)

	result = couponCollectorExpectation(16, 11)
	assert.Equal(t, 17.558330558330557, result)

	result = couponCollectorExpectation(16, 13)
	assert.Equal(t, 24.758330558330556, result)

	result = couponCollectorExpectation(4, 3)
	assert.Equal(t, 4.333333333333333, result)

	result = couponCollectorExpectation(4, 4)
	assert.Equal(t, 8.333333333333332, result)

	// for 8
	result = couponCollectorExpectation(8, 8)
	assert.Equal(t, 21.74285714285714, result)

	result = couponCollectorExpectation(8, 7)
	assert.Equal(t, 13.742857142857142, result)

	result = couponCollectorExpectation(8, 6)
	assert.Equal(t, 9.742857142857142, result)
}

func TestNearestCouponsCount(t *testing.T) {
	k := nearestCouponsCount(256)
	assert.Equal(t, 162, k)

	k = nearestCouponsCount(128)
	assert.Equal(t, 81, k)

	k = nearestCouponsCount(70)
	assert.Equal(t, 44, k)

	k = nearestCouponsCount(64)
	assert.Equal(t, 40, k)

	k = nearestCouponsCount(32)
	assert.Equal(t, 20, k)

	k = nearestCouponsCount(16)
	assert.Equal(t, 10, k)

	k = nearestCouponsCount(8)
	assert.Equal(t, 5, k)

	k = nearestCouponsCount(4)
	assert.Equal(t, 2, k)

	k = nearestCouponsCount(2)
	assert.Equal(t, 1, k)
}

func TestComputeDeviation(t *testing.T) {
	v := computeDeviation(4.0, 40, 64)
	assert.Equal(t, 0.19364916731037085, v)

	v = computeDeviation(1.0, 40, 64)
	assert.Equal(t, 0.09682458365518543, v)
}

func TestInvBoundProb(t *testing.T) {
	prob := inverseBoundProbability(computeDeviation(6.0, 162, 4068), 7.5/6.0)
	assert.Equal(t, 3.4741163323745456e+09, prob)

	prob = inverseBoundProbability(computeDeviation(6.0, 162, 256), 7.5/6.0)
	assert.Equal(t, 8.888570862965199e+24, prob)

	prob = inverseBoundProbability(computeDeviation(6.0, 162, 16000), 7.5/6.0)
	assert.Equal(t, 1.7972234377981377e+09, prob)

	delta := findBoundWithInverseProbability(computeDeviation(6.0, 162, 16000), 1e9)
	assert.Equal(t, 1.2326857990927726, delta)

	prob = inverseBoundProbability(computeDeviation(6.0, 162, 16000), 1.2326857990927726)
	assert.Equal(t, 1.0000000000000064e+09, prob)

	nextBound := 2.0 * math.Pow(2.0, 3.0/4.0)
	dev := computeDeviation(nextBound, 162, 100000)
	delta = findBoundWithInverseProbability(dev, 1e9)
	assert.Equal(t, 0.9269061678325372, delta)
	assert.Equal(t, 4.290491828847395, nextBound+delta)
}

func TestFindUpperBound(t *testing.T) {
	upper := findUpperBoundWithHighProbability(162, 100000)
	assert.Equal(t, 4.332969665681529, upper)

	upper = findUpperBoundWithHighProbability(162, 1e9)
	assert.Equal(t, 4.332969665681529, upper)

	upper = findUpperBoundWithHighProbability(162, 256)
	assert.Equal(t, 4.332969665681529, upper)

	upper = findUpperBoundWithHighProbability(163, 256)
	assert.Equal(t, 4.32986633242024, upper)

	upper = findUpperBoundWithHighProbability(82, 128)
	assert.Equal(t, 4.749235185568182, upper)

	upper = findUpperBoundWithHighProbability(41, 64)
	assert.Equal(t, 5.369282972365251, upper)

	upper = findUpperBoundWithHighProbability(21, 32)
	assert.Equal(t, 6.095238095238095, upper)

	upper = findUpperBoundWithHighProbability(11, 16)
	assert.Equal(t, 5.818181818181818, upper)

	upper = findUpperBoundWithHighProbability(13, 16)
	assert.Equal(t, 4.923076923076923, upper)

	upper = findUpperBoundWithHighProbability(6, 8)
	assert.Equal(t, 5.333333333333333, upper)

	upper = findUpperBoundWithHighProbability(7, 8)
	assert.Equal(t, 4.571428571428571, upper)

	upper = findUpperBoundWithHighProbability(3, 4)
	assert.Equal(t, 5.333333333333333, upper)
}

func TestFindLowerBound(t *testing.T) {
	var lower float64

	lower = findLowerBoundWithHighProbability(162, 1e9)
	assert.Equal(t, 0.6821533604010123, lower)

	lower = findLowerBoundWithHighProbability(162, 256)
	assert.Equal(t, 0.6821533604010123, lower)

	lower = findLowerBoundWithHighProbability(163, 256)
	assert.Equal(t, 0.6835680370871374, lower)

	lower = findLowerBoundWithHighProbability(82, 128)
	assert.Equal(t, 0.640625, lower)

	lower = findLowerBoundWithHighProbability(41, 64)
	assert.Equal(t, 0.640625, lower)

	lower = findLowerBoundWithHighProbability(21, 32)
	assert.Equal(t, 0.65625, lower)

	lower = findLowerBoundWithHighProbability(22, 32)
	assert.Equal(t, 0.6875, lower)

	lower = findLowerBoundWithHighProbability(13, 16)
	assert.Equal(t, 0.8125, lower)

	lower = findLowerBoundWithHighProbability(12, 16)
	assert.Equal(t, 0.75, lower)

	lower = findLowerBoundWithHighProbability(11, 16)
	assert.Equal(t, 0.6875, lower)

	lower = findLowerBoundWithHighProbability(7, 8)
	assert.Equal(t, 0.875, lower)

	lower = findLowerBoundWithHighProbability(6, 8)
	assert.Equal(t, 0.75, lower)
}

func TestUpperChernoffBound(t *testing.T) {
	var prob float64

	prob = upperChernoffBoundInverseProbability(6.0, 162, 1.5)
	assert.Equal(t, 1.6296963853891113e+12, prob)

	prob = upperChernoffBoundInverseProbability(6.0, 82, 1.8)
	assert.Equal(t, 5.97451432177368e+08, prob)

	mid := 2.0 * math.Pow(2.0, boundRatio)
	prob = upperChernoffBoundInverseProbability(mid, 21, 6.1-mid)
	assert.Equal(t, 1.4479380695452923e+08, prob)

	delta := 1.5 / 6.0
	assert.Equal(t, 6.229644421984454e+08, math.Exp(delta*delta/3*162*6))
}

func TestLowerChernoffBound(t *testing.T) {
	var prob float64

	prob = lowerChernoffBoundInverseProbability(1.0, 162, 0.5)
	assert.Equal(t, 6.229072496214558e+10, prob)

	prob = lowerChernoffBoundInverseProbability(1.5, 162, 0.75)
	assert.Equal(t, 1.5546587591574582e+16, prob)
}

func TestFindUpperChernoffBound(t *testing.T) {
	var bound float64
	var prob float64

	bound = findUpperChernoffBoundWithHighProbability(6.0, 162)
	assert.Equal(t, 7.280918415205209, bound)

	prob = upperChernoffBoundInverseProbability(6.0, 162, 7.280918415205209-6.0)
	assert.Equal(t, 9.999999999999673e+08, prob)

	bound = findUpperChernoffBoundWithHighProbability(4.0, 162)
	assert.Equal(t, 5.053415509418841, bound)

	prob = upperChernoffBoundInverseProbability(4.0, 162, 5.053415509418841-4.0)
	assert.Equal(t, 9.99999999999915e+08, prob)

	mid := 2.0 * math.Pow(2.0, boundRatio)
	bound = findUpperChernoffBoundWithHighProbability(mid, 163)
	assert.Equal(t, 4.32986633242024, bound)
}

func TestFindLowerChernoffBound(t *testing.T) {
	var bound float64
	var prob float64

	bound = findLowerChernoffBoundWithHighProbability(1.5, 162)
	assert.Equal(t, 0.9248117072173783, bound)

	prob = lowerChernoffBoundInverseProbability(1.5, 162, 1.5-0.9248117072173783)
	assert.Equal(t, 9.999999999999673e+08, prob)

	bound = findLowerChernoffBoundWithHighProbability(1.0, 163)
	assert.Equal(t, 0.5401994873293556, bound)

	prob = lowerChernoffBoundInverseProbability(1.0, 163, 1.0-0.5401994873293556)
	assert.Equal(t, 9.999999999999875e+08, prob)

	mid := 2.0 / math.Pow(2.0, boundRatio)

	bound = findLowerChernoffBoundWithHighProbability(mid, 163)
	assert.Equal(t, 0.6835680370871374, bound)

	prob = lowerChernoffBoundInverseProbability(mid, 163, mid-0.6835680370871374)
	assert.Equal(t, 9.999999999999775e+08, prob)

	bound = findLowerChernoffBoundWithHighProbability(mid, 82)
	assert.Equal(t, 0.5037875738536676, bound)

	bound = findLowerChernoffBoundWithHighProbability(mid, 41)
	assert.Equal(t, 0.2791786501625013, bound)
}

func TestComputeLowerUpperBound(t *testing.T) {
	var result BucketSizeBound

	result = ComputeLowerAndUpperBound(256)
	assert.Equal(t, BucketSizeBound{
		Lower: 0.6835680370871374,
		Upper: 4.32986633242024,
	}, result)

	result = ComputeLowerAndUpperBound(1024)
	assert.Equal(t, BucketSizeBound{
		Lower: 0.6835680370871374,
		Upper: 4.32986633242024,
	}, result)

	result = ComputeLowerAndUpperBound(128)
	assert.Equal(t, BucketSizeBound{
		Lower: 0.640625,
		Upper: 4.749235185568182,
	}, result)

	result = ComputeLowerAndUpperBound(64)
	assert.Equal(t, BucketSizeBound{
		Lower: 0.640625,
		Upper: 5.369282972365251,
	}, result)

	result = ComputeLowerAndUpperBound(32)
	assert.Equal(t, BucketSizeBound{
		Lower: 0.71875,
		Upper: 5.565217391304348,
	}, result)
	assert.Equal(t, 39.344862126984516, couponCollectorExpectation(32, 23))

	result = ComputeLowerAndUpperBound(16)
	assert.Equal(t, BucketSizeBound{
		Lower: 0.75,
		Upper: 5.333333333333333,
	}, result)
	assert.Equal(t, 20.758330558330556, couponCollectorExpectation(16, 12))

	result = ComputeLowerAndUpperBound(4)
	assert.Equal(t, BucketSizeBound{
		Lower: 0.75,
		Upper: 5.333333333333333,
	}, result)
}
