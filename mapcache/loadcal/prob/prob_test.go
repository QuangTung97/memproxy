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
	assert.Equal(t, 4.290491828847395, upper)

	upper = findUpperBoundWithHighProbability(162, 1e9)
	assert.Equal(t, 4.291243461150992, upper)

	upper = findUpperBoundWithHighProbability(162, 256)
	assert.Equal(t, 3.925709210583752, upper)

	upper = findUpperBoundWithHighProbability(163, 256)
	assert.Equal(t, 3.920993449301621, upper)

	upper = findUpperBoundWithHighProbability(81, 128)
	assert.Equal(t, 4.158548408544493, upper)

	upper = findUpperBoundWithHighProbability(41, 64)
	assert.Equal(t, 4.469005732030621, upper)

	upper = findUpperBoundWithHighProbability(21, 32)
	assert.Equal(t, 4.874211987682589, upper)

	upper = findUpperBoundWithHighProbability(11, 16)
	assert.Equal(t, 5.353681590328888, upper)

	upper = findUpperBoundWithHighProbability(6, 8)
	assert.Equal(t, 5.773711519045098, upper)

	upper = findUpperBoundWithHighProbability(7, 8)
	assert.Equal(t, 4.941383400842568, upper)

	upper = findUpperBoundWithHighProbability(3, 4)
	assert.Equal(t, 6.772018336467316, upper)
}
