package prob

import (
	"math"
)

// CombinationCalculator ...
type CombinationCalculator struct {
	result []int
}

// NewCombinationCalculator ...
func NewCombinationCalculator(n int) *CombinationCalculator {
	prev := make([]int, 0, n+1)
	next := make([]int, 0, n+1)

	prev = append(prev, 1)

	for i := 1; i <= n; i++ {
		next = append(next, 1)
		for k := 1; k <= i-1; k++ {
			next = append(next, prev[k]+prev[k-1])
		}
		next = append(next, 1)

		tmp := prev
		prev = next
		next = tmp[:0]
	}

	return &CombinationCalculator{
		result: prev,
	}
}

// Get ...
func (c *CombinationCalculator) Get(x int) int {
	return c.result[x]
}

// BinomialCalculator ...
type BinomialCalculator struct {
	result         []float64
	greaterOrEqual []float64
	lessOrEqual    []float64
}

// NewBinomialCalculator ...
func NewBinomialCalculator(n int, p float64) *BinomialCalculator {
	comb := NewCombinationCalculator(n)

	result := make([]float64, 0, n+1)
	for x := 0; x <= n; x++ {
		newProb := float64(comb.Get(x)) * math.Pow(p, float64(x)) * math.Pow(1.0-p, float64(n-x))
		result = append(result, newProb)
	}

	last := 0.0
	greaterOrEqual := make([]float64, n+1)
	for i := n; i >= 0; i-- {
		last += result[i]
		greaterOrEqual[i] = last
	}

	last = 0.0
	lessOrEqual := make([]float64, n+1)
	for i := 0; i <= n; i++ {
		last += result[i]
		lessOrEqual[i] = last
	}

	return &BinomialCalculator{
		result:         result,
		greaterOrEqual: greaterOrEqual,
		lessOrEqual:    lessOrEqual,
	}
}

// Get ...
func (c *BinomialCalculator) Get(x int) float64 {
	return c.result[x]
}

// GreaterOrEqual ...
func (c *BinomialCalculator) GreaterOrEqual(x int) float64 {
	return c.greaterOrEqual[x]
}

// LessOrEqual ...
func (c *BinomialCalculator) LessOrEqual(x int) float64 {
	return c.lessOrEqual[x]
}

// expect number of draws to collected to get k different coupons
// E(k) = sum_1^k (n / (n - x + 1)) = n * sum_1^k (1 / (n - x + 1))
// H(n) = sum_1^n (1 / x) = sum_1^n (1 / (n - k + 1))
// E(k) = H(n) - H(n - k)
// H(n) ~ n log n + gamma * n + 1 / 2

const gamma = 0.5772156649

func couponCollectorExpectation(n, k int) float64 {
	result := 0.0
	for i := 1; i <= k; i++ {
		result += float64(n) / float64(n-i+1)
	}
	return result
}

// nearestCouponsCount computes the max coupons that required expect not more than n draws times 1.2
func nearestCouponsCount(n int) int {
	first := 1
	last := n
	for {
		mid := (first + last) / 2
		if mid == first {
			return first
		}

		expect := couponCollectorExpectation(n, mid)
		if expect >= float64(n) {
			last = mid
		} else {
			first = mid
		}
	}
}

func nTimesHarmonic(n float64) float64 {
	return n*math.Log(n) + gamma*n + 0.5
}

// variance of binomial: mu * p (1 - p)
// deviation = sqrt(mu * p (1 - p))
// mean = mu * p

// Tail Probability of Normal Distribution
// P(X > t) <= e^(-t^2 / 2)
// With X have:
// mean = 0.0
// deviation = 1.0
// Ref: https://math.stackexchange.com/questions/988822/normal-distribution-tail-probability-inequality

// For N(muy, dev) => P(X > dev * t) <= e^(-t^2/2)

func computeDeviation(muy float64, b float64, n float64) float64 {
	return math.Sqrt(muy * (1 - b/n) / b)
}

func inverseBoundProbability(deviation float64, delta float64) float64 {
	t := delta / deviation
	return math.Exp(t * t / 2.0)
}

func findBoundWithInverseProbability(deviation float64, inverseProb float64) float64 {
	return math.Sqrt(2*math.Log(inverseProb)) * deviation
}

const boundRatio = 3.0 / 4.0
const highProbability = 1e9

func findUpperBoundWithHighProbability(b float64, n float64) float64 {
	nextBound := 2.0 * math.Pow(2.0, boundRatio)
	dev := computeDeviation(nextBound, b, n)
	delta := findBoundWithInverseProbability(dev, highProbability)
	result := nextBound + delta

	secondUpper := 4.0 * n / b
	if result > secondUpper {
		return secondUpper
	}

	return result
}

func findLowerBoundWithHighProbability(b float64, n float64) float64 {
	lowerBound := 2.0 / math.Pow(2.0, boundRatio)
	dev := computeDeviation(lowerBound, b, n)
	delta := findBoundWithInverseProbability(dev, highProbability)
	result := lowerBound - delta

	secondLower := 1.0 / n * b

	if result < secondLower {
		return secondLower
	}
	return result
}
