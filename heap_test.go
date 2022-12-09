package memproxy

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func newHeapTest() *delayedCallHeap {
	return &delayedCallHeap{}
}

func newTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestHeap_Simple(t *testing.T) {
	h := newHeapTest()

	h.push(delayedCall{startedAt: newTime("2022-05-09T10:00:00+07:00")})
	assert.Equal(t, 1, h.size())

	e := h.pop()
	assert.Equal(t, delayedCall{startedAt: newTime("2022-05-09T10:00:00+07:00")}, e)
	assert.Equal(t, 0, h.size())
}

func TestHeap_Push_Smaller(t *testing.T) {
	h := newHeapTest()

	h.push(delayedCall{startedAt: newTime("2022-05-09T10:00:00+07:00")})
	assert.Equal(t, 1, h.size())

	h.push(delayedCall{startedAt: newTime("2022-05-08T10:00:00+07:00")})
	assert.Equal(t, 2, h.size())

	e := h.pop()
	assert.Equal(t, delayedCall{startedAt: newTime("2022-05-08T10:00:00+07:00")}, e)
	assert.Equal(t, 1, h.size())

	e = h.pop()
	assert.Equal(t, delayedCall{startedAt: newTime("2022-05-09T10:00:00+07:00")}, e)
	assert.Equal(t, 0, h.size())
}

func TestHeap_Properties_Based(t *testing.T) {
	start := newTime("2022-05-09T10:00:00+07:00")
	const num = 1000
	calls := make([]delayedCall, 0, num)
	for i := 0; i < num; i++ {
		calls = append(calls, delayedCall{
			startedAt: start.Add(time.Duration(i) * time.Hour),
		})
	}

	rand.Seed(1234)

	rand.Shuffle(len(calls), func(i, j int) {
		calls[i], calls[j] = calls[j], calls[i]
	})

	h := newHeapTest()

	for _, call := range calls {
		h.push(call)
	}

	assert.Equal(t, delayedCall{startedAt: start}, h.top())

	assert.Equal(t, num, h.size())

	for i := 0; i < num; i++ {
		e := h.pop()
		assert.Equal(t, start.Add(time.Duration(i)*time.Hour), e.startedAt)
	}
}
