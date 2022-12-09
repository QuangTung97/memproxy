package memproxy

type delayedCallHeap struct {
	data []delayedCall
}

func heapParent(index int) int {
	return (index+1)/2 - 1
}

func heapLeftChild(index int) int {
	return index*2 + 1
}

func (h *delayedCallHeap) swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *delayedCallHeap) smaller(i, j int) bool {
	return h.data[i].startedAt.Before(h.data[j].startedAt)
}

func (h *delayedCallHeap) push(e delayedCall) {
	index := len(h.data)
	h.data = append(h.data, e)

	for index > 0 {
		parent := heapParent(index)
		if h.smaller(index, parent) {
			h.swap(index, parent)
		}
		index = parent
	}
}

func (h *delayedCallHeap) size() int {
	return len(h.data)
}

func (h *delayedCallHeap) top() delayedCall {
	return h.data[0]
}

func (h *delayedCallHeap) pop() delayedCall {
	result := h.data[0]
	last := len(h.data) - 1
	h.data[0] = h.data[last]
	h.data[last] = delayedCall{} // clear last
	h.data = h.data[:last]

	index := 0
	for {
		left := heapLeftChild(index)
		right := left + 1

		smallest := index
		if left < len(h.data) && h.smaller(left, smallest) {
			smallest = left
		}
		if right < len(h.data) && h.smaller(right, smallest) {
			smallest = right
		}

		if smallest == index {
			break
		}
		h.swap(index, smallest)
		index = smallest
	}

	return result
}
