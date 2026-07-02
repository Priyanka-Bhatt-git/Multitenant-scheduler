package scheduler

import "container/heap"

// jobHeap implements heap.Interface. Higher Priority runs first; ties are
// broken by earlier CreatedAt (FIFO within the same priority).
type jobHeap []*Job

func (h jobHeap) Len() int { return len(h) }

func (h jobHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	return h[i].CreatedAt.Before(h[j].CreatedAt)
}

func (h jobHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *jobHeap) Push(x interface{}) {
	*h = append(*h, x.(*Job))
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// TenantQueue holds pending jobs for a single tenant, ordered by priority.
type TenantQueue struct {
	heap jobHeap
}

func NewTenantQueue() *TenantQueue {
	tq := &TenantQueue{heap: jobHeap{}}
	heap.Init(&tq.heap)
	return tq
}

func (tq *TenantQueue) Push(j *Job) {
	heap.Push(&tq.heap, j)
}

// Pop removes and returns the highest-priority job, or nil if empty.
func (tq *TenantQueue) Pop() *Job {
	if tq.heap.Len() == 0 {
		return nil
	}
	return heap.Pop(&tq.heap).(*Job)
}

func (tq *TenantQueue) Len() int {
	return tq.heap.Len()
}
