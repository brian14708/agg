package agg

import (
	"container/heap"
)

type entry struct {
	Datum
	worst          float64
	best           float64
	iter           int
	index          int
	unseenIterator uint8
}

type entryQ struct {
	k int
	w byWorstDesc
	b byBest
}

func newEntryQ(k int) *entryQ {
	return &entryQ{
		k: k,
		w: make(byWorstDesc, 0, k),
	}
}

func (w *entryQ) update(e *entry) (ignore bool) {
	if e.index > 0 {
		heap.Fix(&w.w, e.index-1)
		return false
	}

	if len(w.w) == w.k && e.best < w.kthWorst() {
		if e.index < 0 {
			heap.Remove(&w.b, -e.index-1)
		}
		return true
	}

	if e.index < 0 {
		if lessByWorst(e, w.w[0]) {
			heap.Fix(&w.b, -e.index-1)
			return false
		}
		heap.Remove(&w.b, -e.index-1)
	}

	if len(w.w) == w.k {
		if lessByWorst(e, w.w[0]) {
			heap.Push(&w.b, e)
			return false
		}
	}

	heap.Push(&w.w, e)
	if len(w.w) > w.k {
		heap.Push(&w.b, heap.Pop(&w.w))
	}
	return false
}

func (w *entryQ) kthWorst() float64 {
	return w.w[0].worst
}

func (w *entryQ) bestNotInTopK() (float64, bool) {
	if len(w.b) == 0 {
		return 0, false
	}
	return w.b[0].best, true
}

func (w *entryQ) topK() []Datum {
	ret := make([]Datum, len(w.w))
	for i := len(w.w) - 1; i >= 0; i-- {
		ret[i] = heap.Pop(&w.w).(*entry).Datum
	}
	return ret
}

func lessByWorst(i, j *entry) bool {
	if i.worst == j.worst {
		return i.best < j.best
	}
	return i.worst < j.worst
}

type byWorstDesc []*entry

func (q byWorstDesc) Len() int           { return len(q) }
func (q byWorstDesc) Less(i, j int) bool { return lessByWorst(q[i], q[j]) }
func (q byWorstDesc) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i + 1
	q[j].index = j + 1
}

func (q *byWorstDesc) Push(x interface{}) {
	n := len(*q)
	item := x.(*entry)
	item.index = n + 1
	*q = append(*q, item)
}

func (q *byWorstDesc) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = 0
	*q = old[:n-1]
	return item
}

type byBest []*entry

func (q byBest) Len() int { return len(q) }
func (q byBest) Less(i, j int) bool {
	return q[i].best > q[j].best
}

func (q byBest) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = -i - 1
	q[j].index = -j - 1
}

func (q *byBest) Push(x interface{}) {
	n := len(*q)
	item := x.(*entry)
	item.index = -n - 1
	*q = append(*q, item)
}

func (q *byBest) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = 0
	*q = old[:n-1]
	return item
}

type sortByScore struct {
	cand  []Datum
	score []float64
}

func (s *sortByScore) Len() int {
	return len(s.cand)
}

func (s *sortByScore) Swap(i, j int) {
	s.cand[i], s.cand[j] = s.cand[j], s.cand[i]
	s.score[i], s.score[j] = s.score[j], s.score[i]
}

func (s *sortByScore) Less(i, j int) bool {
	return s.score[i] > s.score[j]
}
