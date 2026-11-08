package lsm

import "container/heap"

// iterator is an ordered stream of entries.
type iterator interface {
	valid() bool
	current() entry
	next()
	error() error
}

// sliceIter iterates over a sorted slice (memtable snapshots).
type sliceIter struct {
	es  []entry
	pos int
}

func (it *sliceIter) valid() bool    { return it.pos < len(it.es) }
func (it *sliceIter) current() entry { return it.es[it.pos] }
func (it *sliceIter) next()          { it.pos++ }
func (it *sliceIter) error() error   { return nil }

// mergeIter is a k-way merge of sorted iterators using a min-heap keyed by
// (key, source priority). Sources are given newest first; when several
// contain the same key only the newest version is returned. This is the
// core of both reads (Scan) and compaction. Each step costs O(log k).
type mergeIter struct {
	h   mergeHeap
	cur entry
	ok  bool
	err error
}

type heapItem struct {
	it  iterator
	pri int // lower = newer
}

type mergeHeap []heapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	ki, kj := h[i].it.current().key, h[j].it.current().key
	if ki != kj {
		return ki < kj
	}
	return h[i].pri < h[j].pri
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(heapItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func newMergeIter(sources []iterator) *mergeIter {
	m := &mergeIter{}
	for i, it := range sources {
		if err := it.error(); err != nil {
			m.err = err
		}
		if it.valid() {
			m.h = append(m.h, heapItem{it: it, pri: i})
		}
	}
	heap.Init(&m.h)
	m.advance()
	return m
}

// advance pops the smallest key and skips older versions of it.
func (m *mergeIter) advance() {
	if m.err != nil || len(m.h) == 0 {
		m.ok = false
		return
	}
	top := m.h[0]
	m.cur, m.ok = top.it.current(), true
	for len(m.h) > 0 && m.h[0].it.current().key == m.cur.key {
		it := m.h[0].it
		it.next()
		if err := it.error(); err != nil {
			m.err = err
			m.ok = false
			return
		}
		if it.valid() {
			heap.Fix(&m.h, 0)
		} else {
			heap.Pop(&m.h)
		}
	}
}

func (m *mergeIter) valid() bool    { return m.ok }
func (m *mergeIter) current() entry { return m.cur }
func (m *mergeIter) next()          { m.advance() }
func (m *mergeIter) error() error   { return m.err }
