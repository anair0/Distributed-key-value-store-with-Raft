package lsm

import "math/rand"

// Entry kinds. A tombstone (kindDelete) shadows older values of its key
// until compaction drops both.
const (
	kindDelete byte = 0
	kindPut    byte = 1
)

type entry struct {
	key   string
	value []byte
	kind  byte
}

const (
	maxLevel = 12
	pLevel   = 4 // each level has ~1/4 of the nodes of the one below
)

// skiplist is the memtable: a sorted map with O(log n) expected insert and
// lookup, and cheap in-order iteration. It is not safe for concurrent
// mutation; the DB serialises writers. Once a memtable becomes immutable
// (queued for flush) it may be read concurrently.
type skiplist struct {
	head  *slNode
	level int
	rng   *rand.Rand
	bytes int // approximate memory footprint
	n     int
}

type slNode struct {
	entry
	next []*slNode
}

func newSkiplist() *skiplist {
	return &skiplist{head: &slNode{next: make([]*slNode, maxLevel)}, level: 1, rng: rand.New(rand.NewSource(rand.Int63()))}
}

func (s *skiplist) randomLevel() int {
	l := 1
	for l < maxLevel && s.rng.Intn(pLevel) == 0 {
		l++
	}
	return l
}

// put inserts or overwrites key.
func (s *skiplist) put(key string, value []byte, kind byte) {
	var update [maxLevel]*slNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		update[i] = x
	}
	if n := x.next[0]; n != nil && n.key == key {
		s.bytes += len(value) - len(n.value)
		n.value, n.kind = value, kind
		return
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	n := &slNode{entry: entry{key: key, value: value, kind: kind}, next: make([]*slNode, lvl)}
	for i := range lvl {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	s.bytes += len(key) + len(value) + 16 + 8*lvl
	s.n++
}

// get returns the entry for key, if present (tombstones included).
func (s *skiplist) get(key string) (entry, bool) {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}
	if n := x.next[0]; n != nil && n.key == key {
		return n.entry, true
	}
	return entry{}, false
}

// entries returns all entries in key order.
func (s *skiplist) entries() []entry {
	out := make([]entry, 0, s.n)
	for n := s.head.next[0]; n != nil; n = n.next[0] {
		out = append(out, n.entry)
	}
	return out
}
