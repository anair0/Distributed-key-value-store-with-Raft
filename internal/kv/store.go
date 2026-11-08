package kv

import (
	"sort"
	"sync"
)

// WriteOp is one mutation in an atomic batch.
type WriteOp struct {
	Key    string
	Value  []byte
	Delete bool
}

// Store is the storage engine under the state machine: an in-memory map
// (MemStore) or the LSM tree (LSMStore). A batch must be applied atomically:
// after a crash either all of it or none of it is visible.
type Store interface {
	Get(key string) ([]byte, bool, error)
	Write(ops []WriteOp) error
	// Scan calls fn for every key in ascending order until fn returns false.
	Scan(fn func(key string, value []byte) bool) error
	// Reset deletes everything (used before installing a snapshot).
	Reset() error
	Close() error
}

// MemStore is a Store backed by a map. It is not durable; after a restart
// the state is rebuilt from the Raft snapshot and log.
type MemStore struct {
	mu sync.RWMutex
	m  map[string][]byte
}

// NewMemStore creates an empty MemStore.
func NewMemStore() *MemStore { return &MemStore{m: map[string][]byte{}} }

func (s *MemStore) Get(key string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok, nil
}

func (s *MemStore) Write(ops []WriteOp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, op := range ops {
		if op.Delete {
			delete(s.m, op.Key)
		} else {
			s.m[op.Key] = append([]byte(nil), op.Value...)
		}
	}
	return nil
}

func (s *MemStore) Scan(fn func(string, []byte) bool) error {
	s.mu.RLock()
	keys := make([]string, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	sort.Strings(keys)
	for _, k := range keys {
		s.mu.RLock()
		v, ok := s.m[k]
		s.mu.RUnlock()
		if ok && !fn(k, v) {
			return nil
		}
	}
	return nil
}

func (s *MemStore) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m = map[string][]byte{}
	return nil
}

func (s *MemStore) Close() error { return nil }
