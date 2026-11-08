package raft

import (
	"slices"
	"sync"
)

// MemoryPersister keeps state in memory. Tests use it to simulate crashes:
// keep the persister, discard the Raft instance, and start a new instance
// on a Copy of it (so the dead instance cannot write to the new one's state).
type MemoryPersister struct {
	mu       sync.Mutex
	st       PersistentState
	saved    bool
	snapshot []byte
	size     int
}

func NewMemoryPersister() *MemoryPersister { return &MemoryPersister{} }

func (p *MemoryPersister) SaveState(st PersistentState) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st = cloneState(st)
	p.saved = true
	p.size = stateSize(st)
	return nil
}

func (p *MemoryPersister) SaveStateAndSnapshot(st PersistentState, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st = cloneState(st)
	p.saved = true
	p.size = stateSize(st)
	p.snapshot = slices.Clone(snapshot)
	return nil
}

func (p *MemoryPersister) LoadState() (PersistentState, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneState(p.st), p.saved, nil
}

func (p *MemoryPersister) LoadSnapshot() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.snapshot), nil
}

func (p *MemoryPersister) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.size
}

// SnapshotSize returns the size of the stored snapshot.
func (p *MemoryPersister) SnapshotSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.snapshot)
}

// Copy returns an independent copy (a "disk" for a restarted node).
func (p *MemoryPersister) Copy() *MemoryPersister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &MemoryPersister{st: cloneState(p.st), saved: p.saved, snapshot: slices.Clone(p.snapshot), size: p.size}
}

// cloneState copies the log slice. Commands and member lists are never
// mutated after creation, so they can be shared.
func cloneState(st PersistentState) PersistentState {
	st.Log = slices.Clone(st.Log)
	return st
}

func stateSize(st PersistentState) int {
	n := 16
	for _, e := range st.Log {
		n += 24 + len(e.Command) + 16*len(e.Members)
	}
	return n
}
