package raft

import "sync"

// Config holds static node configuration.
type Config struct {
	ID    NodeID
	Peers []NodeID // all node IDs including self
}

// Raft is a single Raft node. TODO(M1): election; TODO(M2): replication.
type Raft struct {
	mu sync.Mutex

	cfg       Config
	transport Transport
	persister Persister
	applyCh   chan<- ApplyMsg

	// Persistent state (save before replying to RPCs).
	currentTerm uint64
	votedFor    NodeID
	log         []LogEntry

	// Volatile state.
	role        Role
	commitIndex uint64
	lastApplied uint64
}

// New creates a Raft node. It does not start background goroutines yet.
func New(cfg Config, t Transport, p Persister, applyCh chan<- ApplyMsg) *Raft {
	return &Raft{
		cfg:       cfg,
		transport: t,
		persister: p,
		applyCh:   applyCh,
		votedFor:  -1,
		role:      Follower,
	}
}

// State returns the current term and whether this node believes it is leader.
func (r *Raft) State() (term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role == Leader
}
