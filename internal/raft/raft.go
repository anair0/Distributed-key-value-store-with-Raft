package raft

import (
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Raft is a single Raft node.
//
// Concurrency model: one mutex (mu) guards all state. Background goroutines:
//   - ticker: election timeouts and leader check-quorum,
//   - applier: delivers committed entries / snapshots on applyCh,
//   - one replicator per peer while leader: AppendEntries / InstallSnapshot.
//
// The mutex is never held while sending an RPC or writing to applyCh. Every
// RPC reply handler re-checks term and role after re-acquiring it.
type Raft struct {
	mu        sync.Mutex
	cfg       Config
	id        NodeID
	transport Transport
	persister Persister
	applyCh   chan<- ApplyMsg
	log       *slog.Logger
	rng       *rand.Rand

	// Persistent state (persist before replying to any RPC).
	currentTerm uint64
	votedFor    NodeID
	entries     []LogEntry // entries[0] is the snapshot sentinel

	// Volatile state.
	role              Role
	leaderID          NodeID
	commitIndex       uint64
	lastApplied       uint64
	members           []Member // current configuration: latest config entry in the log
	electionDeadline  time.Time
	lastLeaderContact time.Time
	snapshot          []byte    // latest snapshot, sent to lagging followers
	pendingSnapshot   *ApplyMsg // snapshot to hand to the applier

	// Leader state, reinitialised on every election win.
	nextIndex   map[NodeID]uint64
	matchIndex  map[NodeID]uint64
	lastAck     map[NodeID]time.Time
	replicators map[NodeID]*replicator

	elections     uint64
	leaderChanges uint64

	applyCond *sync.Cond
	done      chan struct{}
	killed    atomic.Bool
	wg        sync.WaitGroup
}

// New creates a node, restoring persisted state if there is any. Call Run
// to start its background goroutines.
func New(cfg Config, t Transport, p Persister, applyCh chan<- ApplyMsg) (*Raft, error) {
	cfg = cfg.withDefaults()
	r := &Raft{
		cfg:       cfg,
		id:        cfg.ID,
		transport: t,
		persister: p,
		applyCh:   applyCh,
		log:       cfg.Logger.With("node", cfg.ID),
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		votedFor:  None,
		role:      Follower,
		leaderID:  None,
		done:      make(chan struct{}),
	}
	r.applyCond = sync.NewCond(&r.mu)

	st, ok, err := p.LoadState()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}
	if ok {
		r.currentTerm, r.votedFor, r.entries = st.CurrentTerm, st.VotedFor, st.Log
	}
	if len(r.entries) == 0 {
		r.entries = []LogEntry{{Kind: EntryConfig, Members: slices.Clone(cfg.Members)}}
	}
	snap, err := p.LoadSnapshot()
	if err != nil {
		return nil, fmt.Errorf("raft: load snapshot: %w", err)
	}
	if base := r.entries[0]; base.Index > 0 {
		// Everything up to the snapshot is committed. Re-deliver the snapshot
		// to the state machine first; the applier then continues from there.
		r.snapshot = snap
		r.commitIndex = base.Index
		r.pendingSnapshot = &ApplyMsg{SnapshotValid: true, Snapshot: snap, SnapshotIndex: base.Index, SnapshotTerm: base.Term}
	}
	r.recomputeMembersLocked()
	r.resetElectionTimerLocked()
	return r, nil
}

// Run starts the background goroutines.
func (r *Raft) Run() {
	r.wg.Add(2)
	go r.ticker()
	go r.applier()
}

// Kill stops the node. It is safe to call more than once. After Kill the node
// ignores RPCs and stops delivering on applyCh.
func (r *Raft) Kill() {
	if r.killed.Swap(true) {
		return
	}
	r.mu.Lock()
	close(r.done)
	r.stopReplicatorsLocked()
	r.applyCond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
}

// Killed reports whether Kill was called.
func (r *Raft) Killed() bool { return r.killed.Load() }

// State returns the current term and whether this node believes it is leader.
func (r *Raft) State() (term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role == Leader
}

// Leader returns the node this node believes is leader, and its address.
func (r *Raft) Leader() (NodeID, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.leaderID == None {
		return None, ""
	}
	for _, m := range r.members {
		if m.ID == r.leaderID {
			return m.ID, m.Addr
		}
	}
	return r.leaderID, ""
}

// Status returns a snapshot of the node's state.
func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		ID:            r.id,
		Role:          r.role,
		Term:          r.currentTerm,
		LeaderID:      r.leaderID,
		VotedFor:      r.votedFor,
		CommitIndex:   r.commitIndex,
		LastApplied:   r.lastApplied,
		LastLogIndex:  r.lastIndex(),
		LastLogTerm:   r.lastTerm(),
		SnapshotIndex: r.base(),
		Members:       slices.Clone(r.members),
		Elections:     r.elections,
		LeaderChanges: r.leaderChanges,
	}
}

// Start proposes a command. It returns immediately; the command is committed
// only if it later shows up on applyCh at the returned index with the
// returned term.
func (r *Raft) Start(command []byte) (index, term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != Leader || r.killed.Load() {
		return 0, r.currentTerm, false
	}
	idx := r.appendLocked(LogEntry{Kind: EntryNormal, Command: command})
	return idx, r.currentTerm, true
}

// appendLocked appends an entry in the current term as leader, persists it,
// and kicks the replicators.
func (r *Raft) appendLocked(e LogEntry) uint64 {
	e.Index = r.lastIndex() + 1
	e.Term = r.currentTerm
	r.entries = append(r.entries, e)
	if e.Kind == EntryConfig {
		r.recomputeMembersLocked()
	}
	r.persistLocked()
	r.advanceCommitLocked() // single-node clusters commit immediately
	r.triggerReplicatorsLocked()
	return e.Index
}

// ---- log helpers (indices are absolute; entries[0] is the sentinel) ----

func (r *Raft) base() uint64      { return r.entries[0].Index }
func (r *Raft) lastIndex() uint64 { return r.entries[len(r.entries)-1].Index }
func (r *Raft) lastTerm() uint64  { return r.entries[len(r.entries)-1].Term }

// termAt returns the term of the entry at index i, which must be in
// [base, lastIndex].
func (r *Raft) termAt(i uint64) uint64 { return r.entries[i-r.base()].Term }

func (r *Raft) entryAt(i uint64) LogEntry { return r.entries[i-r.base()] }

// ---- persistence ----

func (r *Raft) persistentStateLocked() PersistentState {
	return PersistentState{CurrentTerm: r.currentTerm, VotedFor: r.votedFor, Log: r.entries}
}

func (r *Raft) persistLocked() {
	if err := r.persister.SaveState(r.persistentStateLocked()); err != nil {
		// Continuing without durable state could violate safety.
		panic(fmt.Sprintf("raft %d: persist: %v", r.id, err))
	}
}

func (r *Raft) persistWithSnapshotLocked() {
	if err := r.persister.SaveStateAndSnapshot(r.persistentStateLocked(), r.snapshot); err != nil {
		panic(fmt.Sprintf("raft %d: persist snapshot: %v", r.id, err))
	}
}

// ---- membership ----

// recomputeMembersLocked sets the configuration to the latest config entry
// in the log (committed or not, as single-server changes require).
func (r *Raft) recomputeMembersLocked() {
	for i := len(r.entries) - 1; i >= 0; i-- {
		if r.entries[i].Kind == EntryConfig {
			r.members = slices.Clone(r.entries[i].Members)
			break
		}
	}
	if ab, ok := r.transport.(AddressBook); ok {
		ab.SetMembers(slices.Clone(r.members))
	}
	if r.role == Leader {
		r.syncReplicatorsLocked()
	}
}

// lastConfigIndexLocked is the index of the latest config entry.
func (r *Raft) lastConfigIndexLocked() uint64 {
	for i := len(r.entries) - 1; i >= 0; i-- {
		if r.entries[i].Kind == EntryConfig {
			return r.entries[i].Index
		}
	}
	return r.base()
}

func (r *Raft) isMemberLocked(id NodeID) bool {
	for _, m := range r.members {
		if m.ID == id {
			return true
		}
	}
	return false
}

func (r *Raft) peersLocked() []NodeID {
	var ps []NodeID
	for _, m := range r.members {
		if m.ID != r.id {
			ps = append(ps, m.ID)
		}
	}
	return ps
}

// quorumLocked reports whether the nodes for which ok returns true form a
// majority of the current configuration.
func (r *Raft) quorumLocked(ok func(NodeID) bool) bool {
	n := 0
	for _, m := range r.members {
		if ok(m.ID) {
			n++
		}
	}
	return len(r.members) > 0 && n > len(r.members)/2
}

// ---- role transitions ----

func (r *Raft) resetElectionTimerLocked() {
	span := r.cfg.ElectionTimeoutMax - r.cfg.ElectionTimeoutMin
	r.electionDeadline = time.Now().Add(r.cfg.ElectionTimeoutMin + time.Duration(r.rng.Int63n(int64(span))))
}

// becomeFollowerLocked steps down to follower, adopting term if it is newer.
func (r *Raft) becomeFollowerLocked(term uint64) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = None
		r.leaderID = None
		r.persistLocked()
	}
	if r.role == Leader {
		r.log.Info("stepping down", "term", r.currentTerm)
		r.stopReplicatorsLocked()
		r.leaderID = None
		r.resetElectionTimerLocked()
	}
	r.role = Follower
}

func (r *Raft) setLeaderLocked(id NodeID) {
	if r.leaderID != id {
		r.leaderID = id
		if id != None {
			r.leaderChanges++
		}
	}
}

// ticker drives election timeouts and the leader's check-quorum.
func (r *Raft) ticker() {
	defer r.wg.Done()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-t.C:
		}
		r.mu.Lock()
		now := time.Now()
		switch r.role {
		case Leader:
			// Check-quorum: step down if a majority has not answered recently,
			// so clients of a partitioned leader are redirected quickly.
			alive := r.quorumLocked(func(id NodeID) bool {
				return id == r.id || now.Sub(r.lastAck[id]) < r.cfg.ElectionTimeoutMax
			})
			if !alive {
				r.log.Info("lost quorum contact", "term", r.currentTerm)
				r.becomeFollowerLocked(r.currentTerm)
			}
		default:
			if now.After(r.electionDeadline) {
				if r.isMemberLocked(r.id) {
					r.startPreVoteLocked()
				} else {
					r.resetElectionTimerLocked()
				}
			}
		}
		r.mu.Unlock()
	}
}
