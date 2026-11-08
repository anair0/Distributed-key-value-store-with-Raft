// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout).
//
// The implementation follows Figure 2 of the paper, plus:
//   - fast log backtracking (ConflictTerm / ConflictIndex),
//   - snapshots and InstallSnapshot (log compaction),
//   - Pre-Vote and leader stickiness, so a partitioned node cannot disrupt a
//     healthy leader when it rejoins,
//   - check-quorum, so a leader cut off from the majority steps down,
//   - ReadIndex, for linearizable reads without writing to the log,
//   - single-server membership changes.
//
// The package depends only on the Transport and Persister interfaces, so
// tests run whole clusters in one process over a fault-injecting network.
package raft

import (
	"errors"
	"fmt"
)

// Role is the state a node is currently in.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return fmt.Sprintf("Role(%d)", int(r))
}

// NodeID identifies a node in the cluster.
type NodeID int

// None means "no node" (no vote cast, no known leader).
const None NodeID = -1

// EntryKind distinguishes client commands from internal entries.
type EntryKind uint8

const (
	// EntryNormal carries an opaque state machine command.
	EntryNormal EntryKind = iota
	// EntryNoop is appended by every new leader so it can commit an entry
	// from its own term (needed before serving ReadIndex or config changes).
	EntryNoop
	// EntryConfig carries a new cluster membership.
	EntryConfig
)

func (k EntryKind) String() string {
	switch k {
	case EntryNormal:
		return "normal"
	case EntryNoop:
		return "noop"
	case EntryConfig:
		return "config"
	}
	return fmt.Sprintf("EntryKind(%d)", int(k))
}

// Member is one voting node of the cluster. Addr is only used by real
// network transports; the in-memory test network ignores it.
type Member struct {
	ID   NodeID
	Addr string
}

// LogEntry is a single replicated log entry.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Kind    EntryKind
	Command []byte   // EntryNormal
	Members []Member // EntryConfig
}

// ApplyMsg is delivered, in log order, on the apply channel. Exactly one of
// CommandValid or SnapshotValid is set.
type ApplyMsg struct {
	CommandValid bool
	Kind         EntryKind
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// RequestVoteArgs is the RequestVote RPC request. With PreVote set the
// receiver only answers "would you vote for me?" and changes no state.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
}

// RequestVoteReply is the RequestVote RPC response.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the AppendEntries RPC request (also the heartbeat).
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the AppendEntries RPC response. On a failed
// consistency check, ConflictTerm/ConflictIndex let the leader skip back a
// whole term at a time instead of one entry per round trip.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictTerm  uint64 // term of the conflicting entry, 0 if follower log is too short
	ConflictIndex uint64 // first index the leader should try next
}

// InstallSnapshotArgs is the InstallSnapshot RPC request. The whole snapshot
// is sent in one message (no chunking).
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Members           []Member // configuration as of LastIncludedIndex
	Data              []byte
}

// InstallSnapshotReply is the InstallSnapshot RPC response.
type InstallSnapshotReply struct {
	Term uint64
}

// Status is a point-in-time view of a node, for metrics, debugging and UIs.
type Status struct {
	ID            NodeID
	Role          Role
	Term          uint64
	LeaderID      NodeID
	VotedFor      NodeID
	CommitIndex   uint64
	LastApplied   uint64
	LastLogIndex  uint64
	LastLogTerm   uint64
	SnapshotIndex uint64
	Members       []Member
	Elections     uint64 // elections started by this node
	LeaderChanges uint64 // times this node observed a new leader
}

var (
	// ErrNotLeader is returned by leader-only operations on a non-leader.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrNotReady means the leader has not yet committed an entry from its
	// own term, so it cannot know the true commit index yet. Retry shortly.
	ErrNotReady = errors.New("raft: leader not ready")
	// ErrChangeInProgress means a membership change is still uncommitted.
	ErrChangeInProgress = errors.New("raft: membership change in progress")
	// ErrShutdown is returned after Kill.
	ErrShutdown = errors.New("raft: node is shut down")
	// ErrLeadershipLost means leadership could not be confirmed.
	ErrLeadershipLost = errors.New("raft: leadership could not be confirmed")
)
