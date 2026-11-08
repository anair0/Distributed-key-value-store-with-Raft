// Package raft implements the Raft consensus algorithm.
//
// The package depends only on the interfaces declared here (Transport,
// Persister, and the apply channel), so it can be tested with a fake network
// and in-memory storage. Follow Figure 2 of the Raft paper literally.
package raft

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
	return "Unknown"
}

// NodeID identifies a node in the cluster.
type NodeID int

// LogEntry is a single replicated command.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// ApplyMsg is delivered to the state machine on the apply channel once an
// entry is committed (or when a snapshot must be installed).
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// RequestVoteArgs / Reply: RequestVote RPC (paper Figure 2).
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs / Reply: AppendEntries RPC (also used as heartbeat).
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// Fast-backup hints (M2 optimization).
	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshotArgs / Reply: InstallSnapshot RPC (M6).
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}
