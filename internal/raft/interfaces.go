package raft

import "context"

// Transport sends RPCs to peers. Implementations: the in-memory
// fault-injecting network (internal/transport/fakenet) and TCP
// (internal/transport/tcp). A call returns an error if the request or reply
// was lost or the peer is unreachable; Raft simply retries later.
type Transport interface {
	RequestVote(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, to NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Handler is the receiving side of the RPCs; *Raft implements it. Transports
// dispatch incoming requests to it.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}

// AddressBook is optionally implemented by a Transport that needs network
// addresses. Raft calls it whenever the cluster membership changes.
type AddressBook interface {
	SetMembers(members []Member)
}

// PersistentState is the state that must be durable before replying to any
// RPC (Figure 2). Log[0] is a sentinel holding the index, term and
// membership of the latest snapshot (all zero / initial config if none).
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    NodeID
	Log         []LogEntry
}

// Persister durably stores Raft state and the latest snapshot. Save methods
// must not return until the data is on stable storage (fsync).
type Persister interface {
	SaveState(st PersistentState) error
	// SaveStateAndSnapshot atomically (from the reader's point of view)
	// replaces both the state and the snapshot.
	SaveStateAndSnapshot(st PersistentState, snapshot []byte) error
	// LoadState returns ok=false if nothing was ever saved.
	LoadState() (st PersistentState, ok bool, err error)
	LoadSnapshot() ([]byte, error)
	// StateSize is the approximate persisted size of the Raft state (mostly
	// the log). The service uses it to decide when to take a snapshot.
	StateSize() int
}
