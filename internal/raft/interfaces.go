package raft

import (
	"context"
	"time"
)

// Transport sends RPCs to peers. Implementations: gRPC (internal/transport)
// and an in-memory fault-injecting network (test harness). Calls return an
// error if the message was dropped or the peer is unreachable.
type Transport interface {
	RequestVote(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, to NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// PersistentState is what must be durable before replying to any RPC.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    NodeID // -1 if none
	Log         []LogEntry
}

// Persister durably stores Raft state and snapshots. Save must fsync.
type Persister interface {
	SaveState(st PersistentState) error
	LoadState() (PersistentState, bool, error)
	SaveSnapshot(data []byte, lastIncludedIndex, lastIncludedTerm uint64) error
	LoadSnapshot() (data []byte, lastIncludedIndex, lastIncludedTerm uint64, err error)
}

// Clock abstracts time so tests can control election/heartbeat timing.
type Clock interface {
	NewTimer(d time.Duration) Timer
}

// Timer is a resettable timer.
type Timer interface {
	C() <-chan struct{}
	Reset(d time.Duration)
	Stop()
}
