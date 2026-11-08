// Package kv is the replicated key-value service: a deterministic state
// machine fed by Raft, plus the client library.
//
// Client semantics:
//   - Leader discovery: non-leaders answer ErrWrongLeader with a hint; the
//     client retries there (or round-robin).
//   - Exactly-once writes: each client has a unique ID and numbers its
//     requests. The state machine remembers the last sequence number (and
//     reply) per client and skips duplicates, so retries are idempotent.
//   - Linearizable reads: by default through Raft's ReadIndex; optionally
//     through the log (see ReadMode).
package kv

import "github.com/anair0/distributed-key-value-store-with-raft/internal/raft"

// OpType is the kind of request.
type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpDelete
	OpAddMember
	OpRemoveMember
)

func (o OpType) String() string {
	switch o {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpAddMember:
		return "add_member"
	case OpRemoveMember:
		return "remove_member"
	}
	return "unknown"
}

// Err is a request outcome. The empty string means success.
type Err string

const (
	OK             Err = ""
	ErrWrongLeader Err = "wrong leader"
	ErrTimeout     Err = "timeout"
	ErrShutdown    Err = "shutting down"
	ErrBadRequest  Err = "bad request"
	ErrRetry       Err = "retry"
)

// Request is a client request. ClientID+Seq identify it uniquely.
type Request struct {
	ClientID string
	Seq      uint64
	Op       OpType
	Key      string
	Value    []byte
	Member   raft.Member // OpAddMember / OpRemoveMember
}

// Response is the reply to a Request.
type Response struct {
	Err        Err
	Value      []byte
	Found      bool // Get: key exists. Delete: key existed.
	LeaderID   raft.NodeID
	LeaderAddr string
}

// ReadMode selects how Gets are made linearizable.
type ReadMode int

const (
	// ReadIndex confirms leadership with a heartbeat round, waits until the
	// commit index is applied, then reads locally. No log write.
	ReadIndex ReadMode = iota
	// ReadLog sends Gets through the Raft log like writes. Simplest, but
	// every read costs a replicated, fsynced log entry.
	ReadLog
)
