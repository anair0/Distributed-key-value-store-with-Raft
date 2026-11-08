// Package kv is the replicated key-value service built on top of Raft.
package kv

// Op types carried inside Raft log commands.
const (
	OpSet    = "SET"
	OpDelete = "DELETE"
	OpGet    = "GET"
)

// Command is the payload encoded into a Raft log entry. ClientID+Seq give
// idempotency: the state machine skips duplicates (M4).
type Command struct {
	Op       string
	Key      string
	Value    []byte
	ClientID string
	Seq      uint64
}

// Store is the storage engine behind the state machine. Initially an
// in-memory map (+WAL); later the LSM tree in internal/lsm.
type Store interface {
	Get(key string) (value []byte, ok bool, err error)
	Put(key string, value []byte) error
	Delete(key string) error
	Close() error
}

// StateMachine is applied to committed Raft entries in order.
type StateMachine interface {
	Apply(cmd []byte) (result []byte, err error)
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}
