package raft

import (
	"log/slog"
	"time"
)

// Config holds a node's static configuration.
type Config struct {
	// ID of this node.
	ID NodeID
	// Members is the initial cluster configuration, used only when the
	// persister holds no state. Leave it empty for a node that will be added
	// to an existing cluster with AddMember: such a node never starts an
	// election until it learns a configuration that includes it.
	Members []Member

	// HeartbeatInterval is how often the leader contacts each follower.
	HeartbeatInterval time.Duration
	// Election timeouts are drawn uniformly from [Min, Max).
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// RPCTimeout bounds a single AppendEntries / RequestVote call.
	RPCTimeout time.Duration
	// SnapshotRPCTimeout bounds a single InstallSnapshot call.
	SnapshotRPCTimeout time.Duration
	// MaxEntriesPerAppend caps the batch size of one AppendEntries.
	MaxEntriesPerAppend int

	// Logger receives debug logs. Nil discards them.
	Logger *slog.Logger
	// Seed for the election-timeout RNG. Zero means time-based.
	Seed int64
}

func (c Config) withDefaults() Config {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = c.ElectionTimeoutMin / 2
	}
	if c.SnapshotRPCTimeout == 0 {
		c.SnapshotRPCTimeout = 5 * c.ElectionTimeoutMax
	}
	if c.MaxEntriesPerAppend == 0 {
		c.MaxEntriesPerAppend = 256
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano() + int64(c.ID)*7919
	}
	return c
}
