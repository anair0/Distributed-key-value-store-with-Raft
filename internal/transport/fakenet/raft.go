package fakenet

import (
	"context"
	"fmt"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
)

// NodeName is the endpoint name used for Raft node id.
func NodeName(id raft.NodeID) string { return fmt.Sprintf("node-%d", id) }

// RaftTransport implements raft.Transport over the fake network. Peers are
// addressed as Prefix+NodeName(id), so several Raft groups can share one
// network.
type RaftTransport struct {
	Net    *Network
	From   string
	Prefix string
}

func (t *RaftTransport) RequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	reply := &raft.RequestVoteReply{}
	return reply, t.Net.Call(ctx, t.From, t.Prefix+NodeName(to), "Raft.RequestVote", args, reply)
}

func (t *RaftTransport) AppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	reply := &raft.AppendEntriesReply{}
	return reply, t.Net.Call(ctx, t.From, t.Prefix+NodeName(to), "Raft.AppendEntries", args, reply)
}

func (t *RaftTransport) InstallSnapshot(ctx context.Context, to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	reply := &raft.InstallSnapshotReply{}
	return reply, t.Net.Call(ctx, t.From, t.Prefix+NodeName(to), "Raft.InstallSnapshot", args, reply)
}

// RegisterRaft exposes h's RPC handlers on s.
func RegisterRaft(s *Server, h raft.Handler) {
	Handle(s, "Raft.RequestVote", h.HandleRequestVote)
	Handle(s, "Raft.AppendEntries", h.HandleAppendEntries)
	Handle(s, "Raft.InstallSnapshot", h.HandleInstallSnapshot)
}
