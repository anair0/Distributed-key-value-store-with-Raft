package raft

import (
	"slices"
	"time"
)

// Snapshot tells Raft that the service has captured its state up to and
// including index, so the log up to there can be discarded. The service
// must have applied index already.
func (r *Raft) Snapshot(index uint64, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.killed.Load() || index <= r.base() || index > r.commitIndex || index > r.lastIndex() {
		return
	}
	sentinel := LogEntry{
		Index:   index,
		Term:    r.termAt(index),
		Kind:    EntryConfig,
		Members: r.membersAtLocked(index),
	}
	rest := r.entries[index-r.base()+1:]
	r.entries = append([]LogEntry{sentinel}, rest...)
	r.snapshot = data
	r.persistWithSnapshotLocked()
	r.log.Debug("snapshot", "index", index)
}

// membersAtLocked returns the configuration in effect at index.
func (r *Raft) membersAtLocked(index uint64) []Member {
	for i := index - r.base(); ; i-- {
		if r.entries[i].Kind == EntryConfig {
			return slices.Clone(r.entries[i].Members)
		}
		if i == 0 {
			return nil // unreachable: entries[0] is always a config entry
		}
	}
}

// HandleInstallSnapshot handles InstallSnapshot from a leader.
func (r *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &InstallSnapshotReply{Term: r.currentTerm}
	if r.killed.Load() || args.Term < r.currentTerm {
		return reply
	}
	if args.Term > r.currentTerm || r.role != Follower {
		r.becomeFollowerLocked(args.Term)
	}
	reply.Term = r.currentTerm
	r.setLeaderLocked(args.LeaderID)
	r.lastLeaderContact = time.Now()
	r.resetElectionTimerLocked()

	if args.LastIncludedIndex <= r.commitIndex {
		return reply // we already have (and may have applied) all of it
	}

	sentinel := LogEntry{
		Index:   args.LastIncludedIndex,
		Term:    args.LastIncludedTerm,
		Kind:    EntryConfig,
		Members: slices.Clone(args.Members),
	}
	newEntries := []LogEntry{sentinel}
	// If our log already contains the snapshot's last entry, keep the
	// entries after it (Figure 13, step 6); otherwise discard the whole log.
	if args.LastIncludedIndex <= r.lastIndex() && args.LastIncludedIndex >= r.base() &&
		r.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		newEntries = append(newEntries, r.entries[args.LastIncludedIndex-r.base()+1:]...)
	}
	r.entries = newEntries
	r.snapshot = args.Data
	r.commitIndex = args.LastIncludedIndex
	r.recomputeMembersLocked()
	r.persistWithSnapshotLocked()

	r.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	r.applyCond.Broadcast()
	return reply
}
