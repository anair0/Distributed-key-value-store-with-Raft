package raft

import (
	"context"
	"time"
)

// replicator is the per-peer goroutine a leader runs. It keeps at most one
// RPC in flight to its peer, which keeps ordering simple. It wakes up on
// every heartbeat tick and whenever new entries are appended.
type replicator struct {
	peer    NodeID
	term    uint64
	trigger chan struct{}
	stop    chan struct{}
}

func (rp *replicator) kick() {
	select {
	case rp.trigger <- struct{}{}:
	default:
	}
}

// syncReplicatorsLocked starts replicators for new peers and stops those for
// peers no longer in the configuration.
func (r *Raft) syncReplicatorsLocked() {
	if r.killed.Load() {
		return
	}
	want := map[NodeID]bool{}
	for _, p := range r.peersLocked() {
		want[p] = true
		if _, ok := r.replicators[p]; ok {
			continue
		}
		if _, ok := r.nextIndex[p]; !ok {
			r.nextIndex[p] = r.lastIndex() + 1
			r.matchIndex[p] = 0
			r.lastAck[p] = time.Now()
		}
		rp := &replicator{peer: p, term: r.currentTerm, trigger: make(chan struct{}, 1), stop: make(chan struct{})}
		r.replicators[p] = rp
		r.wg.Add(1)
		go r.runReplicator(rp)
		rp.kick()
	}
	for p, rp := range r.replicators {
		if !want[p] {
			close(rp.stop)
			delete(r.replicators, p)
			delete(r.nextIndex, p)
			delete(r.matchIndex, p)
			delete(r.lastAck, p)
		}
	}
}

func (r *Raft) stopReplicatorsLocked() {
	for p, rp := range r.replicators {
		close(rp.stop)
		delete(r.replicators, p)
	}
}

func (r *Raft) triggerReplicatorsLocked() {
	for _, rp := range r.replicators {
		rp.kick()
	}
}

func (r *Raft) runReplicator(rp *replicator) {
	defer r.wg.Done()
	hb := time.NewTicker(r.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-rp.stop:
			return
		case <-hb.C:
		case <-rp.trigger:
		}
		// Keep sending while the peer is behind and RPCs succeed.
		for r.replicateOnce(rp) {
			select {
			case <-r.done:
				return
			case <-rp.stop:
				return
			default:
			}
		}
	}
}

// replicateOnce sends one AppendEntries or InstallSnapshot to the peer. It
// returns true if it should be called again immediately.
func (r *Raft) replicateOnce(rp *replicator) bool {
	r.mu.Lock()
	if r.role != Leader || r.currentTerm != rp.term || r.killed.Load() {
		r.mu.Unlock()
		return false
	}
	next := r.nextIndex[rp.peer]
	if next <= r.base() {
		// The entries the peer needs were compacted away: send the snapshot.
		args := &InstallSnapshotArgs{
			Term:              r.currentTerm,
			LeaderID:          r.id,
			LastIncludedIndex: r.base(),
			LastIncludedTerm:  r.entries[0].Term,
			Members:           r.entries[0].Members,
			Data:              r.snapshot,
		}
		r.mu.Unlock()
		return r.sendSnapshot(rp, args)
	}

	prev := next - 1
	end := min(r.lastIndex(), prev+uint64(r.cfg.MaxEntriesPerAppend))
	args := &AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: prev,
		PrevLogTerm:  r.termAt(prev),
		Entries:      append([]LogEntry(nil), r.entries[next-r.base():end-r.base()+1]...),
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
	reply, err := r.transport.AppendEntries(ctx, rp.peer, args)
	cancel()
	if err != nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		return false
	}
	if r.role != Leader || r.currentTerm != args.Term {
		return false
	}
	r.lastAck[rp.peer] = time.Now()

	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > r.matchIndex[rp.peer] {
			r.matchIndex[rp.peer] = match
			r.advanceCommitLocked()
		}
		if match+1 > r.nextIndex[rp.peer] {
			r.nextIndex[rp.peer] = match + 1
		}
		return r.nextIndex[rp.peer] <= r.lastIndex()
	}
	if reply.ConflictIndex == 0 {
		return false
	}

	// Fast backtracking: if we have entries from ConflictTerm, retry just
	// after our last one; otherwise skip the follower's whole conflicting term.
	ni := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		for i := r.lastIndex(); i > r.base(); i-- {
			t := r.termAt(i)
			if t == reply.ConflictTerm {
				ni = i + 1
				break
			}
			if t < reply.ConflictTerm {
				break
			}
		}
	}
	ni = max(ni, r.matchIndex[rp.peer]+1)
	ni = min(ni, r.lastIndex()+1)
	r.nextIndex[rp.peer] = max(ni, 1)
	return true
}

func (r *Raft) sendSnapshot(rp *replicator, args *InstallSnapshotArgs) bool {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.SnapshotRPCTimeout)
	reply, err := r.transport.InstallSnapshot(ctx, rp.peer, args)
	cancel()
	if err != nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		return false
	}
	if r.role != Leader || r.currentTerm != args.Term {
		return false
	}
	r.lastAck[rp.peer] = time.Now()
	if args.LastIncludedIndex > r.matchIndex[rp.peer] {
		r.matchIndex[rp.peer] = args.LastIncludedIndex
		r.advanceCommitLocked()
	}
	if args.LastIncludedIndex+1 > r.nextIndex[rp.peer] {
		r.nextIndex[rp.peer] = args.LastIncludedIndex + 1
	}
	return r.nextIndex[rp.peer] <= r.lastIndex()
}

// advanceCommitLocked moves commitIndex to the highest index stored on a
// majority, but only for entries of the current term (§5.4.2, Figure 8):
// counting replicas of an older-term entry is not enough to commit it.
func (r *Raft) advanceCommitLocked() {
	if r.role != Leader {
		return
	}
	for n := r.lastIndex(); n > r.commitIndex; n-- {
		if r.termAt(n) != r.currentTerm {
			break // terms only decrease going back
		}
		if r.quorumLocked(func(id NodeID) bool { return id == r.id || r.matchIndex[id] >= n }) {
			r.commitIndex = n
			r.applyCond.Broadcast()
			break
		}
	}
	// A leader that removed itself steps down once that change commits.
	if !r.isMemberLocked(r.id) && r.commitIndex >= r.lastConfigIndexLocked() {
		r.log.Info("removed from cluster, stepping down")
		r.becomeFollowerLocked(r.currentTerm)
	}
}

// HandleAppendEntries handles AppendEntries requests (and heartbeats).
func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &AppendEntriesReply{Term: r.currentTerm}
	if r.killed.Load() || args.Term < r.currentTerm {
		return reply
	}
	// A valid leader for this term exists: it is args.LeaderID.
	if args.Term > r.currentTerm || r.role != Follower {
		r.becomeFollowerLocked(args.Term)
	}
	reply.Term = r.currentTerm
	r.setLeaderLocked(args.LeaderID)
	r.lastLeaderContact = time.Now()
	r.resetElectionTimerLocked()

	prevIdx, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	if prevIdx < r.base() {
		// Part of the request is covered by our snapshot: those entries are
		// committed, so they must match. Skip them.
		skip := r.base() - prevIdx
		if uint64(len(entries)) <= skip {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prevIdx, prevTerm = r.base(), r.entries[0].Term
	}

	// Consistency check.
	if prevIdx > r.lastIndex() {
		reply.ConflictIndex = r.lastIndex() + 1
		return reply
	}
	if t := r.termAt(prevIdx); t != prevTerm {
		reply.ConflictTerm = t
		i := prevIdx
		for i > r.base()+1 && r.termAt(i-1) == t {
			i--
		}
		reply.ConflictIndex = i
		return reply
	}

	// Append new entries, truncating only at the first real conflict (a
	// delayed, shorter AppendEntries must never truncate newer entries).
	changed := false
	for i, e := range entries {
		idx := prevIdx + 1 + uint64(i)
		if idx <= r.lastIndex() {
			if r.termAt(idx) == e.Term {
				continue
			}
			if idx <= r.commitIndex {
				panic("raft: leader tried to overwrite a committed entry")
			}
			cut := idx - r.base()
			r.entries = r.entries[:cut:cut]
		}
		r.entries = append(r.entries, entries[i:]...)
		changed = true
		break
	}
	if changed {
		r.recomputeMembersLocked()
		r.persistLocked()
	}

	lastNew := prevIdx + uint64(len(entries))
	if c := min(args.LeaderCommit, lastNew); c > r.commitIndex {
		r.commitIndex = c
		r.applyCond.Broadcast()
	}
	reply.Success = true
	return reply
}
