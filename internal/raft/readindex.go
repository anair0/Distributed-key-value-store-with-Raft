package raft

import (
	"context"
	"time"
)

// ReadIndex implements linearizable reads without writing to the log
// (Raft thesis §6.4):
//
//  1. The leader records its commit index as the read index. This requires
//     an entry from its own term to be committed, or the leader might not
//     know the true commit index yet (hence the no-op on election).
//  2. It confirms it is still leader by exchanging a round of heartbeats
//     with a majority. A deposed leader cannot get a majority to answer
//     with its term, so it can never serve stale data.
//  3. The caller waits until its state machine has applied the read index,
//     then reads locally.
//
// Compared to sending reads through the log this costs one round of
// heartbeats instead of a disk write on a majority. Leader leases would
// avoid even that, but depend on bounded clock drift for safety.
func (r *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	for {
		r.mu.Lock()
		if r.killed.Load() {
			r.mu.Unlock()
			return 0, ErrShutdown
		}
		if r.role != Leader {
			r.mu.Unlock()
			return 0, ErrNotLeader
		}
		if r.termAt(r.commitIndex) == r.currentTerm {
			break // still holding the lock
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ErrNotReady
		case <-time.After(5 * time.Millisecond):
		}
	}
	readIndex := r.commitIndex
	term := r.currentTerm
	peers := r.peersLocked()
	args := &AppendEntriesArgs{
		// PrevLogIndex 0 always matches (it is at or before every follower's
		// snapshot), and with no entries the follower's log is untouched.
		Term:         term,
		LeaderID:     r.id,
		LeaderCommit: r.commitIndex,
	}
	acks := map[NodeID]bool{r.id: true}
	if r.quorumLocked(func(id NodeID) bool { return acks[id] }) {
		r.mu.Unlock()
		return readIndex, nil
	}
	r.mu.Unlock()

	type result struct {
		peer NodeID
		ok   bool
	}
	results := make(chan result, len(peers))
	for _, p := range peers {
		go func(p NodeID) {
			cctx, cancel := context.WithTimeout(ctx, r.cfg.RPCTimeout)
			defer cancel()
			reply, err := r.transport.AppendEntries(cctx, p, args)
			if err != nil {
				results <- result{p, false}
				return
			}
			r.mu.Lock()
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
			} else if reply.Term == term && r.role == Leader && r.currentTerm == term {
				r.lastAck[p] = time.Now()
			}
			r.mu.Unlock()
			results <- result{p, reply.Term == term}
		}(p)
	}

	for range peers {
		var res result
		select {
		case res = <-results:
		case <-ctx.Done():
			return 0, ErrLeadershipLost
		}
		if !res.ok {
			continue
		}
		acks[res.peer] = true
		r.mu.Lock()
		stillLeader := r.role == Leader && r.currentTerm == term
		ok := r.quorumLocked(func(id NodeID) bool { return acks[id] })
		r.mu.Unlock()
		if !stillLeader {
			return 0, ErrNotLeader
		}
		if ok {
			return readIndex, nil
		}
	}
	return 0, ErrLeadershipLost
}
