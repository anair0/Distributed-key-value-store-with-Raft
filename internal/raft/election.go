package raft

import (
	"context"
	"time"
)

// Elections run in two phases:
//
//  1. Pre-vote: ask "would you vote for me in term T+1?" without changing
//     any state. A node that cannot win (stale log, or the others still hear
//     from a leader) never increments its term, so it cannot depose a healthy
//     leader after a partition heals.
//  2. Real election (Figure 2): increment the term, vote for self, request
//     votes, become leader on a majority.

// startPreVoteLocked begins the pre-vote phase.
func (r *Raft) startPreVoteLocked() {
	r.resetElectionTimerLocked()
	term := r.currentTerm
	args := &RequestVoteArgs{
		Term:         term + 1,
		CandidateID:  r.id,
		LastLogIndex: r.lastIndex(),
		LastLogTerm:  r.lastTerm(),
		PreVote:      true,
	}
	granted := map[NodeID]bool{r.id: true}
	if r.quorumLocked(func(id NodeID) bool { return granted[id] }) {
		r.startElectionLocked()
		return
	}
	r.log.Debug("pre-vote", "term", args.Term)
	for _, p := range r.peersLocked() {
		go func(p NodeID) {
			reply, err := r.callRequestVote(p, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			// Stale: an election already started, or the term moved on.
			if r.currentTerm != term || r.role == Leader || !reply.VoteGranted {
				return
			}
			granted[p] = true
			if r.quorumLocked(func(id NodeID) bool { return granted[id] }) {
				r.startElectionLocked()
			}
		}(p)
	}
}

// startElectionLocked begins a real election for term currentTerm+1.
func (r *Raft) startElectionLocked() {
	r.role = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.leaderID = None
	r.elections++
	r.persistLocked()
	r.resetElectionTimerLocked()

	term := r.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  r.id,
		LastLogIndex: r.lastIndex(),
		LastLogTerm:  r.lastTerm(),
	}
	r.log.Info("starting election", "term", term)
	votes := map[NodeID]bool{r.id: true}
	if r.quorumLocked(func(id NodeID) bool { return votes[id] }) {
		r.becomeLeaderLocked()
		return
	}
	for _, p := range r.peersLocked() {
		go func(p NodeID) {
			reply, err := r.callRequestVote(p, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			if r.currentTerm != term || r.role != Candidate || !reply.VoteGranted {
				return
			}
			votes[p] = true
			if r.quorumLocked(func(id NodeID) bool { return votes[id] }) {
				r.becomeLeaderLocked()
			}
		}(p)
	}
}

func (r *Raft) callRequestVote(p NodeID, args *RequestVoteArgs) (*RequestVoteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
	defer cancel()
	return r.transport.RequestVote(ctx, p, args)
}

func (r *Raft) becomeLeaderLocked() {
	r.log.Info("became leader", "term", r.currentTerm)
	r.role = Leader
	r.setLeaderLocked(r.id)
	now := time.Now()
	r.nextIndex = map[NodeID]uint64{}
	r.matchIndex = map[NodeID]uint64{}
	r.lastAck = map[NodeID]time.Time{}
	for _, p := range r.peersLocked() {
		r.nextIndex[p] = r.lastIndex() + 1
		r.lastAck[p] = now
	}
	r.replicators = map[NodeID]*replicator{}
	r.syncReplicatorsLocked()
	// A no-op in the new term lets the leader commit (and so learn the
	// commit index of) everything from earlier terms; see Figure 8.
	r.appendLocked(LogEntry{Kind: EntryNoop})
}

// upToDateLocked reports whether a candidate's log is at least as up to
// date as ours (§5.4.1): higher last term wins, equal terms compare length.
func (r *Raft) upToDateLocked(lastIndex, lastTerm uint64) bool {
	if lastTerm != r.lastTerm() {
		return lastTerm > r.lastTerm()
	}
	return lastIndex >= r.lastIndex()
}

// inLeaderLeaseLocked reports whether we have a live leader: we are the
// leader, or we heard from one less than ElectionTimeoutMin ago. Such nodes
// refuse to vote (leader stickiness, Raft thesis §4.2.3), which stops
// removed or partitioned nodes from disrupting the cluster.
func (r *Raft) inLeaderLeaseLocked() bool {
	if r.role == Leader {
		return true
	}
	return r.leaderID != None && time.Since(r.lastLeaderContact) < r.cfg.ElectionTimeoutMin
}

// HandleRequestVote handles RequestVote (and pre-vote) requests.
func (r *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &RequestVoteReply{Term: r.currentTerm}
	if r.killed.Load() {
		return reply
	}

	if args.PreVote {
		reply.VoteGranted = args.Term > r.currentTerm &&
			!r.inLeaderLeaseLocked() &&
			r.upToDateLocked(args.LastLogIndex, args.LastLogTerm)
		return reply
	}

	if args.Term < r.currentTerm {
		return reply
	}
	if args.Term > r.currentTerm {
		if r.inLeaderLeaseLocked() {
			return reply // ignore without adopting the term
		}
		r.becomeFollowerLocked(args.Term)
		reply.Term = r.currentTerm
	}
	if (r.votedFor == None || r.votedFor == args.CandidateID) &&
		r.upToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		r.votedFor = args.CandidateID
		r.persistLocked()
		r.resetElectionTimerLocked()
		reply.VoteGranted = true
	}
	return reply
}
