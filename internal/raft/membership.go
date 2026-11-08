package raft

import "slices"

// Membership changes use the single-server approach (Raft thesis §4.1):
// add or remove one node at a time. Any majority of the old configuration
// and any majority of the new one then overlap, so no joint consensus is
// needed. Rules enforced here:
//
//   - a configuration takes effect as soon as it is appended to a log,
//     committed or not (recomputeMembersLocked),
//   - only one change may be uncommitted at a time,
//   - the leader must have committed an entry in its current term first
//     (fixes the bug described on the raft-dev list in 2015),
//   - a leader that removes itself keeps leading until the change commits,
//     without counting itself in majorities, then steps down.

// AddMember proposes adding m to the cluster. It returns the log index of
// the configuration entry; the change is done when that index is applied.
func (r *Raft) AddMember(m Member) (index, term uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.canChangeMembershipLocked(); err != nil {
		return 0, 0, err
	}
	members := slices.Clone(r.members)
	for i, old := range members {
		if old.ID == m.ID {
			if old.Addr == m.Addr {
				return r.lastConfigIndexLocked(), r.currentTerm, nil // already a member
			}
			members[i] = m // address change only
			return r.appendConfigLocked(members)
		}
	}
	return r.appendConfigLocked(append(members, m))
}

// RemoveMember proposes removing node id from the cluster.
func (r *Raft) RemoveMember(id NodeID) (index, term uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.canChangeMembershipLocked(); err != nil {
		return 0, 0, err
	}
	members := slices.DeleteFunc(slices.Clone(r.members), func(m Member) bool { return m.ID == id })
	if len(members) == len(r.members) {
		return r.lastConfigIndexLocked(), r.currentTerm, nil // not a member
	}
	if len(members) == 0 {
		return 0, 0, ErrChangeInProgress
	}
	return r.appendConfigLocked(members)
}

func (r *Raft) canChangeMembershipLocked() error {
	if r.killed.Load() {
		return ErrShutdown
	}
	if r.role != Leader {
		return ErrNotLeader
	}
	if r.termAt(r.commitIndex) != r.currentTerm {
		return ErrNotReady
	}
	if r.lastConfigIndexLocked() > r.commitIndex {
		return ErrChangeInProgress
	}
	return nil
}

func (r *Raft) appendConfigLocked(members []Member) (uint64, uint64, error) {
	idx := r.appendLocked(LogEntry{Kind: EntryConfig, Members: members})
	r.log.Info("membership change", "index", idx, "members", members)
	return idx, r.currentTerm, nil
}
