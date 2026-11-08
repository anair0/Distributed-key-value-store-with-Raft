package raft

// applier is the only goroutine that writes to applyCh, so the state machine
// sees snapshots and entries strictly in log order. It never holds the lock
// while sending, so the service may call back into Raft (e.g. Snapshot)
// while handling a message.
func (r *Raft) applier() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		for !r.killed.Load() && r.pendingSnapshot == nil && r.lastApplied >= r.commitIndex {
			r.applyCond.Wait()
		}
		if r.killed.Load() {
			r.mu.Unlock()
			return
		}

		if snap := r.pendingSnapshot; snap != nil {
			r.pendingSnapshot = nil
			r.mu.Unlock()
			if !r.send(*snap) {
				return
			}
			r.mu.Lock()
			r.lastApplied = max(r.lastApplied, snap.SnapshotIndex)
			r.mu.Unlock()
			continue
		}

		start := max(r.lastApplied+1, r.base()+1)
		end := r.commitIndex
		msgs := make([]ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			e := r.entryAt(i)
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Kind:         e.Kind,
				Command:      e.Command,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			})
		}
		r.mu.Unlock()

		for _, m := range msgs {
			if !r.send(m) {
				return
			}
		}
		r.mu.Lock()
		r.lastApplied = max(r.lastApplied, end)
		r.mu.Unlock()
	}
}

func (r *Raft) send(m ApplyMsg) bool {
	select {
	case r.applyCh <- m:
		return true
	case <-r.done:
		return false
	}
}
