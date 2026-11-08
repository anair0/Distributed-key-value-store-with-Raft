package raft_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
)

// M6: snapshots and InstallSnapshot.

const snapEvery = 10

func snapCommon(t *testing.T, disconnect, reliable, crash bool) {
	c := newCluster(t, 3, reliable, snapEvery)
	c.one("init", 3, true)
	leader1 := c.checkOneLeader()

	for i := range 20 {
		victim := (leader1 + 1) % 3
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % 3
			victim = leader1
		}
		if disconnect {
			c.disconnect(victim)
			c.one(fmt.Sprintf("d-%d", i), 2, true)
		}
		if crash {
			c.crash(victim)
			c.one(fmt.Sprintf("c-%d", i), 2, true)
		}
		// Enough entries that the victim will need a snapshot.
		for j := range snapEvery + 1 {
			c.raft(sender).Start([]byte(fmt.Sprintf("x-%d-%d", i, j)))
		}
		c.one(fmt.Sprintf("y-%d", i), 2, true)

		// The log must have been compacted.
		if st := c.raft(sender).Status(); st.LastLogIndex-st.SnapshotIndex > 4*snapEvery {
			t.Fatalf("log not trimmed: last=%d snapshot=%d", st.LastLogIndex, st.SnapshotIndex)
		}
		if disconnect {
			c.connect(victim)
			c.one(fmt.Sprintf("dd-%d", i), 3, true)
			leader1 = c.checkOneLeader()
		}
		if crash {
			c.restart(victim)
			c.one(fmt.Sprintf("cc-%d", i), 3, true)
			leader1 = c.checkOneLeader()
		}
	}
}

func TestSnapshotBasic(t *testing.T)             { snapCommon(t, false, true, false) }
func TestSnapshotInstall(t *testing.T)           { snapCommon(t, true, true, false) }
func TestSnapshotInstallUnreliable(t *testing.T) { snapCommon(t, true, false, false) }
func TestSnapshotInstallCrash(t *testing.T)      { snapCommon(t, false, true, true) }

func TestSnapshotAllCrash(t *testing.T) {
	c := newCluster(t, 3, false, snapEvery)
	c.one("init", 3, true)
	for i := range 5 {
		for j := range snapEvery + 1 {
			c.one(fmt.Sprintf("%d-%d", i, j), 3, true)
		}
		idx1 := c.one(fmt.Sprintf("a-%d", i), 3, true)
		for k := range 3 {
			c.crash(k)
		}
		for k := range 3 {
			c.restart(k)
		}
		idx2 := c.one(fmt.Sprintf("b-%d", i), 3, true)
		if idx2 <= idx1 {
			t.Fatalf("index decreased from %d to %d", idx1, idx2)
		}
	}
}

// ---- ReadIndex ----

func TestReadIndex(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	idx := c.one("w", 3, true)
	leader := c.checkOneLeader()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ri, err := c.raft(leader).ReadIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ri < idx {
		t.Fatalf("read index %d < committed write %d", ri, idx)
	}
	if _, err := c.raft((leader + 1) % 3).ReadIndex(ctx); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("follower ReadIndex: got %v, want ErrNotLeader", err)
	}

	// An isolated leader must not be able to confirm leadership.
	c.disconnect(leader)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel2()
	if _, err := c.raft(leader).ReadIndex(ctx2); err == nil {
		t.Fatal("isolated leader served a ReadIndex")
	}
}

// ---- membership changes ----

func TestMembershipChange(t *testing.T) {
	c := newCluster(t, 3, true, snapEvery)
	for i := range 2 * snapEvery {
		c.one(fmt.Sprintf("pre-%d", i), 3, true)
	}

	// Grow the cluster to 5 with fresh nodes that know nothing; they catch up
	// via InstallSnapshot.
	grow := func() {
		c.mu.Lock()
		id := c.n
		c.n++
		c.rafts = append(c.rafts, nil)
		c.persisters = append(c.persisters, raft.NewMemoryPersister())
		c.applied = append(c.applied, map[uint64]string{})
		c.lastIdx = append(c.lastIdx, 0)
		c.connected = append(c.connected, true)
		c.applyErr = append(c.applyErr, "")
		c.mu.Unlock()
		c.startNode(id, nil) // joining node: empty configuration
		c.connect(id)
		changeMembership(t, c, func(rf *raft.Raft) (uint64, error) {
			idx, _, err := rf.AddMember(raft.Member{ID: raft.NodeID(id)})
			return idx, err
		})
	}
	grow()
	grow()
	c.one("five", 5, true)

	// Remove the leader and one follower; the remaining three carry on.
	leader := c.checkOneLeader()
	changeMembership(t, c, func(rf *raft.Raft) (uint64, error) {
		idx, _, err := rf.RemoveMember(raft.NodeID(leader))
		return idx, err
	})
	time.Sleep(time.Second)
	c.disconnect(leader)
	newLeader := c.checkOneLeader()
	victim := (newLeader + 1) % c.n
	if victim == leader {
		victim = (victim + 1) % c.n
	}
	changeMembership(t, c, func(rf *raft.Raft) (uint64, error) {
		idx, _, err := rf.RemoveMember(raft.NodeID(victim))
		return idx, err
	})
	c.disconnect(victim)
	for i := range 5 {
		c.one(fmt.Sprintf("post-%d", i), 3, true)
	}
	st := c.raft(c.checkOneLeader()).Status()
	if len(st.Members) != 3 {
		t.Fatalf("expected 3 members, got %v", st.Members)
	}
}

// changeMembership retries op on the leader until it succeeds and the
// configuration entry commits.
func changeMembership(t *testing.T, c *cluster, op func(*raft.Raft) (uint64, error)) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		rf := c.raft(c.checkOneLeader())
		idx, err := op(rf)
		if err == nil {
			for t0 := time.Now(); time.Since(t0) < 3*time.Second; time.Sleep(20 * time.Millisecond) {
				if rf.Status().CommitIndex >= idx {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("membership change did not complete")
}
