package raft_test

import (
	"math/rand"
	"testing"
	"time"
)

// M1: leader election.

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.checkOneLeader()
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, expected >= 1", term1)
	}
	// With no failures, the leader keeps its job and the term stays put.
	time.Sleep(2 * time.Second)
	if term2 := c.checkTerms(); term1 != term2 {
		t.Fatalf("term changed from %d to %d with no failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	leader1 := c.checkOneLeader()

	// Leader disconnects: a new one is elected.
	c.disconnect(leader1)
	c.checkOneLeader()

	// The old leader rejoins: it must not disturb the new leader.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// No quorum: no leader.
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % 3)
	time.Sleep(2 * time.Second)
	c.checkNoLeader()

	// Quorum restored.
	c.connect((leader2 + 1) % 3)
	c.checkOneLeader()
	c.connect(leader2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	const n = 7
	c := newCluster(t, n, true, 0)
	c.checkOneLeader()
	for range 8 {
		// Disconnect three random nodes; four remain, still a majority.
		i1, i2, i3 := rand.Intn(n), rand.Intn(n), rand.Intn(n)
		c.disconnect(i1)
		c.disconnect(i2)
		c.disconnect(i3)
		c.checkOneLeader()
		c.connect(i1)
		c.connect(i2)
		c.connect(i3)
	}
	c.checkOneLeader()
}

// Pre-vote + leader stickiness: an isolated follower that rejoins must not
// force an election (its term must not have run away while it was alone).
func TestPartitionedFollowerDoesNotDisrupt(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	leader := c.checkOneLeader()
	term, _ := c.raft(leader).State()

	f := (leader + 1) % 3
	c.disconnect(f)
	time.Sleep(3 * time.Second) // many election timeouts
	c.connect(f)
	c.one("after-rejoin", 3, true)

	if l2 := c.checkOneLeader(); l2 != leader {
		t.Fatalf("leader changed from %d to %d after a follower rejoined", leader, l2)
	}
	if term2, _ := c.raft(leader).State(); term2 != term {
		t.Fatalf("term changed from %d to %d after a follower rejoined", term, term2)
	}
}

// Check-quorum: a leader cut off from the majority steps down by itself.
func TestIsolatedLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	leader := c.checkOneLeader()
	c.disconnect(leader)
	time.Sleep(1500 * time.Millisecond)
	if _, isLeader := c.raft(leader).State(); isLeader {
		t.Fatal("isolated leader did not step down")
	}
}
