package raft_test

import (
	"fmt"
	"testing"
	"time"
)

// M2: log replication.

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	var last uint64
	for i := range 5 {
		idx := c.one(fmt.Sprintf("cmd-%d", i), 3, false)
		if idx <= last {
			t.Fatalf("index went from %d to %d", last, idx)
		}
		last = idx
	}
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one("101", 3, false)

	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 1) % 3)
	// The remaining two can still commit.
	c.one("102", 2, false)
	time.Sleep(time.Second)
	c.one("103", 2, false)

	// Disconnect the other follower: nothing can commit now.
	leader2 := c.checkOneLeader()
	c.disconnect((leader2 + 1) % 3)
	c.disconnect((leader2 + 2) % 3)
	idx, _, ok := c.raft(leader2).Start([]byte("104"))
	if !ok {
		t.Fatal("leader rejected Start()")
	}
	time.Sleep(2 * time.Second)
	if n, _ := c.nCommitted(idx); n > 0 {
		t.Fatalf("%d committed but no majority", n)
	}
}

func TestLeaderFailure(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one("101", 3, false)
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.one("102", 2, true)
	c.one("103", 2, true)
}

func TestFailAgree(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one("101", 3, false)
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 3)
	c.one("102", 2, false)
	c.one("103", 2, false)
	c.one("104", 2, false)
	c.connect((leader + 1) % 3)
	// The rejoined follower catches up.
	c.one("105", 3, true)
	c.one("106", 3, true)
}

func TestFailNoAgree(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one("10", 5, false)
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 5)
	c.disconnect((leader + 2) % 5)
	c.disconnect((leader + 3) % 5)

	idx, _, ok := c.raft(leader).Start([]byte("20"))
	if !ok {
		t.Fatal("leader rejected Start()")
	}
	time.Sleep(2 * time.Second)
	if n, _ := c.nCommitted(idx); n > 0 {
		t.Fatalf("%d committed but no majority", n)
	}

	c.connect((leader + 1) % 5)
	c.connect((leader + 2) % 5)
	c.connect((leader + 3) % 5)
	c.one("30", 5, true)
}

func TestRejoin(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one("101", 3, true)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	// The old leader appends entries that will never commit.
	c.raft(leader1).Start([]byte("102"))
	c.raft(leader1).Start([]byte("103"))
	c.raft(leader1).Start([]byte("104"))

	c.one("103", 2, true)

	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.connect(leader1)
	c.one("104", 2, true)

	c.connect(leader2)
	c.one("105", 3, true)
}

// TestBackup forces a follower to discard many conflicting entries, which
// exercises fast backtracking.
func TestBackup(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one("start", 5, true)

	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % 5)
	c.disconnect((leader1 + 3) % 5)
	c.disconnect((leader1 + 4) % 5)
	// Lots of entries that will not commit.
	for i := range 50 {
		c.raft(leader1).Start([]byte(fmt.Sprintf("dead-%d", i)))
	}
	time.Sleep(500 * time.Millisecond)

	c.disconnect(leader1)
	c.disconnect((leader1 + 1) % 5)
	c.connect((leader1 + 2) % 5)
	c.connect((leader1 + 3) % 5)
	c.connect((leader1 + 4) % 5)
	for i := range 50 {
		c.one(fmt.Sprintf("live-%d", i), 3, true)
	}

	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % 5
	if other == leader2 {
		other = (leader2 + 1) % 5
	}
	c.disconnect(other)
	for i := range 50 {
		c.raft(leader2).Start([]byte(fmt.Sprintf("dead2-%d", i)))
	}
	time.Sleep(500 * time.Millisecond)

	for i := range 5 {
		c.disconnect(i)
	}
	c.connect(leader1)
	c.connect((leader1 + 1) % 5)
	c.connect(other)
	for i := range 50 {
		c.one(fmt.Sprintf("live2-%d", i), 3, true)
	}

	for i := range 5 {
		c.connect(i)
	}
	c.one("end", 5, true)
}

// TestUnreliableAgree replicates while the network drops, delays,
// duplicates and reorders messages.
func TestUnreliableAgree(t *testing.T) {
	c := newCluster(t, 5, false, 0)
	done := make(chan struct{})
	for iters := 1; iters < 20; iters++ {
		for j := range 4 {
			go func(cmd string) {
				c.one(cmd, 1, true)
				done <- struct{}{}
			}(fmt.Sprintf("%d-%d", iters, j))
		}
		c.one(fmt.Sprintf("%d", iters), 1, true)
	}
	for range 19 * 4 {
		<-done
	}
	c.net.SetReliable(true)
	c.one("final", 5, true)
}

// TestCount checks that an idle, healthy cluster does not send an
// unreasonable number of RPCs (heartbeats only).
func TestCount(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.checkOneLeader()
	before := c.net.Calls()
	time.Sleep(time.Second)
	got := c.net.Calls() - before
	// 2 followers * 20 heartbeats/s, with generous slack.
	if got > 100 {
		t.Fatalf("too many RPCs (%d) in one idle second", got)
	}
}
