package raft_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// M3: persistence across crashes.

func (c *cluster) restart(i int) {
	c.startNode(i, c.members())
	c.connect(i)
}

func TestPersistAll(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one("11", 3, true)

	// Crash and restart everyone.
	for i := range 3 {
		c.crash(i)
	}
	for i := range 3 {
		c.restart(i)
	}
	c.one("12", 3, true)

	leader1 := c.checkOneLeader()
	c.crash(leader1)
	c.restart(leader1)
	c.one("13", 3, true)

	leader2 := c.checkOneLeader()
	c.crash(leader2)
	idx := c.one("14", 2, true)
	c.restart(leader2)
	c.wait(idx, 3, 0) // the restarted node catches up
}

func TestPersistRepeatedLeaderCrash(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	index := 1
	for iters := range 5 {
		c.one(fmt.Sprintf("%d", 10+index), 5, true)
		index++
		leader1 := c.checkOneLeader()
		c.crash((leader1 + 1) % 5)
		c.crash((leader1 + 2) % 5)
		c.one(fmt.Sprintf("%d", 10+index), 3, true)
		index++
		c.crash(leader1)
		c.crash((leader1 + 3) % 5)
		c.crash((leader1 + 4) % 5)
		c.restart((leader1 + 1) % 5)
		c.restart((leader1 + 2) % 5)
		time.Sleep(time.Second)
		c.restart((leader1 + 3) % 5)
		c.one(fmt.Sprintf("%d", 10+index), 3, true)
		index++
		c.restart((leader1 + 4) % 5)
		c.restart(leader1)
		_ = iters
	}
	c.one("1000", 5, true)
}

// TestFigure8 recreates the scenario of Figure 8 in the paper many times at
// random: leaders crash right after appending entries, so old-term entries
// sit on a majority without being committed. A leader that committed them
// by counting replicas would lose committed data; the apply checker would
// then see two different commands at the same index.
func TestFigure8(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one("start", 1, true)
	up := 5
	for iters := range 150 {
		leader := -1
		for i := range 5 {
			if rf := c.raft(i); rf != nil {
				if _, _, ok := rf.Start([]byte(fmt.Sprintf("f8-%d", iters))); ok {
					leader = i
				}
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}
		if leader != -1 {
			c.crash(leader)
			up--
		}
		if up < 3 {
			if s := rand.Intn(5); c.raft(s) == nil {
				c.restart(s)
				up++
			}
		}
	}
	for i := range 5 {
		if c.raft(i) == nil {
			c.restart(i)
		}
	}
	c.one("final", 5, true)
}

func TestFigure8Unreliable(t *testing.T) {
	c := newCluster(t, 5, false, 0)
	c.net.SetLongReordering(true)
	c.one("start", 1, true)
	nup := 5
	for iters := range 300 {
		if iters == 200 {
			c.net.SetLongReordering(false)
		}
		leader := -1
		for i := range 5 {
			if _, _, ok := c.raft(i).Start([]byte(fmt.Sprintf("f8u-%d", iters))); ok {
				c.mu.Lock()
				conn := c.connected[i]
				c.mu.Unlock()
				if conn {
					leader = i
				}
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Intn(45)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}
		if leader != -1 && rand.Intn(1000) < 500 {
			c.disconnect(leader)
			nup--
		}
		if nup < 3 {
			s := rand.Intn(5)
			c.mu.Lock()
			conn := c.connected[s]
			c.mu.Unlock()
			if !conn {
				c.connect(s)
				nup++
			}
		}
	}
	for i := range 5 {
		c.connect(i)
	}
	c.net.SetReliable(true)
	c.one("final", 5, true)
}
