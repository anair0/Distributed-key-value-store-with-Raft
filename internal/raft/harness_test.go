package raft_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/transport/fakenet"
)

// cluster is an in-process Raft cluster over the fake network, modelled on
// the MIT 6.5840 test harness. Every node's applied commands are recorded
// and cross-checked: if two nodes ever apply different commands at the same
// index, the test fails.
type cluster struct {
	t   *testing.T
	net *fakenet.Network
	n   int

	mu         sync.Mutex
	rafts      []*raft.Raft
	persisters []*raft.MemoryPersister
	applied    []map[uint64]string // node -> index -> command
	lastIdx    []uint64            // last applied index per node
	connected  []bool
	applyErr   []string
	maxIndex   uint64

	snapshotEvery int // >0: each node snapshots every N applied entries
	start         time.Time
}

func newCluster(t *testing.T, n int, reliable bool, snapshotEvery int) *cluster {
	t.Helper()
	c := &cluster{
		t:             t,
		net:           fakenet.New(0),
		n:             n,
		rafts:         make([]*raft.Raft, n),
		persisters:    make([]*raft.MemoryPersister, n),
		applied:       make([]map[uint64]string, n),
		lastIdx:       make([]uint64, n),
		connected:     make([]bool, n),
		applyErr:      make([]string, n),
		snapshotEvery: snapshotEvery,
		start:         time.Now(),
	}
	c.net.SetReliable(reliable)
	for i := range n {
		c.persisters[i] = raft.NewMemoryPersister()
		c.startNode(i, c.members())
		c.connect(i)
	}
	t.Cleanup(c.cleanup)
	return c
}

func (c *cluster) members() []raft.Member {
	var ms []raft.Member
	for i := range c.n {
		ms = append(ms, raft.Member{ID: raft.NodeID(i)})
	}
	return ms
}

func testConfig(id int, members []raft.Member) raft.Config {
	return raft.Config{
		ID:                 raft.NodeID(id),
		Members:            members,
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
	}
}

// startNode starts (or restarts) node i from its persister.
func (c *cluster) startNode(i int, members []raft.Member) {
	c.crash(i)
	c.mu.Lock()
	c.persisters[i] = c.persisters[i].Copy()
	p := c.persisters[i]
	c.applied[i] = map[uint64]string{}
	c.lastIdx[i] = 0
	c.mu.Unlock()

	applyCh := make(chan raft.ApplyMsg)
	tr := &fakenet.RaftTransport{Net: c.net, From: fakenet.NodeName(raft.NodeID(i))}
	rf, err := raft.New(testConfig(i, members), tr, p, applyCh)
	if err != nil {
		c.t.Fatal(err)
	}
	srv := fakenet.NewServer()
	fakenet.RegisterRaft(srv, rf)

	c.mu.Lock()
	c.rafts[i] = rf
	c.mu.Unlock()
	go c.applier(i, rf, applyCh)
	rf.Run()
	c.net.AddServer(fakenet.NodeName(raft.NodeID(i)), srv)
	c.mu.Lock()
	if !c.connected[i] {
		c.net.Connect(fakenet.NodeName(raft.NodeID(i)), false)
	}
	c.mu.Unlock()
}

type snapshotData struct {
	Index    uint64
	Applied  map[uint64]string
	LastSeen uint64
}

func (c *cluster) applier(i int, rf *raft.Raft, ch chan raft.ApplyMsg) {
	for m := range ch {
		c.mu.Lock()
		if c.rafts[i] != rf {
			c.mu.Unlock()
			return
		}
		switch {
		case m.SnapshotValid:
			var sd snapshotData
			if err := gob.NewDecoder(bytes.NewReader(m.Snapshot)).Decode(&sd); err != nil {
				c.applyErr[i] = fmt.Sprintf("decode snapshot: %v", err)
				break
			}
			if m.SnapshotIndex <= c.lastIdx[i] {
				break // stale
			}
			c.applied[i] = sd.Applied
			c.lastIdx[i] = m.SnapshotIndex
		case m.CommandValid:
			if m.CommandIndex != c.lastIdx[i]+1 {
				c.applyErr[i] = fmt.Sprintf("node %d applied index %d out of order (last %d)", i, m.CommandIndex, c.lastIdx[i])
				break
			}
			c.lastIdx[i] = m.CommandIndex
			if m.Kind == raft.EntryNormal {
				cmd := string(m.Command)
				for j := range c.n {
					if old, ok := c.applied[j][m.CommandIndex]; ok && old != cmd {
						c.applyErr[i] = fmt.Sprintf("commit index=%d node=%d %q != node=%d %q", m.CommandIndex, i, cmd, j, old)
					}
				}
				c.applied[i][m.CommandIndex] = cmd
				c.maxIndex = max(c.maxIndex, m.CommandIndex)
			}
			if c.snapshotEvery > 0 && m.CommandIndex%uint64(c.snapshotEvery) == 0 {
				var buf bytes.Buffer
				cp := make(map[uint64]string, len(c.applied[i]))
				for k, v := range c.applied[i] {
					cp[k] = v
				}
				_ = gob.NewEncoder(&buf).Encode(snapshotData{Index: m.CommandIndex, Applied: cp})
				c.mu.Unlock()
				rf.Snapshot(m.CommandIndex, buf.Bytes())
				continue
			}
		}
		if e := c.applyErr[i]; e != "" {
			c.mu.Unlock()
			c.t.Errorf("apply error: %s", e)
			return
		}
		c.mu.Unlock()
	}
}

func (c *cluster) crash(i int) {
	c.net.RemoveServer(fakenet.NodeName(raft.NodeID(i)))
	c.mu.Lock()
	rf := c.rafts[i]
	c.rafts[i] = nil
	if c.persisters[i] != nil {
		c.persisters[i] = c.persisters[i].Copy()
	}
	c.mu.Unlock()
	if rf != nil {
		rf.Kill()
	}
}

func (c *cluster) connect(i int) {
	c.mu.Lock()
	c.connected[i] = true
	c.mu.Unlock()
	c.net.Connect(fakenet.NodeName(raft.NodeID(i)), true)
}

func (c *cluster) disconnect(i int) {
	c.mu.Lock()
	c.connected[i] = false
	c.mu.Unlock()
	c.net.Connect(fakenet.NodeName(raft.NodeID(i)), false)
}

func (c *cluster) raft(i int) *raft.Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

func (c *cluster) cleanup() {
	for i := range c.n {
		c.crash(i)
	}
}

// checkOneLeader waits for exactly one leader among connected nodes in the
// newest term and returns it.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for iter := 0; iter < 20; iter++ {
		time.Sleep(time.Duration(200+rand.Intn(100)) * time.Millisecond)
		leaders := map[uint64][]int{}
		for i := range c.n {
			c.mu.Lock()
			conn, rf := c.connected[i], c.rafts[i]
			c.mu.Unlock()
			if conn && rf != nil {
				if term, isLeader := rf.State(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		var last uint64
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			last = max(last, term)
		}
		if len(leaders) > 0 {
			return leaders[last][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i := range c.n {
		c.mu.Lock()
		conn, rf := c.connected[i], c.rafts[i]
		c.mu.Unlock()
		if conn && rf != nil {
			if _, isLeader := rf.State(); isLeader {
				c.t.Fatalf("expected no leader among connected nodes, but %d claims to be leader", i)
			}
		}
	}
}

// checkTerms checks that all connected nodes agree on the term.
func (c *cluster) checkTerms() uint64 {
	c.t.Helper()
	var term uint64
	for i := range c.n {
		c.mu.Lock()
		conn, rf := c.connected[i], c.rafts[i]
		c.mu.Unlock()
		if conn && rf != nil {
			t, _ := rf.State()
			if term == 0 {
				term = t
			} else if t != term {
				c.t.Fatalf("nodes disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// nCommitted returns how many nodes applied a command at index.
func (c *cluster) nCommitted(index uint64) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count, cmd := 0, ""
	for i := range c.n {
		if e := c.applyErr[i]; e != "" {
			c.t.Fatal(e)
		}
		if v, ok := c.applied[i][index]; ok {
			if count > 0 && v != cmd {
				c.t.Fatalf("committed values do not match at index %d: %q vs %q", index, cmd, v)
			}
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one submits cmd and waits for it to be committed on at least expected
// nodes. With retry, it re-submits if a leader fails before committing.
func (c *cluster) one(cmd string, expected int, retry bool) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	starts := 0
	for time.Now().Before(deadline) {
		index := uint64(0)
		for range c.n {
			starts = (starts + 1) % c.n
			c.mu.Lock()
			conn, rf := c.connected[starts], c.rafts[starts]
			c.mu.Unlock()
			if conn && rf != nil {
				if idx, _, ok := rf.Start([]byte(cmd)); ok {
					index = idx
					break
				}
			}
		}
		if index != 0 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				if n, v := c.nCommitted(index); n > 0 && n >= expected && v == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%q) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%q) failed to reach agreement", cmd)
	return 0
}

// wait waits for index to be committed on n nodes, giving up if the term
// moves past startTerm (the entry may then never commit).
func (c *cluster) wait(index uint64, n int, startTerm uint64) string {
	c.t.Helper()
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if nd, _ := c.nCommitted(index); nd >= n {
			break
		}
		time.Sleep(to)
		to = min(2*to, time.Second)
		if startTerm != 0 {
			for i := range c.n {
				rf := c.raft(i)
				if rf == nil {
					continue
				}
				if t, _ := rf.State(); t > startTerm {
					return ""
				}
			}
		}
	}
	nd, cmd := c.nCommitted(index)
	if nd < n {
		c.t.Fatalf("only %d nodes committed index %d, wanted %d", nd, index, n)
	}
	return cmd
}
