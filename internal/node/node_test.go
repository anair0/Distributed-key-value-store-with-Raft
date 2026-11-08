package node

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/transport/tcp"
)

// End-to-end over real TCP, with the FilePersister and the LSM store on disk.

type testCluster struct {
	t       *testing.T
	dir     string
	members []raft.Member
	nodes   map[raft.NodeID]*Node
}

func listen(t *testing.T) net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func (c *testCluster) start(id raft.NodeID, ln net.Listener, join bool) {
	c.t.Helper()
	if ln == nil {
		var err error
		for _, m := range c.members {
			if m.ID == id {
				// Rebind the node's address after a restart.
				for range 50 {
					if ln, err = net.Listen("tcp", m.Addr); err == nil {
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		}
		if ln == nil {
			c.t.Fatalf("cannot listen for node %d: %v", id, err)
		}
	}
	n, err := Start(Config{
		ID:                 id,
		Members:            c.members,
		Join:               join,
		Listener:           ln,
		DataDir:            filepath.Join(c.dir, fmt.Sprint(id)),
		SnapshotBytes:      16 << 10, // small, so snapshots happen
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = n
}

func (c *testCluster) stop(id raft.NodeID) {
	if n := c.nodes[id]; n != nil {
		n.Stop()
		delete(c.nodes, id)
	}
}

func (c *testCluster) leader() raft.NodeID {
	c.t.Helper()
	for range 100 {
		for id, n := range c.nodes {
			if _, ok := n.KV().Raft().State(); ok {
				return id
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return raft.None
}

func TestClusterOverTCP(t *testing.T) {
	c := &testCluster{t: t, dir: t.TempDir(), nodes: map[raft.NodeID]*Node{}}
	var lns []net.Listener
	for i := 1; i <= 3; i++ {
		ln := listen(t)
		lns = append(lns, ln)
		c.members = append(c.members, raft.Member{ID: raft.NodeID(i), Addr: ln.Addr().String()})
	}
	for i, ln := range lns {
		c.start(raft.NodeID(i+1), ln, false)
	}
	defer func() {
		for id := range c.nodes {
			c.stop(id)
		}
	}()

	pool := tcp.NewPool(nil)
	defer pool.Close()
	cl := tcp.NewClient(pool, c.members)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	put := func(k, v string) {
		t.Helper()
		if err := cl.Put(ctx, k, []byte(v)); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	check := func(k, want string) {
		t.Helper()
		v, ok, err := cl.Get(ctx, k)
		if err != nil || !ok || string(v) != want {
			t.Fatalf("get %s = %q,%v,%v want %q", k, v, ok, err, want)
		}
	}

	for i := range 300 {
		put(fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i))
	}

	// Kill the leader; the cluster keeps working.
	old := c.leader()
	c.stop(old)
	for i := range 100 {
		put(fmt.Sprintf("k%03d", i), fmt.Sprintf("w%d", i))
	}
	// Restart it from disk; it catches up.
	c.start(old, nil, false)
	check("k050", "w50")
	check("k250", "v250")

	// Isolating a node looks like a partition to the others.
	l := c.leader()
	c.nodes[l].SetIsolated(true)
	put("during-isolation", "yes")
	if c.leader() == l {
		time.Sleep(time.Second)
	}
	c.nodes[l].SetIsolated(false)

	// Restart every node: all data must come back from disk.
	for id := range c.nodes {
		c.stop(id)
	}
	for _, m := range c.members {
		c.start(m.ID, nil, false)
	}
	check("k000", "w0")
	check("k299", "v299")
	check("during-isolation", "yes")

	// Membership: add node 4, then remove node 1.
	ln4 := listen(t)
	c.members = append(c.members, raft.Member{ID: 4, Addr: ln4.Addr().String()})
	c.start(4, ln4, true)
	if err := cl.AddMember(ctx, raft.Member{ID: 4, Addr: ln4.Addr().String()}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for c.nodes[4].KV().LastApplied() < c.nodes[c.leader()].KV().LastApplied() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if c.nodes[4].KV().LastApplied() == 0 {
		t.Fatal("new node did not catch up")
	}
	if err := cl.RemoveMember(ctx, 1); err != nil {
		t.Fatal(err)
	}
	c.stop(1)
	put("after-membership", "ok")
	check("after-membership", "ok")
	st := c.nodes[c.leader()].KV().Raft().Status()
	if len(st.Members) != 3 {
		t.Fatalf("members = %v", st.Members)
	}
}
