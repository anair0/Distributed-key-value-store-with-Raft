package kv_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/kv"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/linearizability"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/transport/fakenet"
)

// fakeConn is a kv.Conn over the fake network.
type fakeConn struct {
	net      *fakenet.Network
	from, to string
}

func (c *fakeConn) Do(ctx context.Context, req *kv.Request) (*kv.Response, error) {
	resp := &kv.Response{}
	return resp, c.net.Call(ctx, c.from, c.to, "KV.Do", req, resp)
}

type kvCluster struct {
	t   *testing.T
	net *fakenet.Network
	n   int
	cfg kv.ServerConfig

	mu         sync.Mutex
	servers    []*kv.Server
	persisters []*raft.MemoryPersister
	nclients   int
}

func newKVCluster(t *testing.T, n int, reliable bool, cfg kv.ServerConfig) *kvCluster {
	t.Helper()
	c := &kvCluster{
		t:          t,
		net:        fakenet.New(0),
		n:          n,
		cfg:        cfg,
		servers:    make([]*kv.Server, n),
		persisters: make([]*raft.MemoryPersister, n),
	}
	c.net.SetReliable(reliable)
	for i := range n {
		c.persisters[i] = raft.NewMemoryPersister()
		c.startServer(i)
	}
	t.Cleanup(func() {
		for i := range n {
			c.crash(i)
		}
	})
	return c
}

func (c *kvCluster) members() []raft.Member {
	var ms []raft.Member
	for i := range c.n {
		ms = append(ms, raft.Member{ID: raft.NodeID(i)})
	}
	return ms
}

func (c *kvCluster) startServer(i int) {
	c.crash(i)
	c.mu.Lock()
	c.persisters[i] = c.persisters[i].Copy()
	p := c.persisters[i]
	c.mu.Unlock()

	name := fakenet.NodeName(raft.NodeID(i))
	rcfg := raft.Config{
		ID:                 raft.NodeID(i),
		Members:            c.members(),
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
	}
	s, err := kv.NewServer(rcfg, c.cfg, &fakenet.RaftTransport{Net: c.net, From: name}, p, kv.NewMemStore())
	if err != nil {
		c.t.Fatal(err)
	}
	srv := fakenet.NewServer()
	fakenet.RegisterRaft(srv, s.Raft())
	fakenet.Handle(srv, "KV.Do", func(req *kv.Request) *kv.Response {
		return s.Do(context.Background(), req)
	})
	s.Start()
	c.mu.Lock()
	c.servers[i] = s
	c.mu.Unlock()
	c.net.AddServer(name, srv)
}

func (c *kvCluster) crash(i int) {
	c.net.RemoveServer(fakenet.NodeName(raft.NodeID(i)))
	c.mu.Lock()
	s := c.servers[i]
	c.servers[i] = nil
	if c.persisters[i] != nil {
		c.persisters[i] = c.persisters[i].Copy()
	}
	c.mu.Unlock()
	if s != nil {
		s.Kill()
	}
}

func (c *kvCluster) server(i int) *kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i]
}

// client creates a client endpoint that can reach the given servers.
func (c *kvCluster) client(servers ...int) (*kv.Client, string) {
	c.mu.Lock()
	name := fmt.Sprintf("client-%d", c.nclients)
	c.nclients++
	c.mu.Unlock()
	if len(servers) == 0 {
		for i := range c.n {
			servers = append(servers, i)
		}
	}
	var peers []kv.Peer
	for _, i := range servers {
		peers = append(peers, kv.Peer{ID: raft.NodeID(i), Conn: &fakeConn{net: c.net, from: name, to: fakenet.NodeName(raft.NodeID(i))}})
	}
	cl := kv.NewClient(peers)
	cl.AttemptTimeout = time.Second
	return cl, name
}

// partition splits servers randomly into a majority and a minority. Each
// client lands on a random side.
func (c *kvCluster) partition(clients []string) {
	perm := rand.Perm(c.n)
	majority := c.n/2 + 1
	var a, b []string
	for i, p := range perm {
		if i < majority {
			a = append(a, fakenet.NodeName(raft.NodeID(p)))
		} else {
			b = append(b, fakenet.NodeName(raft.NodeID(p)))
		}
	}
	for _, cl := range clients {
		if rand.Intn(2) == 0 {
			a = append(a, cl)
		} else {
			b = append(b, cl)
		}
	}
	c.net.Partition(a, b)
}

func (c *kvCluster) leader() int {
	for range 50 {
		for i := range c.n {
			if s := c.server(i); s != nil {
				if _, ok := s.Raft().State(); ok {
					return i
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return -1
}

// ---- recorded operations ----

func opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 60*time.Second)
}

// The helpers below may run on client goroutines, so they report errors
// with t.Errorf and return false instead of calling t.Fatal.

func put(t *testing.T, cl *kv.Client, rec *linearizability.Recorder, id int, key, val string) bool {
	t.Helper()
	ctx, cancel := opCtx()
	defer cancel()
	start := linearizability.Now()
	if err := cl.Put(ctx, key, []byte(val)); err != nil {
		t.Errorf("put(%q): %v", key, err)
		return false
	}
	rec.Record(id, linearizability.Input{Op: linearizability.Put, Key: key, Value: val}, start, linearizability.Output{}, linearizability.Now())
	return true
}

func get(t *testing.T, cl *kv.Client, rec *linearizability.Recorder, id int, key string) (string, bool) {
	t.Helper()
	ctx, cancel := opCtx()
	defer cancel()
	start := linearizability.Now()
	v, ok, err := cl.Get(ctx, key)
	if err != nil {
		t.Errorf("get(%q): %v", key, err)
		return "", false
	}
	rec.Record(id, linearizability.Input{Op: linearizability.Get, Key: key}, start, linearizability.Output{Value: string(v), Found: ok}, linearizability.Now())
	return string(v), ok
}

func del(t *testing.T, cl *kv.Client, rec *linearizability.Recorder, id int, key string) bool {
	t.Helper()
	ctx, cancel := opCtx()
	defer cancel()
	start := linearizability.Now()
	ok, err := cl.Delete(ctx, key)
	if err != nil {
		t.Errorf("delete(%q): %v", key, err)
		return false
	}
	rec.Record(id, linearizability.Input{Op: linearizability.Delete, Key: key}, start, linearizability.Output{Found: ok}, linearizability.Now())
	return true
}

type scenario struct {
	nclients   int
	unreliable bool
	crash      bool
	partitions bool
	cfg        kv.ServerConfig
	iterations int
	duration   time.Duration
}

// runScenario is the generic test: clients hammer the cluster with random
// operations on a small shared key space while the harness injects faults.
// Afterwards every recorded history must be linearizable, and every
// acknowledged write must be visible to a final read.
func runScenario(t *testing.T, sc scenario) {
	if sc.iterations == 0 {
		sc.iterations = 3
	}
	if sc.duration == 0 {
		sc.duration = 4 * time.Second
	}
	c := newKVCluster(t, 5, !sc.unreliable, sc.cfg)
	var rec linearizability.Recorder
	keys := []string{"a", "b", "c", "d"}
	var totalOps atomic.Int64

	for iter := range sc.iterations {
		var stop atomic.Bool
		var wg sync.WaitGroup
		var names []string
		for ci := range sc.nclients {
			cl, name := c.client()
			names = append(names, name)
			id := iter*sc.nclients + ci
			wg.Add(1)
			go func() {
				defer wg.Done()
				j := 0
				for !stop.Load() && !t.Failed() {
					key := keys[rand.Intn(len(keys))]
					switch r := rand.Intn(10); {
					case r < 5:
						get(t, cl, &rec, id, key)
					case r < 9:
						put(t, cl, &rec, id, key, fmt.Sprintf("%d-%d", id, j))
					default:
						del(t, cl, &rec, id, key)
					}
					j++
					totalOps.Add(1)
				}
			}()
		}

		var pwg sync.WaitGroup
		if sc.partitions {
			pwg.Add(1)
			go func() {
				defer pwg.Done()
				for !stop.Load() {
					c.partition(names)
					time.Sleep(time.Duration(800+rand.Intn(400)) * time.Millisecond)
				}
			}()
		}

		time.Sleep(sc.duration)
		stop.Store(true)
		pwg.Wait()
		c.net.Heal()

		if sc.crash {
			// Crash and restart everyone while clients finish their last ops.
			for i := range c.n {
				c.crash(i)
			}
			time.Sleep(200 * time.Millisecond)
			for i := range c.n {
				c.startServer(i)
			}
		}
		wg.Wait()

		if sc.cfg.MaxRaftState > 0 {
			for i := range c.n {
				c.mu.Lock()
				sz := c.persisters[i].StateSize()
				c.mu.Unlock()
				if sz > 8*sc.cfg.MaxRaftState {
					t.Fatalf("server %d raft state is %d bytes, limit %d: not snapshotting?", i, sz, sc.cfg.MaxRaftState)
				}
			}
		}
	}

	if t.Failed() {
		t.FailNow()
	}
	if totalOps.Load() < 20 {
		t.Fatalf("only %d operations completed", totalOps.Load())
	}
	if err := rec.Check(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d operations, linearizable", totalOps.Load())
}
