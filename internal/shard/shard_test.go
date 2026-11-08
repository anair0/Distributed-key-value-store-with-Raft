package shard

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/kv"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/linearizability"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/transport/fakenet"
)

func TestRingBalance(t *testing.T) {
	groups := []int{1, 2, 3, 4, 5}
	r := NewRing(groups, 200)
	counts := map[int]int{}
	const n = 100000
	for i := range n {
		counts[r.Lookup(fmt.Sprintf("key-%d", i))]++
	}
	for _, g := range groups {
		share := float64(counts[g]) / n
		if share < 0.12 || share > 0.28 { // ideal 0.20
			t.Fatalf("group %d owns %.1f%% of keys: %v", g, 100*share, counts)
		}
	}
}

func TestRingMinimalMovement(t *testing.T) {
	before := NewRing([]int{1, 2, 3, 4}, 200)
	after := NewRing([]int{1, 2, 3, 4, 5}, 200)
	moved := 0
	const n = 50000
	for i := range n {
		k := fmt.Sprintf("key-%d", i)
		a, b := before.Lookup(k), after.Lookup(k)
		if a != b {
			moved++
			if b != 5 {
				t.Fatalf("key %s moved between old groups %d -> %d", k, a, b)
			}
		}
	}
	// Ideal: 1/5 of keys move to the new group.
	if f := float64(moved) / n; f < 0.12 || f > 0.28 {
		t.Fatalf("%.1f%% of keys moved, want about 20%%", 100*f)
	}
}

type kvConn struct {
	net      *fakenet.Network
	from, to string
}

func (c *kvConn) Do(ctx context.Context, req *kv.Request) (*kv.Response, error) {
	resp := &kv.Response{}
	return resp, c.net.Call(ctx, c.from, c.to, "KV.Do", req, resp)
}

// TestShardedCluster runs two 3-node Raft groups on one network, kills a
// leader in each group while clients write, and checks linearizability.
func TestShardedCluster(t *testing.T) {
	net := fakenet.New(0)
	groups := []int{1, 2}
	var servers []*kv.Server
	defer func() {
		for _, s := range servers {
			if s != nil {
				s.Kill()
			}
		}
	}()
	members := []raft.Member{{ID: 0}, {ID: 1}, {ID: 2}}
	prefix := func(g int) string { return fmt.Sprintf("g%d-", g) }
	byGroup := map[int][]*kv.Server{}
	for _, g := range groups {
		for i := range 3 {
			name := prefix(g) + fakenet.NodeName(raft.NodeID(i))
			s, err := kv.NewServer(raft.Config{ID: raft.NodeID(i), Members: members},
				kv.ServerConfig{MaxRaftState: 4000},
				&fakenet.RaftTransport{Net: net, From: name, Prefix: prefix(g)},
				raft.NewMemoryPersister(), kv.NewMemStore())
			if err != nil {
				t.Fatal(err)
			}
			srv := fakenet.NewServer()
			fakenet.RegisterRaft(srv, s.Raft())
			fakenet.Handle(srv, "KV.Do", func(req *kv.Request) *kv.Response { return s.Do(context.Background(), req) })
			s.Start()
			net.AddServer(name, srv)
			servers = append(servers, s)
			byGroup[g] = append(byGroup[g], s)
		}
	}

	newClient := func(name string) *Client {
		cls := map[int]*kv.Client{}
		for _, g := range groups {
			var peers []kv.Peer
			for i := range 3 {
				peers = append(peers, kv.Peer{ID: raft.NodeID(i), Conn: &kvConn{net, name, prefix(g) + fakenet.NodeName(raft.NodeID(i))}})
			}
			cls[g] = kv.NewClient(peers)
		}
		return NewClient(cls, 100)
	}

	var rec linearizability.Recorder
	keys := []string{"apple", "banana", "cherry", "date", "elder", "fig"}
	var stop sync.WaitGroup
	done := make(chan struct{})
	for c := range 4 {
		stop.Add(1)
		go func() {
			defer stop.Done()
			cl := newClient(fmt.Sprintf("client-%d", c))
			for j := 0; ; j++ {
				select {
				case <-done:
					return
				default:
				}
				key := keys[rand.Intn(len(keys))]
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				start := linearizability.Now()
				if rand.Intn(2) == 0 {
					v := fmt.Sprintf("%d-%d", c, j)
					if err := cl.Put(ctx, key, []byte(v)); err != nil {
						t.Error(err)
						cancel()
						return
					}
					rec.Record(c, linearizability.Input{Op: linearizability.Put, Key: key, Value: v}, start, linearizability.Output{}, linearizability.Now())
				} else {
					v, ok, err := cl.Get(ctx, key)
					if err != nil {
						t.Error(err)
						cancel()
						return
					}
					rec.Record(c, linearizability.Input{Op: linearizability.Get, Key: key}, start, linearizability.Output{Value: string(v), Found: ok}, linearizability.Now())
				}
				cancel()
			}
		}()
	}

	// Disconnect the leader of each group in turn.
	time.Sleep(time.Second)
	for _, g := range groups {
		for i, s := range byGroup[g] {
			if _, ok := s.Raft().State(); ok {
				name := prefix(g) + fakenet.NodeName(raft.NodeID(i))
				net.Connect(name, false)
				time.Sleep(1500 * time.Millisecond)
				net.Connect(name, true)
				break
			}
		}
	}
	time.Sleep(time.Second)
	close(done)
	stop.Wait()

	// Both groups must have served traffic.
	cl := newClient("checker")
	seen := map[int]bool{}
	for _, k := range keys {
		seen[cl.Group(k)] = true
	}
	if len(seen) != 2 {
		t.Fatalf("test keys all map to one group: %v", seen)
	}
	if rec.Len() < 50 {
		t.Fatalf("only %d operations", rec.Len())
	}
	if err := rec.Check(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d operations across %d groups, linearizable", rec.Len(), len(groups))
}
