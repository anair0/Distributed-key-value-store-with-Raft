package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/kv"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/linearizability"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/transport/fakenet"
)

func TestBasic(t *testing.T) {
	c := newKVCluster(t, 3, true, kv.ServerConfig{})
	cl, _ := c.client()
	var rec linearizability.Recorder
	if _, ok := get(t, cl, &rec, 0, "x"); ok {
		t.Fatal("empty store returned a value")
	}
	put(t, cl, &rec, 0, "x", "1")
	if v, _ := get(t, cl, &rec, 0, "x"); v != "1" {
		t.Fatalf("got %q, want 1", v)
	}
	put(t, cl, &rec, 0, "x", "2")
	del(t, cl, &rec, 0, "x")
	if _, ok := get(t, cl, &rec, 0, "x"); ok {
		t.Fatal("deleted key still present")
	}
	if err := cl.Put(context.Background(), "\x00applied", []byte("x")); err == nil {
		t.Fatal("reserved key accepted")
	}
}

// A client that only talks to a follower is redirected to the leader.
func TestLeaderRedirect(t *testing.T) {
	c := newKVCluster(t, 3, true, kv.ServerConfig{})
	l := c.leader()
	cl, _ := c.client((l+1)%3, l)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
}

// Retried requests (same client ID and sequence number) apply once.
func TestDuplicateRequestAppliedOnce(t *testing.T) {
	c := newKVCluster(t, 3, true, kv.ServerConfig{})
	l := c.leader()
	s := c.server(l)
	ctx := context.Background()

	first := &kv.Request{ClientID: "dup", Seq: 1, Op: kv.OpPut, Key: "k", Value: []byte("first")}
	if r := s.Do(ctx, first); r.Err != kv.OK {
		t.Fatal(r.Err)
	}
	// Another client overwrites the key.
	if r := s.Do(ctx, &kv.Request{ClientID: "other", Seq: 1, Op: kv.OpPut, Key: "k", Value: []byte("second")}); r.Err != kv.OK {
		t.Fatal(r.Err)
	}
	// A late retry of the first request must not resurrect "first".
	if r := s.Do(ctx, first); r.Err != kv.OK {
		t.Fatal(r.Err)
	}
	r := s.Do(ctx, &kv.Request{ClientID: "reader", Seq: 1, Op: kv.OpGet, Key: "k"})
	if string(r.Value) != "second" {
		t.Fatalf("duplicate was re-applied: got %q", r.Value)
	}
}

// A leader cut off from the majority must not serve reads (it could be
// stale) or acknowledge writes.
func TestIsolatedLeaderServesNothing(t *testing.T) {
	for _, mode := range []kv.ReadMode{kv.ReadIndex, kv.ReadLog} {
		c := newKVCluster(t, 5, true, kv.ServerConfig{ReadMode: mode, RequestTimeout: 500 * time.Millisecond})
		all, allName := c.client()
		ctx := context.Background()
		if err := all.Put(ctx, "k", []byte("old")); err != nil {
			t.Fatal(err)
		}
		l := c.leader()
		stale, staleName := c.client(l)

		majority := []string{allName}
		for i := range 5 {
			if i != l {
				majority = append(majority, fakenet.NodeName(raft.NodeID(i)))
			}
		}
		c.net.Partition(majority, []string{fakenet.NodeName(raft.NodeID(l)), staleName})
		if err := all.Put(ctx, "k", []byte("new")); err != nil {
			t.Fatal(err)
		}

		sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		v, _, err := stale.Get(sctx, "k")
		cancel()
		if err == nil {
			t.Fatalf("mode %d: isolated leader answered a read with %q", mode, v)
		}
		sctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		err = stale.Put(sctx, "k", []byte("lost"))
		cancel()
		if err == nil {
			t.Fatalf("mode %d: isolated leader acknowledged a write", mode)
		}
		c.net.Heal()
		if v, _, err := all.Get(ctx, "k"); err != nil || string(v) != "new" && string(v) != "lost" {
			t.Fatalf("after heal got %q, %v", v, err)
		}
	}
}

func TestConcurrent(t *testing.T) {
	runScenario(t, scenario{nclients: 5})
}

func TestUnreliable(t *testing.T) {
	runScenario(t, scenario{nclients: 5, unreliable: true})
}

func TestPartitions(t *testing.T) {
	runScenario(t, scenario{nclients: 5, partitions: true})
}

func TestCrashRestart(t *testing.T) {
	runScenario(t, scenario{nclients: 5, crash: true})
}

func TestUnreliablePartitionsCrash(t *testing.T) {
	runScenario(t, scenario{nclients: 5, unreliable: true, partitions: true, crash: true})
}

func TestSnapshots(t *testing.T) {
	runScenario(t, scenario{nclients: 5, crash: true, cfg: kv.ServerConfig{MaxRaftState: 2000}})
}

func TestSnapshotsUnreliablePartitionsCrash(t *testing.T) {
	runScenario(t, scenario{nclients: 5, unreliable: true, partitions: true, crash: true, cfg: kv.ServerConfig{MaxRaftState: 2000}})
}

func TestReadThroughLog(t *testing.T) {
	runScenario(t, scenario{nclients: 5, partitions: true, crash: true, cfg: kv.ServerConfig{ReadMode: kv.ReadLog, MaxRaftState: 2000}})
}
