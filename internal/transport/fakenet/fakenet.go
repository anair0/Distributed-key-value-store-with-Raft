// Package fakenet is an in-memory network for tests. It can disconnect
// nodes, partition them, and (in unreliable mode) drop, delay, duplicate and
// reorder messages. Every message is gob-encoded and decoded, so nodes never
// share memory, just like over a real network.
//
// Endpoints are named by strings ("node-1", "client-3"). A server registers
// named methods; a caller invokes them with Call.
package fakenet

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ErrUnreachable is returned when a message is dropped or the target is down.
var ErrUnreachable = errors.New("fakenet: unreachable")

// HandlerFunc handles one encoded request and returns an encoded reply.
type HandlerFunc func(req []byte) ([]byte, error)

// Server is a set of methods registered under one endpoint name.
type Server struct {
	mu      sync.Mutex
	methods map[string]HandlerFunc
}

// NewServer creates an empty server.
func NewServer() *Server { return &Server{methods: map[string]HandlerFunc{}} }

// Handle registers a typed method on s.
func Handle[A, R any](s *Server, method string, fn func(*A) *R) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.methods[method] = func(req []byte) ([]byte, error) {
		args := new(A)
		if err := gob.NewDecoder(bytes.NewReader(req)).Decode(args); err != nil {
			return nil, err
		}
		return encode(fn(args))
	}
}

func (s *Server) lookup(method string) (HandlerFunc, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.methods[method]
	return h, ok
}

// Network connects endpoints.
type Network struct {
	mu        sync.Mutex
	servers   map[string]*Server
	connected map[string]bool
	group     map[string]int // partition group; nodes in different groups cannot talk
	reliable  bool
	reorder   bool
	rng       *rand.Rand

	calls atomic.Int64
	bytes atomic.Int64
}

// New creates a reliable network.
func New(seed int64) *Network {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Network{
		servers:   map[string]*Server{},
		connected: map[string]bool{},
		group:     map[string]int{},
		reliable:  true,
		rng:       rand.New(rand.NewSource(seed)),
	}
}

// AddServer registers (or replaces) the server behind an endpoint and
// connects the endpoint.
func (n *Network) AddServer(name string, s *Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.servers[name] = s
	n.connected[name] = true
}

// RemoveServer simulates a crash: calls to the endpoint fail.
func (n *Network) RemoveServer(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.servers, name)
}

// Connect connects or disconnects an endpoint (both directions). Clients
// are connected implicitly unless disconnected.
func (n *Network) Connect(name string, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[name] = on
}

// Partition places each listed group of endpoints in its own partition.
// Endpoints not listed are in partition 0.
func (n *Network) Partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[string]int{}
	for i, g := range groups {
		for _, name := range g {
			n.group[name] = i + 1
		}
	}
}

// Heal removes all partitions.
func (n *Network) Heal() { n.Partition() }

// SetReliable toggles message loss and delay.
func (n *Network) SetReliable(on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = on
}

// SetLongReordering occasionally delays replies by a long time, so they
// arrive after newer messages.
func (n *Network) SetLongReordering(on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reorder = on
}

// Calls returns the total number of calls made, for RPC-count tests.
func (n *Network) Calls() int64 { return n.calls.Load() }

// Bytes returns the total request bytes sent.
func (n *Network) Bytes() int64 { return n.bytes.Load() }

func (n *Network) linkUpLocked(from, to string) bool {
	c1, ok1 := n.connected[from]
	c2, ok2 := n.connected[to]
	if (ok1 && !c1) || (ok2 && !c2) {
		return false
	}
	return n.group[from] == n.group[to]
}

func (n *Network) randIntn(k int) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rng.Intn(k)
}

// Call invokes method on endpoint to, on behalf of endpoint from.
func (n *Network) Call(ctx context.Context, from, to, method string, args, reply any) error {
	n.calls.Add(1)
	req, err := encode(args)
	if err != nil {
		return err
	}
	n.bytes.Add(int64(len(req)))

	n.mu.Lock()
	srv, ok := n.servers[to]
	up := ok && n.linkUpLocked(from, to)
	reliable, reorder := n.reliable, n.reorder
	n.mu.Unlock()

	if !up {
		// Simulate a connection timeout, bounded by the caller's deadline.
		return sleepCtx(ctx, time.Duration(n.randIntn(10))*time.Millisecond, ErrUnreachable)
	}
	if !reliable {
		if err := sleepCtx(ctx, time.Duration(n.randIntn(27))*time.Millisecond, nil); err != nil {
			return err
		}
		if n.randIntn(10) == 0 {
			return sleepCtx(ctx, time.Duration(n.randIntn(10))*time.Millisecond, ErrUnreachable) // request lost
		}
	}
	h, ok := srv.lookup(method)
	if !ok {
		return fmt.Errorf("fakenet: %s has no method %q", to, method)
	}

	type result struct {
		b   []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		b, err := h(req)
		done <- result{b, err}
	}()
	if !reliable && n.randIntn(20) == 0 {
		go h(req) // duplicate delivery
	}
	var res result
	select {
	case res = <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if res.err != nil {
		return res.err
	}

	// The server may have crashed or been cut off while handling the call.
	n.mu.Lock()
	cur, ok := n.servers[to]
	up = ok && cur == srv && n.linkUpLocked(from, to)
	n.mu.Unlock()
	if !up {
		return ErrUnreachable
	}
	if !reliable && n.randIntn(10) == 0 {
		return ErrUnreachable // reply lost (the request was executed!)
	}
	if reorder && n.randIntn(900) < 600 && n.randIntn(10) == 0 {
		if err := sleepCtx(ctx, time.Duration(200+n.randIntn(1+n.randIntn(2000)))*time.Millisecond, nil); err != nil {
			return err
		}
	}
	return gob.NewDecoder(bytes.NewReader(res.b)).Decode(reply)
}

func sleepCtx(ctx context.Context, d time.Duration, ret error) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return ret
	case <-ctx.Done():
		return ctx.Err()
	}
}

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
