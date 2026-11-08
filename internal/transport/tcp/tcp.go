// Package tcp is the real network transport: Go's net/rpc (gob encoding)
// over persistent TCP connections. One listener per node serves three RPC
// services:
//
//	Raft.RequestVote / Raft.AppendEntries / Raft.InstallSnapshot   peer traffic
//	KV.Do                                                          client requests
//	Node.Status                                                    monitoring
//
// net/rpc keeps the project dependency-free; gRPC would add streaming,
// deadlines propagated to the server and cross-language clients, at the cost
// of protobuf code generation. The raft.Transport interface makes swapping
// one for the other a local change.
//
// For demos and fault-injection by hand, a node can be "isolated": it then
// refuses all incoming RPCs and fails all outgoing ones, which looks to the
// rest of the cluster exactly like a network partition.
package tcp

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/kv"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
)

// ErrIsolated is returned while a node is isolated.
var ErrIsolated = errors.New("tcp: node is isolated")

// ---- server side ----

// KVHandler serves client requests (*kv.Server implements it).
type KVHandler interface {
	Do(ctx context.Context, req *kv.Request) *kv.Response
}

// Server serves RPCs on a listener.
type Server struct {
	ln       net.Listener
	rpc      *rpc.Server
	isolated *atomic.Bool

	mu     sync.Mutex
	conns  map[net.Conn]bool
	closed bool
	wg     sync.WaitGroup
}

// RaftService exposes a raft.Handler over net/rpc.
type RaftService struct {
	h   raft.Handler
	iso *atomic.Bool
}

func (s *RaftService) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	if s.iso.Load() {
		return ErrIsolated
	}
	*reply = *s.h.HandleRequestVote(args)
	return nil
}

func (s *RaftService) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	if s.iso.Load() {
		return ErrIsolated
	}
	*reply = *s.h.HandleAppendEntries(args)
	return nil
}

func (s *RaftService) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	if s.iso.Load() {
		return ErrIsolated
	}
	*reply = *s.h.HandleInstallSnapshot(args)
	return nil
}

// KVService exposes a KVHandler over net/rpc.
type KVService struct {
	h   KVHandler
	iso *atomic.Bool
}

func (s *KVService) Do(req *kv.Request, resp *kv.Response) error {
	if s.iso.Load() {
		return ErrIsolated
	}
	*resp = *s.h.Do(context.Background(), req)
	return nil
}

// NodeService exposes node status (served even while isolated, so a
// dashboard can still show the node).
type NodeService struct {
	status func() raft.Status
}

func (s *NodeService) Status(_ *struct{}, reply *raft.Status) error {
	*reply = s.status()
	return nil
}

// Serve starts serving on ln. isolated may be shared with the Transport.
func Serve(ln net.Listener, rh raft.Handler, kh KVHandler, status func() raft.Status, isolated *atomic.Bool) (*Server, error) {
	srv := &Server{ln: ln, rpc: rpc.NewServer(), isolated: isolated, conns: map[net.Conn]bool{}}
	if err := srv.rpc.RegisterName("Raft", &RaftService{h: rh, iso: isolated}); err != nil {
		return nil, err
	}
	if err := srv.rpc.RegisterName("KV", &KVService{h: kh, iso: isolated}); err != nil {
		return nil, err
	}
	if err := srv.rpc.RegisterName("Node", &NodeService{status: status}); err != nil {
		return nil, err
	}
	srv.wg.Add(1)
	go srv.acceptLoop()
	return srv, nil
}

// Addr returns the listening address.
func (s *Server) Addr() string { return s.ln.Addr().String() }

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			c.Close()
			return
		}
		s.conns[c] = true
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.rpc.ServeConn(c)
			s.mu.Lock()
			delete(s.conns, c)
			s.mu.Unlock()
		}()
	}
}

// Close stops the listener and closes all connections.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	err := s.ln.Close()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

// ---- client side ----

// Pool caches one RPC connection per address.
type Pool struct {
	mu          sync.Mutex
	clients     map[string]*rpc.Client
	isolated    *atomic.Bool
	DialTimeout time.Duration
}

// NewPool creates a connection pool. isolated may be nil.
func NewPool(isolated *atomic.Bool) *Pool {
	if isolated == nil {
		isolated = new(atomic.Bool)
	}
	return &Pool{clients: map[string]*rpc.Client{}, isolated: isolated, DialTimeout: time.Second}
}

func (p *Pool) client(ctx context.Context, addr string) (*rpc.Client, error) {
	p.mu.Lock()
	c, ok := p.clients[addr]
	p.mu.Unlock()
	if ok {
		return c, nil
	}
	d := net.Dialer{Timeout: p.DialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c = rpc.NewClient(conn)
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.clients[addr]; ok {
		c.Close() // lost a race with another dialer
		return old, nil
	}
	p.clients[addr] = c
	return c, nil
}

func (p *Pool) drop(addr string, c *rpc.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.clients[addr] == c {
		delete(p.clients, addr)
	}
	c.Close()
}

// Call invokes method on addr, honouring ctx.
func (p *Pool) Call(ctx context.Context, addr, method string, args, reply any) error {
	if p.isolated.Load() {
		return ErrIsolated
	}
	if addr == "" {
		return errors.New("tcp: unknown address")
	}
	c, err := p.client(ctx, addr)
	if err != nil {
		return err
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		var se rpc.ServerError
		if call.Error != nil && !errors.As(call.Error, &se) {
			p.drop(addr, c) // connection-level failure: reconnect next time
		}
		return call.Error
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes all connections.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for a, c := range p.clients {
		c.Close()
		delete(p.clients, a)
	}
}

// Transport implements raft.Transport and raft.AddressBook over TCP.
type Transport struct {
	pool  *Pool
	mu    sync.RWMutex
	addrs map[raft.NodeID]string
}

// NewTransport creates a Raft transport.
func NewTransport(isolated *atomic.Bool) *Transport {
	return &Transport{pool: NewPool(isolated), addrs: map[raft.NodeID]string{}}
}

// SetMembers updates peer addresses (called by Raft on config changes).
func (t *Transport) SetMembers(members []raft.Member) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range members {
		if m.Addr != "" {
			t.addrs[m.ID] = m.Addr
		}
	}
}

func (t *Transport) addr(id raft.NodeID) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.addrs[id]
}

func (t *Transport) RequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	reply := &raft.RequestVoteReply{}
	return reply, t.pool.Call(ctx, t.addr(to), "Raft.RequestVote", args, reply)
}

func (t *Transport) AppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	reply := &raft.AppendEntriesReply{}
	return reply, t.pool.Call(ctx, t.addr(to), "Raft.AppendEntries", args, reply)
}

func (t *Transport) InstallSnapshot(ctx context.Context, to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	reply := &raft.InstallSnapshotReply{}
	return reply, t.pool.Call(ctx, t.addr(to), "Raft.InstallSnapshot", args, reply)
}

// Close closes connections.
func (t *Transport) Close() { t.pool.Close() }

// KVConn is a kv.Conn to one server.
type KVConn struct {
	Pool *Pool
	Addr string
}

func (c *KVConn) Do(ctx context.Context, req *kv.Request) (*kv.Response, error) {
	resp := &kv.Response{}
	return resp, c.Pool.Call(ctx, c.Addr, "KV.Do", req, resp)
}

// Status fetches a node's Raft status.
func Status(ctx context.Context, p *Pool, addr string) (raft.Status, error) {
	var st raft.Status
	err := p.Call(ctx, addr, "Node.Status", &struct{}{}, &st)
	return st, err
}

// NewClient creates a KV client for the given members.
func NewClient(p *Pool, members []raft.Member) *kv.Client {
	var peers []kv.Peer
	for _, m := range members {
		peers = append(peers, kv.Peer{ID: m.ID, Conn: &KVConn{Pool: p, Addr: m.Addr}})
	}
	c := kv.NewClient(peers)
	c.Dial = func(_ raft.NodeID, addr string) kv.Conn { return &KVConn{Pool: p, Addr: addr} }
	return c
}
