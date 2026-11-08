package kv

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
)

// Keys starting with metaPrefix hold state machine metadata in the Store,
// written in the same atomic batch as the data they describe:
//
//	\x00applied          last applied log index
//	\x00session/<client> last sequence number and reply for that client
//
// Keeping them in the store means a durable store (the LSM tree) restarts
// exactly where it left off, and snapshots carry the dedup table for free.
const (
	metaPrefix  = "\x00"
	metaApplied = "\x00applied"
	metaSession = "\x00session/"
)

type session struct {
	Seq  uint64
	Resp Response
}

// ServerConfig configures a Server.
type ServerConfig struct {
	// MaxRaftState triggers a snapshot when the persisted Raft state grows
	// beyond this many bytes. 0 disables snapshots.
	MaxRaftState int
	// ReadMode selects how Gets are served.
	ReadMode ReadMode
	// RequestTimeout bounds how long a request waits to be applied.
	RequestTimeout time.Duration
	// OnRequest, if set, is called after every request (for metrics).
	OnRequest func(op OpType, d time.Duration, err Err)
}

type applyResult struct {
	term     uint64
	clientID string
	seq      uint64
	resp     Response
}

// Server is one replica of the KV service.
type Server struct {
	cfg       ServerConfig
	rf        *raft.Raft
	persister raft.Persister
	applyCh   chan raft.ApplyMsg

	mu          sync.Mutex
	store       Store
	lastApplied uint64
	sessions    map[string]session
	waiters     map[uint64][]chan applyResult
	appliedSig  chan struct{} // closed and replaced whenever lastApplied advances

	done chan struct{}
	wg   sync.WaitGroup
}

// NewServer creates a KV replica and the Raft node underneath it.
func NewServer(rcfg raft.Config, cfg ServerConfig, t raft.Transport, p raft.Persister, store Store) (*Server, error) {
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 2 * time.Second
	}
	s := &Server{
		cfg:        cfg,
		persister:  p,
		applyCh:    make(chan raft.ApplyMsg, 64),
		store:      store,
		sessions:   map[string]session{},
		waiters:    map[uint64][]chan applyResult{},
		appliedSig: make(chan struct{}),
		done:       make(chan struct{}),
	}
	if err := s.loadMetaLocked(); err != nil {
		return nil, err
	}
	rf, err := raft.New(rcfg, t, p, s.applyCh)
	if err != nil {
		return nil, err
	}
	s.rf = rf
	return s, nil
}

// Raft returns the underlying Raft node (for transports, status and tests).
func (s *Server) Raft() *raft.Raft { return s.rf }

// Start starts Raft and the apply loop.
func (s *Server) Start() {
	s.wg.Add(1)
	go s.applyLoop()
	s.rf.Run()
}

// Kill stops the server and its Raft node.
func (s *Server) Kill() {
	select {
	case <-s.done:
		return
	default:
	}
	close(s.done)
	s.rf.Kill()
	s.wg.Wait()
}

// LastApplied returns the index of the last applied entry.
func (s *Server) LastApplied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastApplied
}

// Do executes a client request. It is the RPC entry point.
func (s *Server) Do(ctx context.Context, req *Request) *Response {
	start := time.Now()
	var resp Response
	switch {
	case req.Op <= OpDelete && strings.HasPrefix(req.Key, metaPrefix):
		resp = Response{Err: ErrBadRequest}
	case req.Op == OpGet && s.cfg.ReadMode == ReadIndex:
		resp = s.read(ctx, req.Key)
	case req.Op == OpAddMember || req.Op == OpRemoveMember:
		resp = s.changeMembership(ctx, req)
	default:
		resp = s.submit(ctx, req)
	}
	if resp.Err == ErrWrongLeader {
		resp.LeaderID, resp.LeaderAddr = s.rf.Leader()
	}
	if s.cfg.OnRequest != nil {
		s.cfg.OnRequest(req.Op, time.Since(start), resp.Err)
	}
	return &resp
}

// submit replicates a request through the log and waits for it to apply.
func (s *Server) submit(ctx context.Context, req *Request) Response {
	cmd, err := json.Marshal(req)
	if err != nil {
		return Response{Err: ErrBadRequest}
	}
	// Holding s.mu across Start guarantees the waiter is registered before
	// the apply loop can deliver the entry (it needs s.mu to apply).
	s.mu.Lock()
	idx, term, isLeader := s.rf.Start(cmd)
	if !isLeader {
		s.mu.Unlock()
		return Response{Err: ErrWrongLeader}
	}
	ch := s.addWaiterLocked(idx)
	s.mu.Unlock()
	return s.await(ctx, idx, term, req.ClientID, req.Seq, ch)
}

func (s *Server) addWaiterLocked(idx uint64) chan applyResult {
	ch := make(chan applyResult, 1)
	s.waiters[idx] = append(s.waiters[idx], ch)
	return ch
}

func (s *Server) await(ctx context.Context, idx, term uint64, clientID string, seq uint64, ch chan applyResult) Response {
	timer := time.NewTimer(s.cfg.RequestTimeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		// A different entry at our index means we lost leadership and our
		// command was overwritten. The client will retry elsewhere.
		if res.term != term || res.clientID != clientID || res.seq != seq {
			return Response{Err: ErrWrongLeader}
		}
		return res.resp
	case <-timer.C:
	case <-ctx.Done():
	case <-s.done:
		return Response{Err: ErrShutdown}
	}
	s.mu.Lock()
	ws := s.waiters[idx]
	for i, w := range ws {
		if w == ch {
			s.waiters[idx] = append(ws[:i:i], ws[i+1:]...)
			break
		}
	}
	if len(s.waiters[idx]) == 0 {
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
	return Response{Err: ErrTimeout}
}

// read serves a linearizable Get using ReadIndex.
func (s *Server) read(ctx context.Context, key string) Response {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	ri, err := s.rf.ReadIndex(ctx)
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return Response{Err: ErrWrongLeader}
	case err != nil:
		return Response{Err: ErrTimeout}
	}
	for {
		s.mu.Lock()
		if s.lastApplied >= ri {
			v, ok, err := s.store.Get(key)
			s.mu.Unlock()
			if err != nil {
				return Response{Err: ErrRetry}
			}
			return Response{Value: v, Found: ok}
		}
		sig := s.appliedSig
		s.mu.Unlock()
		select {
		case <-sig:
		case <-ctx.Done():
			return Response{Err: ErrTimeout}
		case <-s.done:
			return Response{Err: ErrShutdown}
		}
	}
}

func (s *Server) changeMembership(ctx context.Context, req *Request) Response {
	s.mu.Lock()
	var idx, term uint64
	var err error
	if req.Op == OpAddMember {
		idx, term, err = s.rf.AddMember(req.Member)
	} else {
		idx, term, err = s.rf.RemoveMember(req.Member.ID)
	}
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		s.mu.Unlock()
		return Response{Err: ErrWrongLeader}
	case err != nil:
		s.mu.Unlock()
		return Response{Err: ErrRetry}
	}
	if s.lastApplied >= idx {
		s.mu.Unlock()
		return Response{} // no-op change that is already in effect
	}
	ch := s.addWaiterLocked(idx)
	s.mu.Unlock()
	return s.await(ctx, idx, term, "", 0, ch)
}

// ---- apply loop ----

func (s *Server) applyLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case m := <-s.applyCh:
			s.mu.Lock()
			if m.SnapshotValid {
				s.installSnapshotLocked(m)
			} else if m.CommandValid {
				s.applyEntryLocked(m)
				s.maybeSnapshotLocked()
			}
			s.mu.Unlock()
		}
	}
}

func (s *Server) applyEntryLocked(m raft.ApplyMsg) {
	if m.CommandIndex <= s.lastApplied {
		return // already applied (durable store restarted ahead of the log)
	}
	res := applyResult{term: m.CommandTerm}
	ops := []WriteOp{{Key: metaApplied, Value: binary.BigEndian.AppendUint64(nil, m.CommandIndex)}}

	if m.Kind == raft.EntryNormal {
		var req Request
		if err := json.Unmarshal(m.Command, &req); err != nil {
			panic(fmt.Sprintf("kv: undecodable command at %d: %v", m.CommandIndex, err))
		}
		res.clientID, res.seq = req.ClientID, req.Seq
		sess, seen := s.sessions[req.ClientID]
		switch {
		case req.Op != OpGet && seen && req.Seq <= sess.Seq:
			// Duplicate of a write we already applied: do not apply again,
			// return the original reply.
			res.resp = sess.Resp
		case req.Op == OpGet:
			v, ok, err := s.store.Get(req.Key)
			if err != nil {
				panic(fmt.Sprintf("kv: store get: %v", err))
			}
			res.resp = Response{Value: v, Found: ok}
		default:
			_, existed, err := s.store.Get(req.Key)
			if err != nil {
				panic(fmt.Sprintf("kv: store get: %v", err))
			}
			if req.Op == OpPut {
				ops = append(ops, WriteOp{Key: req.Key, Value: req.Value})
				res.resp = Response{Found: existed}
			} else {
				ops = append(ops, WriteOp{Key: req.Key, Delete: true})
				res.resp = Response{Found: existed}
			}
			sess = session{Seq: req.Seq, Resp: res.resp}
			s.sessions[req.ClientID] = sess
			enc, _ := json.Marshal(sess)
			ops = append(ops, WriteOp{Key: metaSession + req.ClientID, Value: enc})
		}
	}

	if err := s.store.Write(ops); err != nil {
		panic(fmt.Sprintf("kv: store write: %v", err))
	}
	s.setAppliedLocked(m.CommandIndex)

	for _, ch := range s.waiters[m.CommandIndex] {
		ch <- res
	}
	delete(s.waiters, m.CommandIndex)
}

func (s *Server) setAppliedLocked(idx uint64) {
	s.lastApplied = idx
	close(s.appliedSig)
	s.appliedSig = make(chan struct{})
}

// maybeSnapshotLocked hands Raft a snapshot once its log grows too large.
func (s *Server) maybeSnapshotLocked() {
	if s.cfg.MaxRaftState <= 0 || s.persister.StateSize() < s.cfg.MaxRaftState {
		return
	}
	data, err := s.encodeSnapshotLocked()
	if err != nil {
		panic(fmt.Sprintf("kv: snapshot: %v", err))
	}
	s.rf.Snapshot(s.lastApplied, data)
}

// encodeSnapshotLocked serialises every key (data and metadata) as
// [uvarint len][key][uvarint len][value] pairs.
func (s *Server) encodeSnapshotLocked() ([]byte, error) {
	var buf []byte
	err := s.store.Scan(func(k string, v []byte) bool {
		buf = binary.AppendUvarint(buf, uint64(len(k)))
		buf = append(buf, k...)
		buf = binary.AppendUvarint(buf, uint64(len(v)))
		buf = append(buf, v...)
		return true
	})
	return buf, err
}

func (s *Server) installSnapshotLocked(m raft.ApplyMsg) {
	if m.SnapshotIndex <= s.lastApplied {
		return
	}
	if err := s.store.Reset(); err != nil {
		panic(fmt.Sprintf("kv: reset store: %v", err))
	}
	b := m.Snapshot
	var batch []WriteOp
	for len(b) > 0 {
		kl, n := binary.Uvarint(b)
		b = b[n:]
		k := string(b[:kl])
		b = b[kl:]
		vl, n := binary.Uvarint(b)
		b = b[n:]
		v := b[:vl]
		b = b[vl:]
		batch = append(batch, WriteOp{Key: k, Value: v})
		if len(batch) >= 1024 {
			if err := s.store.Write(batch); err != nil {
				panic(err)
			}
			batch = nil
		}
	}
	batch = append(batch, WriteOp{Key: metaApplied, Value: binary.BigEndian.AppendUint64(nil, m.SnapshotIndex)})
	if err := s.store.Write(batch); err != nil {
		panic(err)
	}
	if err := s.loadMetaLocked(); err != nil {
		panic(err)
	}
	// Requests waiting on entries covered by the snapshot will never see
	// them individually; tell their clients to retry (dedup keeps it safe).
	for idx, ws := range s.waiters {
		if idx <= m.SnapshotIndex {
			for _, ch := range ws {
				ch <- applyResult{term: 0}
			}
			delete(s.waiters, idx)
		}
	}
}

// loadMetaLocked reloads lastApplied and the session table from the store.
func (s *Server) loadMetaLocked() error {
	s.sessions = map[string]session{}
	var applied uint64
	var decodeErr error
	err := s.store.Scan(func(k string, v []byte) bool {
		if !strings.HasPrefix(k, metaPrefix) {
			return false // metadata sorts first
		}
		switch {
		case k == metaApplied:
			applied = binary.BigEndian.Uint64(v)
		case strings.HasPrefix(k, metaSession):
			var sess session
			if err := json.Unmarshal(v, &sess); err != nil {
				decodeErr = err
				return false
			}
			s.sessions[strings.TrimPrefix(k, metaSession)] = sess
		}
		return true
	})
	if err != nil {
		return err
	}
	if decodeErr != nil {
		return decodeErr
	}
	s.lastApplied = applied
	close(s.appliedSig)
	s.appliedSig = make(chan struct{})
	return nil
}
