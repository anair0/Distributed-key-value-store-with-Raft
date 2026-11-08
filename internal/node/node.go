// Package node wires one complete server together: Raft + KV state machine
// + storage engine + TCP transport + HTTP (status, metrics, admin, KV API).
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/kv"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/lsm"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/metrics"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/transport/tcp"
)

// Config configures a node.
type Config struct {
	ID raft.NodeID
	// Members is the initial cluster (ignored once state is on disk).
	Members []raft.Member
	// Join starts the node with no configuration, waiting to be added to an
	// existing cluster with an AddMember request.
	Join bool

	// Listener for Raft and client RPCs. If nil, Listen is used.
	Listener net.Listener
	Listen   string
	// HTTPListener / HTTP serve status, metrics, admin and a JSON KV API.
	// Both empty disables HTTP.
	HTTPListener net.Listener
	HTTP         string

	DataDir       string
	Storage       string // "lsm" (default) or "mem"
	ReadMode      kv.ReadMode
	SnapshotBytes int // snapshot when Raft state exceeds this; 0 = 1 MiB, <0 = never

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	Logger *slog.Logger
	// Exit is called by POST /admin/crash. Defaults to doing nothing.
	Exit func()
}

// Node is a running server.
type Node struct {
	cfg       Config
	isolated  *atomic.Bool
	persister *raft.FilePersister
	store     kv.Store
	kv        *kv.Server
	transport *tcp.Transport
	rpc       *tcp.Server
	http      *http.Server
	httpLn    net.Listener
	client    *kv.Client
	pool      *tcp.Pool
}

// Start starts a node.
func Start(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.SnapshotBytes == 0 {
		cfg.SnapshotBytes = 1 << 20
	}
	n := &Node{cfg: cfg, isolated: new(atomic.Bool)}
	var err error
	if n.persister, err = raft.OpenFilePersister(filepath.Join(cfg.DataDir, "raft")); err != nil {
		return nil, err
	}
	switch cfg.Storage {
	case "", "lsm":
		n.store, err = kv.OpenLSMStore(filepath.Join(cfg.DataDir, "kv"), lsm.Options{})
	case "mem":
		n.store = kv.NewMemStore()
	default:
		err = fmt.Errorf("unknown storage %q", cfg.Storage)
	}
	if err != nil {
		n.persister.Close()
		return nil, err
	}

	members := cfg.Members
	if cfg.Join {
		members = nil
	}
	n.transport = tcp.NewTransport(n.isolated)
	n.transport.SetMembers(cfg.Members)
	reg := prometheus.NewRegistry()
	var m *metrics.Metrics
	rcfg := raft.Config{
		ID:                 cfg.ID,
		Members:            members,
		HeartbeatInterval:  cfg.HeartbeatInterval,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		Logger:             cfg.Logger,
	}
	kcfg := kv.ServerConfig{
		MaxRaftState: max(cfg.SnapshotBytes, 0),
		ReadMode:     cfg.ReadMode,
		OnRequest:    func(op kv.OpType, d time.Duration, e kv.Err) { m.Observe(op, d, e) },
	}
	if n.kv, err = kv.NewServer(rcfg, kcfg, n.transport, n.persister, n.store); err != nil {
		n.closeStorage()
		return nil, err
	}
	m = metrics.Register(reg, n.kv.Raft().Status)
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "node_isolated", Help: "1 if the node is isolated."}, func() float64 {
		if n.isolated.Load() {
			return 1
		}
		return 0
	}))

	ln := cfg.Listener
	if ln == nil {
		if ln, err = net.Listen("tcp", cfg.Listen); err != nil {
			n.closeStorage()
			return nil, err
		}
	}
	n.kv.Start()
	if n.rpc, err = tcp.Serve(ln, n.kv.Raft(), n.kv, n.kv.Raft().Status, n.isolated); err != nil {
		n.Stop()
		return nil, err
	}

	n.pool = tcp.NewPool(n.isolated) // an isolated node's HTTP API is cut off too
	n.client = tcp.NewClient(n.pool, cfg.Members)

	if cfg.HTTPListener != nil || cfg.HTTP != "" {
		hl := cfg.HTTPListener
		if hl == nil {
			if hl, err = net.Listen("tcp", cfg.HTTP); err != nil {
				n.Stop()
				return nil, err
			}
		}
		n.httpLn = hl
		n.http = &http.Server{Handler: n.routes(reg)}
		go n.http.Serve(hl)
	}
	cfg.Logger.Info("node started", "id", cfg.ID, "rpc", n.rpc.Addr())
	return n, nil
}

// Addr returns the RPC address.
func (n *Node) Addr() string { return n.rpc.Addr() }

// HTTPAddr returns the HTTP address, if any.
func (n *Node) HTTPAddr() string {
	if n.httpLn == nil {
		return ""
	}
	return n.httpLn.Addr().String()
}

// KV returns the KV server.
func (n *Node) KV() *kv.Server { return n.kv }

// SetIsolated cuts the node off from the network (or reconnects it).
func (n *Node) SetIsolated(on bool) { n.isolated.Store(on) }

// Stop shuts the node down.
func (n *Node) Stop() {
	if n.http != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		n.http.Shutdown(ctx)
		cancel()
	}
	if n.rpc != nil {
		n.rpc.Close()
	}
	n.kv.Kill()
	n.transport.Close()
	if n.pool != nil {
		n.pool.Close()
	}
	n.closeStorage()
}

func (n *Node) closeStorage() {
	if n.store != nil {
		n.store.Close()
	}
	n.persister.Close()
}

// StatusJSON is the /status payload.
type StatusJSON struct {
	raft.Status
	RoleName string
	Isolated bool
	Storage  string
}

func (n *Node) status() StatusJSON {
	st := n.kv.Raft().Status()
	s := StatusJSON{Status: st, RoleName: st.Role.String(), Isolated: n.isolated.Load(), Storage: n.cfg.Storage}
	if s.Storage == "" {
		s.Storage = "lsm"
	}
	return s
}

func (n *Node) routes(reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, n.status())
	})
	mux.HandleFunc("POST /admin/isolate", func(w http.ResponseWriter, r *http.Request) {
		on := r.URL.Query().Get("on") != "0"
		n.SetIsolated(on)
		writeJSON(w, n.status())
	})
	mux.HandleFunc("POST /admin/crash", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		if n.cfg.Exit != nil {
			go func() {
				time.Sleep(100 * time.Millisecond)
				n.cfg.Exit()
			}()
		}
	})
	mux.HandleFunc("POST /admin/members", func(w http.ResponseWriter, r *http.Request) {
		var m raft.Member
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := n.client.AddMember(ctx, m); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, n.status())
	})
	mux.HandleFunc("DELETE /admin/members/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := n.client.RemoveMember(ctx, raft.NodeID(id)); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, n.status())
	})
	// JSON KV API, served through a cluster client (works on any node).
	mux.HandleFunc("/kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		var err error
		switch r.Method {
		case http.MethodGet:
			var v []byte
			var ok bool
			if v, ok, err = n.client.Get(ctx, key); err == nil {
				if !ok {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				w.Write(v)
				return
			}
		case http.MethodPut, http.MethodPost:
			var body []byte
			if body, err = io.ReadAll(io.LimitReader(r.Body, 1<<20)); err == nil {
				err = n.client.Put(ctx, key, body)
			}
		case http.MethodDelete:
			_, err = n.client.Delete(ctx, key)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		switch {
		case errors.Is(err, kv.ErrBadKey):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case err != nil:
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// ParseMembers parses "1=host:port,2=host:port".
func ParseMembers(s string) ([]raft.Member, error) {
	var ms []raft.Member
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad member %q, want id=host:port", part)
		}
		n, err := strconv.Atoi(id)
		if err != nil {
			return nil, fmt.Errorf("bad member id %q", id)
		}
		ms = append(ms, raft.Member{ID: raft.NodeID(n), Addr: addr})
	}
	return ms, nil
}
