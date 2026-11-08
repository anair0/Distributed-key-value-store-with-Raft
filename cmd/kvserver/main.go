// Command kvserver runs one node of the replicated key-value store.
//
//	kvserver -id 1 -cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
//	         -http :8001 -data ./data/1
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/kv"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/node"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
)

func main() {
	id := flag.Int("id", 1, "this node's ID")
	cluster := flag.String("cluster", "", "cluster members: id=host:port,... (this node included)")
	join := flag.Bool("join", false, "start empty and wait to be added with 'kvcli members add'")
	listen := flag.String("listen", "", "RPC listen address (default: this node's address in -cluster)")
	httpAddr := flag.String("http", "", "HTTP address for /status, /metrics, /admin and /kv (empty disables)")
	dataDir := flag.String("data", "", "data directory (default ./data/<id>)")
	storage := flag.String("storage", "lsm", "storage engine: lsm or mem")
	readMode := flag.String("read", "readindex", "read path: readindex or log")
	snapshot := flag.Int("snapshot-bytes", 1<<20, "snapshot when the Raft log exceeds this many bytes (-1 disables)")
	heartbeat := flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
	election := flag.Duration("election-timeout", 300*time.Millisecond, "minimum election timeout (max is 2x)")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	members, err := node.ParseMembers(*cluster)
	if err != nil || len(members) == 0 {
		fatal(logger, "need -cluster id=host:port,...", err)
	}
	self := raft.Member{ID: raft.NodeID(*id)}
	for _, m := range members {
		if m.ID == self.ID {
			self = m
		}
	}
	if *listen == "" {
		*listen = self.Addr
	}
	if *listen == "" {
		fatal(logger, fmt.Sprintf("node %d is not in -cluster; pass -listen", *id), nil)
	}
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/%d", *id)
	}
	rm := kv.ReadIndex
	if *readMode == "log" {
		rm = kv.ReadLog
	}

	n, err := node.Start(node.Config{
		ID:                 self.ID,
		Members:            members,
		Join:               *join,
		Listen:             *listen,
		HTTP:               *httpAddr,
		DataDir:            *dataDir,
		Storage:            *storage,
		ReadMode:           rm,
		SnapshotBytes:      *snapshot,
		HeartbeatInterval:  *heartbeat,
		ElectionTimeoutMin: *election,
		ElectionTimeoutMax: 2 * *election,
		Logger:             logger,
		Exit:               func() { logger.Warn("crash requested via /admin/crash"); os.Exit(1) },
	})
	if err != nil {
		fatal(logger, "start", err)
	}
	logger.Info("serving", "rpc", n.Addr(), "http", n.HTTPAddr(), "data", *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")
	n.Stop()
}

func fatal(l *slog.Logger, msg string, err error) {
	l.Error(msg, "err", err)
	os.Exit(2)
}
