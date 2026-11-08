# Implementation plan

Bottom-up milestones; each leaves a working, tested system. Follow Figure 2 of
the Raft paper literally. Run everything with `go test -race`.

| Milestone | Scope |
|---|---|
| M0 | Scaffold, CI, interfaces (Transport, Persister, Clock, StateMachine) |
| M1 | Leader election: roles, terms, RequestVote, randomized timeouts, heartbeats |
| M2 | Log replication: AppendEntries, conflict repair, commitIndex, apply loop, Figure 8 rule |
| M3 | Persistence: fsync term, votedFor, log before replying to RPCs |
| M4 | KV service: client retries, NotLeader hints, dedup via clientID+seq, linearizable reads (ReadIndex) |
| M5 | Fault-injection harness (drop/delay/duplicate/reorder/partition), Porcupine linearizability checks |
| M6 | Snapshots, log compaction, InstallSnapshot |
| M7 | gRPC transport, CLI client, multi-process cluster |
| M8 | LSM tree: WAL, memtable, SSTable, bloom filter, manifest, compaction |
| M9 | Stretch: metrics, docker compose demo, membership changes, sharding |

## Layout

- `cmd/kvserver`, `cmd/kvcli`: binaries
- `internal/raft`: consensus
- `internal/kv`: state machine, store interface, service
- `internal/lsm`: storage engine
- `internal/transport`: gRPC and in-memory fake network
- `test`: cluster harness and scenarios
- `proto`, `deploy`, `docs`
