# Distributed Key-Value Store with Raft

A fault-tolerant, replicated key-value store in Go, built from scratch:
Raft consensus, a replicated state machine with exactly-once semantics and
linearizable reads, an LSM-tree storage engine, a TCP transport, metrics, a
web dashboard and sharding. Correctness is tested with a fault-injecting
network and the Porcupine linearizability checker.

> **This branch is a complete reference implementation**, written so you can
> compare it with your own as you work through the milestones. See
> [docs/DESIGN.md](docs/DESIGN.md) for a guided tour of each part.

```
client ──► kvcli / kv.Client ──TCP──► kvserver
                                        │
                     ┌──────────────────┴──────────────────┐
                     │ KV service (internal/kv)            │  sessions (dedup), ReadIndex reads
                     │   ▲ committed entries   │ proposals │
                     │ Raft (internal/raft)    ▼           │◄──► other nodes (AppendEntries,
                     │                                     │      RequestVote, InstallSnapshot)
                     │ Storage: FilePersister (Raft log)   │
                     │          LSM tree (KV data)         │
                     └─────────────────────────────────────┘
```

## Features

| Area | What is implemented |
|---|---|
| Raft | Leader election, log replication, fast log backtracking, Figure 8 commit rule, persistence with fsync, snapshots and InstallSnapshot, Pre-Vote, leader stickiness, check-quorum, ReadIndex, single-server membership changes |
| KV service | Put / Get / Delete, leader redirection, client sessions for exactly-once writes, linearizable reads through ReadIndex or through the log |
| Storage | LSM tree: WAL, skip-list memtable, SSTables with block index and bloom filters, k-way merge compaction, atomic manifest. Also an in-memory store. |
| Networking | `net/rpc` over TCP with connection pooling; an "isolate" switch that simulates a partition |
| Operations | `kvserver`, `kvcli`, `kvdash` (web dashboard), Prometheus metrics, Grafana dashboard, Docker Compose |
| Scaling | Sharding across independent Raft groups with consistent hashing |
| Testing | In-memory network with drops, delays, duplicates, reordering, partitions and crashes; MIT 6.5840-style Raft tests; randomized KV fault scenarios checked with Porcupine; end-to-end tests over real TCP and disk |

## Quick start

Requires Go 1.24+.

```bash
make build
```

```bash
scripts/run-local.sh
```

That starts three nodes and the dashboard at http://127.0.0.1:8080.
In another terminal:

```bash
bin/kvcli put x 5
```

```bash
bin/kvcli get x
```

```bash
bin/kvcli status
```

```bash
curl -X PUT --data hello http://127.0.0.1:8001/kv/greeting
```

Crash or isolate the leader from the dashboard and watch a new one get
elected, while reads and writes keep working.

### Docker

```bash
docker compose -f deploy/docker-compose.yml up --build
```

| URL | What |
|---|---|
| http://localhost:8080 | Dashboard: node roles, terms, log indices; isolate / crash buttons; KV form |
| http://localhost:3000 | Grafana, dashboard "Raft KV" |
| http://localhost:9090 | Prometheus |
| http://localhost:8001 to 8003 | Each node's `/status`, `/metrics`, `/kv/<key>` |

A crashed container is restarted by Docker and recovers from its volume.

## Testing

```bash
make race
```

The full suite takes about 5 minutes with the race detector. Highlights:

- `internal/raft`: elections, re-elections, partitions, agreement with
  failed followers and leaders, log backtracking over 50 conflicting
  entries, repeated crashes, the Figure 8 scenario (reliable and unreliable
  networks), snapshots with crashes and partitions, ReadIndex, membership
  changes, and file-persister crash recovery.
- `internal/kv`: clients issue random operations while the harness drops
  and reorders messages, partitions the cluster and crashes every node.
  Every history is checked with Porcupine.
- `internal/lsm`: random operations compared with a map across reopen,
  crash recovery with a torn WAL record, tombstone compaction, concurrency.
- `internal/node`: a real cluster over TCP with on-disk state: leader crash,
  restart from disk, full-cluster restart, adding and removing a node.
- `internal/shard`: two Raft groups with leader failures, checked with
  Porcupine.

## Layout

```
cmd/kvserver             one node
cmd/kvcli                command-line client
cmd/kvdash               web dashboard
internal/raft            consensus
internal/kv              replicated state machine, client library, Store interface
internal/lsm             LSM-tree storage engine
internal/transport/tcp   net/rpc transport
internal/transport/fakenet  fault-injecting in-memory network (tests)
internal/linearizability Porcupine model and history recorder
internal/node            wires a full server together (Raft + KV + storage + RPC + HTTP)
internal/metrics         Prometheus metrics
internal/shard           consistent hashing and the sharded client
deploy/                  Docker Compose, Prometheus, Grafana
docs/                    design notes
```

## Design decisions and tradeoffs

- **Reads use ReadIndex** by default: one heartbeat round confirms
  leadership, then the leader reads locally. Reads cost no disk write and
  need no clock assumptions. `-read log` routes reads through the log
  instead, which is simpler and slower. Leader leases would be faster still,
  but are only safe if clock drift is bounded.
- **Consistency over availability.** A minority partition rejects writes
  and reads. Check-quorum makes a cut-off leader step down within an
  election timeout, so its clients get redirected.
- **Pre-Vote and leader stickiness** stop a node that was partitioned, or
  removed from the cluster, from forcing elections when it comes back.
- **The KV store keeps its applied index and the client session table in
  the store itself**, written in the same atomic batch as the data. A
  durable (LSM) replica therefore restarts exactly where it left off, and
  snapshots carry the dedup table automatically.
- **The LSM WAL is not fsynced per write.** The Raft log is the source of
  truth and is fsynced. If the store loses its newest batches in a crash,
  its applied index goes back too, and Raft re-delivers those entries.
- **net/rpc instead of gRPC** keeps the build free of code generation.
  Raft depends only on the `Transport` interface, so switching is local.
- **Compaction is size-tiered:** all tables are merged once there are too
  many. It is simple, and tombstones can always be dropped, at the cost of
  more write amplification than leveled compaction.

Known limitations: client sessions are never expired; snapshots are sent in
one message; the shard map is static (no shard migration).

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper)
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (thesis: Pre-Vote, ReadIndex, membership changes)
- MIT 6.5840 Distributed Systems, Labs 3 and 4
- LevelDB implementation notes; Porcupine by Anish Athalye
