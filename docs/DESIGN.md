# Design and code tour

This document maps each milestone of the plan to the code that implements it
and explains the parts that are easy to get wrong. Read it next to the
source.

| Milestone | Where |
|---|---|
| M1 Leader election | `internal/raft/election.go`, `raft.go` (ticker, role changes) |
| M2 Log replication | `internal/raft/replication.go`, `apply.go` |
| M3 Persistence | `internal/raft/filepersist.go`, `persist.go` |
| M4 KV service | `internal/kv/server.go`, `client.go`, `internal/raft/readindex.go` |
| M5 Fault injection and linearizability | `internal/transport/fakenet`, `internal/linearizability`, `*_test.go` |
| M6 Snapshots | `internal/raft/snapshot.go`, `internal/kv/server.go` (snapshot encoding) |
| M7 Real networking | `internal/transport/tcp`, `internal/node`, `cmd/` |
| M8 LSM tree | `internal/lsm`, `internal/kv/lsmstore.go` |
| M9 Stretch goals | `internal/raft/membership.go`, `internal/shard`, `internal/metrics`, `cmd/kvdash`, `deploy/` |

## 1. Raft (`internal/raft`)

### State and concurrency

`Raft` holds the state from Figure 2 of the paper behind a single mutex.
Background goroutines:

- **ticker** (every 10 ms): starts an election when the randomized election
  deadline passes; as leader, runs check-quorum.
- **applier**: the only writer to `applyCh`. It sends committed entries (and
  installed snapshots) strictly in order, without holding the lock.
- **one replicator per peer** while leader: sends AppendEntries or
  InstallSnapshot, with at most one RPC in flight per peer, woken by a
  heartbeat ticker or by new entries.

Rule followed everywhere: **never hold the lock during an RPC; after
re-acquiring it, check that term and role are unchanged** before using the
reply (`replicateOnce`, the vote callbacks in `election.go`).

### Log indexing

`entries[0]` is a sentinel holding the index, term and membership of the
latest snapshot. All indices are absolute; `entryAt(i)` and `termAt(i)`
subtract `base()`. With no snapshot the sentinel is index 0, term 0.

### Elections (`election.go`)

1. **Pre-vote**: ask peers "would you vote for me in term T+1?" No state
   changes. A node that cannot win (its log is stale, or the others hear
   from a live leader) never bumps its term.
2. **Election**: increment the term, vote for self, persist, request votes.
   A majority of the current configuration makes the node leader.

`HandleRequestVote` grants a vote only if the candidate's log is at least
as up to date (`upToDateLocked`: compare last term, then length). This is
what guarantees a new leader holds every committed entry.

*Leader stickiness* (`inLeaderLeaseLocked`): a node that heard from a leader
less than one minimum election timeout ago ignores vote requests. Together
with pre-vote, this means a rejoining or removed node cannot depose a
healthy leader. `TestPartitionedFollowerDoesNotDisrupt` checks this.

A new leader appends a **no-op entry**. Committing it is how the leader
learns the real commit index; ReadIndex and membership changes wait for it.

### Replication (`replication.go`)

The follower's `HandleAppendEntries`:

1. rejects a stale term; otherwise accepts the sender as leader and resets
   its election timer;
2. if `PrevLogIndex` is inside its snapshot, skips the covered entries;
3. runs the consistency check. On mismatch it returns `ConflictTerm` and the
   first index of that term (or its log length), so the leader can skip a
   whole term per round trip;
4. appends, **truncating only at the first conflicting entry**. A delayed,
   shorter AppendEntries must never delete entries a newer one added;
5. advances `commitIndex` to `min(LeaderCommit, last new index)`.

The leader's `advanceCommitLocked` implements the **Figure 8 rule**: it only
commits index N when `term(N) == currentTerm` and a majority has it. Entries
from older terms are committed indirectly. `TestFigure8*` builds random
crash schedules that break this rule if it is missing.

### Persistence (`filepersist.go`)

Raft persists `currentTerm`, `votedFor` and the log before answering any
RPC (`persistLocked` is called before returning from handlers).
`FilePersister`:

- `hardstate`: term and vote, written atomically (temp file, fsync, rename,
  fsync the directory).
- `log`: append-only records with length and CRC. A save appends only the
  new suffix when the old on-disk log is a prefix of the new one. By the
  Log Matching property, that is true exactly when the new log still has the
  old last entry with the same term. Otherwise (a conflict truncation or a
  snapshot) the file is rewritten atomically.
- A torn record at the tail (crash mid-append, never acknowledged) is
  dropped on load.
- The snapshot is written before the log is rewritten. A crash in between
  is reconciled on load from the snapshot's metadata.

### Snapshots (`snapshot.go`)

The service calls `Snapshot(index, data)`; Raft replaces the log prefix with
a new sentinel. A follower whose `nextIndex` falls inside the leader's
snapshot receives `InstallSnapshot`. It keeps any log suffix that matches
and hands the snapshot to the applier, which delivers it before any later
entries.

### ReadIndex (`readindex.go`)

1. Require an entry from the current term to be committed.
2. Record `commitIndex` as the read index.
3. Send a heartbeat round. A majority answering in our term proves no newer
   leader exists.
4. The KV layer waits until it has applied the read index, then reads.

### Membership (`membership.go`)

Single-server changes: add or remove one node per configuration entry. A
config takes effect when appended, not when committed. Only one change may
be uncommitted at a time, and the leader must first have committed an entry
in its own term. A leader that removes itself keeps leading until the
change commits, without counting itself, then steps down. New nodes start
with an empty configuration (`Config.Members == nil`) and never campaign
until a configuration that includes them reaches them.

## 2. KV service (`internal/kv`)

- `Server.submit` proposes a request with `raft.Start` and waits on a
  channel registered for that log index. If a *different* entry is applied
  at that index (a lost leadership), the client gets `ErrWrongLeader` and
  retries.
- **Exactly-once:** each request carries `ClientID` and `Seq`. The state
  machine keeps the last `Seq` and its response per client. A duplicate
  write returns the cached response without being applied again.
- **Metadata in the store:** `\x00applied` and `\x00session/<id>` are written
  in the same atomic batch as the data. A durable store restarts at the
  right index, and a snapshot is just a dump of all keys.
- **Client** (`client.go`): it follows `LeaderID` hints, otherwise tries
  servers round-robin, and backs off after a full round. One request is in
  flight at a time, and every retry uses the same sequence number.

## 3. Testing (`fakenet`, `linearizability`)

`fakenet.Network` connects endpoints by name. Every message is gob
round-tripped, so nodes never share memory. It supports:

- disconnecting a node, and grouping nodes into partitions;
- unreliable mode: 0–27 ms delay, 10% request loss, 10% reply loss (the
  request was executed!), and 5% duplicate delivery;
- long reordering: replies occasionally delayed by up to about 2 s.

Raft tests use a harness (`harness_test.go`) that records what every node
applies and fails as soon as two nodes apply different commands at the same
index. KV tests (`kv/harness_test.go`) run clients against a 5-node
cluster under the chosen faults, record every operation's call and return
time, and check the history with Porcupine (`internal/linearizability`).

## 4. LSM tree (`internal/lsm`)

```
Write ─► WAL append ─► memtable (skip list)
                         │ full
                         ▼
                   immutable memtable ──flush (background)──► SSTable (newest)
Get ─► memtable ─► immutable ─► SSTables newest→oldest (bloom filter first)
Compaction: when #tables ≥ trigger, k-way merge all tables into one; drop
           shadowed versions and tombstones; swap in via MANIFEST
```

- **WAL** (`wal.go`): one checksummed record per batch, so batches are
  atomic. Replay stops at the first torn or corrupt record.
- **SSTable** (`sstable.go`): about 4 KiB data blocks with CRCs, a sparse
  index (last key per block) and a bloom filter, both loaded into memory.
  A lookup is a bloom check, a binary search over the index, and one block
  read.
- **Bloom filter** (`bloom.go`): 10 bits per key with k = 7 gives under 1%
  false positives. It uses double hashing.
- **Merge iterator** (`merge.go`): a heap keyed by (key, source age). For
  equal keys the newest source wins.
- **Manifest and refcounts** (`db.go`): the manifest lists live tables and
  the oldest WAL still needed. It is replaced atomically. Tables are
  reference counted, so a compaction never deletes a file a reader is using.

## 5. Networking and operations

- `internal/transport/tcp`: net/rpc services `Raft`, `KV` and `Node`.
  `Pool` keeps one connection per address and redials after errors.
  Isolation makes a node refuse and fail all RPCs, like a partition.
- `internal/node`: builds a full server and its HTTP API (`/status`,
  `/metrics`, `/kv/<key>`, `/admin/isolate`, `/admin/crash`,
  `/admin/members`).
- `internal/metrics`: gauges read from `raft.Status` at scrape time, plus
  request counters and latency histograms.

## 6. Sharding (`internal/shard`)

A consistent-hash ring with virtual nodes maps each key to a Raft group.
The sharded client holds one `kv.Client` per group. Adding a group moves
about 1/N of the keys (`TestRingMinimalMovement`). The shard map is static;
moving data between groups is not implemented.
