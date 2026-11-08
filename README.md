# Distributed Key-Value Store with Raft

A fault-tolerant, replicated key-value store written in Go. A cluster of 3 or 5
nodes stays correct and available while a minority of nodes crash or are
partitioned.

```
Client -> Server API -> Raft consensus -> State machine (KV) -> Storage engine (LSM tree)
```

## Status

Scaffold only (milestone M0). See [docs/PLAN.md](docs/PLAN.md).

## Development

```bash
make build   # build binaries into bin/
make race    # go test -race ./...
make ci      # vet + race tests
```

## References

- Raft paper (Ongaro & Ousterhout), especially Figure 2
- Ongaro's thesis (ReadIndex, membership changes)
- MIT 6.5840 Labs 2 and 3
- raft.github.io
