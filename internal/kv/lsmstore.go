package kv

import "github.com/anair0/distributed-key-value-store-with-raft/internal/lsm"

// LSMStore adapts the LSM tree to the Store interface. It is durable: on
// restart the server resumes from the last applied index recorded in the
// store, instead of rebuilding from the Raft snapshot.
//
// The WAL does not need an fsync per batch here. Raft's log is the source of
// truth and is fsynced; if the store loses its last few batches in a crash,
// its recorded applied index is correspondingly older (batches are atomic),
// and Raft re-delivers those entries.
type LSMStore struct{ DB *lsm.DB }

// OpenLSMStore opens an LSM store in dir.
func OpenLSMStore(dir string, opts lsm.Options) (*LSMStore, error) {
	db, err := lsm.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &LSMStore{DB: db}, nil
}

func (s *LSMStore) Get(key string) ([]byte, bool, error) { return s.DB.Get(key) }

func (s *LSMStore) Write(ops []WriteOp) error {
	batch := make([]lsm.Op, len(ops))
	for i, op := range ops {
		batch[i] = lsm.Op{Key: op.Key, Value: op.Value, Delete: op.Delete}
	}
	return s.DB.Write(batch)
}

func (s *LSMStore) Scan(fn func(string, []byte) bool) error { return s.DB.Scan(fn) }
func (s *LSMStore) Reset() error                            { return s.DB.Reset() }
func (s *LSMStore) Close() error                            { return s.DB.Close() }
