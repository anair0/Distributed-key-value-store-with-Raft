// Package lsm is a log-structured merge-tree storage engine, in the style of
// LevelDB/RocksDB:
//
//	Write  -> WAL (append) -> memtable (skip list)
//	memtable full -> becomes immutable -> flushed in the background to an SSTable
//	Get    -> memtable -> immutable memtable -> SSTables newest to oldest (bloom filters skip most)
//	Delete -> writes a tombstone
//	too many SSTables -> background compaction merges them (k-way merge),
//	                     dropping overwritten values and tombstones
//
// Compaction strategy: "universal" / size-tiered in its simplest form.
// When the number of tables reaches CompactionTrigger, all of them are
// merged into one. Because the merge always includes the oldest table,
// tombstones can be dropped safely. This is simple and keeps reads fast, at
// the cost of higher write amplification than leveled compaction.
//
// The set of live files is recorded in MANIFEST, replaced atomically.
package lsm

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Op is one mutation in an atomic batch.
type Op struct {
	Key    string
	Value  []byte
	Delete bool
}

// Options configures a DB.
type Options struct {
	// MemtableSize is the approximate memtable size that triggers a flush.
	MemtableSize int
	// SyncWrites fsyncs the WAL on every Write. Without it a crash may lose
	// the most recent writes (but never part of a batch).
	SyncWrites bool
	// CompactionTrigger is the number of SSTables that triggers compaction.
	CompactionTrigger int
	// BlockSize is the target size of SSTable data blocks.
	BlockSize int
	// BloomBitsPerKey sizes the bloom filters (10 gives ~1% false positives).
	BloomBitsPerKey int
}

func (o Options) withDefaults() Options {
	if o.MemtableSize == 0 {
		o.MemtableSize = 4 << 20
	}
	if o.CompactionTrigger == 0 {
		o.CompactionTrigger = 4
	}
	if o.BlockSize == 0 {
		o.BlockSize = 4 << 10
	}
	if o.BloomBitsPerKey == 0 {
		o.BloomBitsPerKey = 10
	}
	return o
}

// Stats are counters for tests and metrics.
type Stats struct {
	Tables      int
	Flushes     int
	Compactions int
	MemtableKB  int
}

type manifest struct {
	NextFile  uint64
	LogNumber uint64   // WAL files numbered >= this must be replayed
	Tables    []uint64 // newest first
}

// ErrClosed is returned after Close.
var ErrClosed = errors.New("lsm: database closed")

// DB is an LSM-tree key-value store. It is safe for concurrent use.
type DB struct {
	dir  string
	opts Options

	mu         sync.Mutex
	cond       *sync.Cond // signalled when background work finishes
	mem        *skiplist
	imm        *skiplist // being flushed, or nil
	wal        *wal
	walNum     uint64
	immWalNum  uint64
	tables     []*table // newest first
	man        manifest
	compacting bool
	bgErr      error
	closed     bool
	stats      Stats
	wg         sync.WaitGroup
}

// Open opens or creates a database in dir.
func Open(dir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{dir: dir, opts: opts.withDefaults(), mem: newSkiplist()}
	db.cond = sync.NewCond(&db.mu)
	if err := db.recover(); err != nil {
		db.closeFiles()
		return nil, err
	}
	return db, nil
}

func (db *DB) path(name string) string { return filepath.Join(db.dir, name) }
func tableName(n uint64) string        { return fmt.Sprintf("%06d.sst", n) }
func walName(n uint64) string          { return fmt.Sprintf("%06d.log", n) }

func (db *DB) recover() error {
	b, err := os.ReadFile(db.path("MANIFEST"))
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &db.man); err != nil {
			return fmt.Errorf("lsm: bad manifest: %w", err)
		}
	case errors.Is(err, os.ErrNotExist):
		db.man = manifest{NextFile: 1}
	default:
		return err
	}
	for _, n := range db.man.Tables {
		t, err := openTable(db.path(tableName(n)), n)
		if err != nil {
			return err
		}
		db.tables = append(db.tables, t)
	}

	// Replay WALs newer than the last flush into the memtable.
	files, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}
	live := map[uint64]bool{}
	for _, n := range db.man.Tables {
		live[n] = true
	}
	var wals []uint64
	for _, f := range files {
		name := f.Name()
		if strings.HasSuffix(name, ".tmp") {
			os.Remove(db.path(name))
			continue
		}
		num, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimSuffix(name, ".sst"), ".log"), 10, 64)
		if err != nil {
			continue
		}
		switch {
		case strings.HasSuffix(name, ".log") && num >= db.man.LogNumber:
			wals = append(wals, num)
		case strings.HasSuffix(name, ".log"), strings.HasSuffix(name, ".sst") && !live[num]:
			os.Remove(db.path(name)) // obsolete leftovers of an interrupted flush/compaction
		}
		db.man.NextFile = max(db.man.NextFile, num+1)
	}
	sort.Slice(wals, func(i, j int) bool { return wals[i] < wals[j] })
	for _, n := range wals {
		err := replayWAL(db.path(walName(n)), func(es []entry) {
			for _, e := range es {
				db.mem.put(e.key, e.value, e.kind)
			}
		})
		if err != nil {
			return err
		}
	}

	// Persist the replayed data as a table so the old WALs can go.
	if db.mem.n > 0 {
		t, err := db.writeTable(db.mem.entries(), false)
		if err != nil {
			return err
		}
		db.tables = append([]*table{t}, db.tables...)
		db.mem = newSkiplist()
	}
	if err := db.newWALLocked(); err != nil {
		return err
	}
	db.man.LogNumber = db.walNum
	if err := db.saveManifestLocked(); err != nil {
		return err
	}
	for _, n := range wals {
		os.Remove(db.path(walName(n)))
	}
	db.maybeCompactLocked()
	return nil
}

func (db *DB) newWALLocked() error {
	n := db.man.NextFile
	db.man.NextFile++
	w, err := createWAL(db.path(walName(n)), db.opts.SyncWrites)
	if err != nil {
		return err
	}
	db.wal, db.walNum = w, n
	return nil
}

func (db *DB) saveManifestLocked() error {
	db.man.Tables = db.man.Tables[:0]
	for _, t := range db.tables {
		db.man.Tables = append(db.man.Tables, t.num)
	}
	b, err := json.Marshal(db.man)
	if err != nil {
		return err
	}
	return writeFileAtomic(db.path("MANIFEST"), b)
}

// writeTable writes entries (sorted, unique) to a new SSTable. If
// dropTombstones, deletions are omitted (only valid when no older data
// exists below the output).
func (db *DB) writeTable(es []entry, dropTombstones bool) (*table, error) {
	return db.writeTableFrom(&sliceIter{es: es}, dropTombstones)
}

func (db *DB) allocFileNum() uint64 {
	n := db.man.NextFile
	db.man.NextFile++
	return n
}

func (db *DB) writeTableFrom(it iterator, dropTombstones bool) (*table, error) {
	db.mu.Lock()
	num := db.allocFileNum()
	db.mu.Unlock()
	path := db.path(tableName(num))
	tw, err := newTableWriter(path, db.opts.BlockSize, db.opts.BloomBitsPerKey)
	if err != nil {
		return nil, err
	}
	for ; it.valid(); it.next() {
		e := it.current()
		if dropTombstones && e.kind == kindDelete {
			continue
		}
		if err := tw.add(e); err != nil {
			tw.abort()
			return nil, err
		}
	}
	if err := it.error(); err != nil {
		tw.abort()
		return nil, err
	}
	if err := tw.finish(); err != nil {
		os.Remove(path)
		return nil, err
	}
	return openTable(path, num)
}

// Write applies a batch atomically.
func (db *DB) Write(ops []Op) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	// Backpressure: if the previous memtable is still being flushed and the
	// current one is full, wait.
	for db.imm != nil && db.mem.bytes >= db.opts.MemtableSize && db.bgErr == nil {
		db.cond.Wait()
	}
	if db.bgErr != nil {
		return db.bgErr
	}
	if err := db.wal.append(ops); err != nil {
		return err
	}
	for _, op := range ops {
		if op.Delete {
			db.mem.put(op.Key, nil, kindDelete)
		} else {
			db.mem.put(op.Key, slices.Clone(op.Value), kindPut)
		}
	}
	if db.mem.bytes >= db.opts.MemtableSize && db.imm == nil {
		return db.rotateLocked()
	}
	return nil
}

// Put sets key to value.
func (db *DB) Put(key string, value []byte) error { return db.Write([]Op{{Key: key, Value: value}}) }

// Delete removes key.
func (db *DB) Delete(key string) error { return db.Write([]Op{{Key: key, Delete: true}}) }

// rotateLocked makes the memtable immutable, starts a new WAL and flushes
// the old memtable in the background.
func (db *DB) rotateLocked() error {
	if err := db.wal.close(); err != nil {
		return err
	}
	db.imm, db.immWalNum = db.mem, db.walNum
	db.mem = newSkiplist()
	if err := db.newWALLocked(); err != nil {
		return err
	}
	db.wg.Add(1)
	go db.flush(db.imm)
	return nil
}

func (db *DB) flush(imm *skiplist) {
	defer db.wg.Done()
	t, err := db.writeTable(imm.entries(), false)
	db.mu.Lock()
	defer db.mu.Unlock()
	defer db.cond.Broadcast()
	if err != nil {
		db.bgErr = err
		return
	}
	db.tables = append([]*table{t}, db.tables...)
	db.imm = nil
	db.man.LogNumber = db.walNum // the flushed WAL is no longer needed
	if err := db.saveManifestLocked(); err != nil {
		db.bgErr = err
		return
	}
	os.Remove(db.path(walName(db.immWalNum)))
	db.stats.Flushes++
	db.maybeCompactLocked()
}

func (db *DB) maybeCompactLocked() {
	if db.compacting || db.closed || len(db.tables) < db.opts.CompactionTrigger {
		return
	}
	inputs := slices.Clone(db.tables)
	for _, t := range inputs {
		t.ref()
	}
	db.compacting = true
	db.wg.Add(1)
	go db.compact(inputs)
}

// compact merges inputs (all tables at the time it started, newest first)
// into a single table. Tables flushed meanwhile are newer than every input,
// so they stay in front of the output.
func (db *DB) compact(inputs []*table) {
	defer db.wg.Done()
	var its []iterator
	for _, t := range inputs {
		its = append(its, t.iter())
	}
	out, err := db.writeTableFrom(newMergeIter(its), true)
	for _, t := range inputs {
		t.unref()
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	defer db.cond.Broadcast()
	db.compacting = false
	if err != nil {
		db.bgErr = err
		return
	}
	n := len(db.tables) - len(inputs)
	db.tables = append(db.tables[:n:n], out)
	if err := db.saveManifestLocked(); err != nil {
		db.bgErr = err
		return
	}
	for _, t := range inputs {
		t.obsolete.Store(true)
		t.unref() // the DB's reference; the file goes when readers are done
	}
	db.stats.Compactions++
	db.maybeCompactLocked()
}

// Get returns the value for key.
func (db *DB) Get(key string) ([]byte, bool, error) {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, false, ErrClosed
	}
	if e, ok := db.mem.get(key); ok {
		db.mu.Unlock()
		return e.value, e.kind == kindPut, nil
	}
	if db.imm != nil {
		if e, ok := db.imm.get(key); ok {
			db.mu.Unlock()
			return e.value, e.kind == kindPut, nil
		}
	}
	tables := db.refTablesLocked()
	db.mu.Unlock()
	defer unrefAll(tables)

	h := keyHash(key)
	for _, t := range tables {
		e, ok, err := t.get(key, h)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return e.value, e.kind == kindPut, nil
		}
	}
	return nil, false, nil
}

func (db *DB) refTablesLocked() []*table {
	ts := slices.Clone(db.tables)
	for _, t := range ts {
		t.ref()
	}
	return ts
}

func unrefAll(ts []*table) {
	for _, t := range ts {
		t.unref()
	}
}

// Scan calls fn for each live key in ascending order until fn returns
// false. It sees a consistent snapshot taken when it starts.
func (db *DB) Scan(fn func(key string, value []byte) bool) error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	sources := []iterator{&sliceIter{es: db.mem.entries()}}
	if db.imm != nil {
		sources = append(sources, &sliceIter{es: db.imm.entries()})
	}
	tables := db.refTablesLocked()
	db.mu.Unlock()
	defer unrefAll(tables)

	for _, t := range tables {
		sources = append(sources, t.iter())
	}
	m := newMergeIter(sources)
	for ; m.valid(); m.next() {
		e := m.current()
		if e.kind == kindDelete {
			continue
		}
		if !fn(e.key, e.value) {
			return nil
		}
	}
	return m.error()
}

// waitIdleLocked waits for background flushes and compactions to finish.
func (db *DB) waitIdleLocked() {
	for db.imm != nil || db.compacting {
		if db.bgErr != nil && db.imm != nil {
			return
		}
		db.cond.Wait()
	}
}

// Reset deletes all data.
func (db *DB) Reset() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.waitIdleLocked()
	if err := db.wal.close(); err != nil {
		return err
	}
	old := db.walNum
	if err := db.newWALLocked(); err != nil {
		return err
	}
	dropped := db.tables
	db.tables = nil
	db.mem, db.imm = newSkiplist(), nil
	db.man.LogNumber = db.walNum
	if err := db.saveManifestLocked(); err != nil {
		return err
	}
	os.Remove(db.path(walName(old)))
	for _, t := range dropped {
		t.obsolete.Store(true)
		t.unref()
	}
	return nil
}

// Flush forces the memtable to an SSTable and waits for it (for tests and
// clean shutdown).
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.waitIdleLocked()
	if db.mem.n == 0 {
		return db.bgErr
	}
	if err := db.rotateLocked(); err != nil {
		return err
	}
	db.waitIdleLocked()
	return db.bgErr
}

// Stats returns counters.
func (db *DB) Stats() Stats {
	db.mu.Lock()
	defer db.mu.Unlock()
	s := db.stats
	s.Tables = len(db.tables)
	s.MemtableKB = db.mem.bytes / 1024
	return s
}

// Close waits for background work and closes all files. Data in the
// memtable is in the (synced) WAL and is recovered on the next Open.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.waitIdleLocked()
	db.closed = true
	db.mu.Unlock()
	db.wg.Wait()
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.closeFiles()
}

func (db *DB) closeFiles() error {
	var err error
	if db.wal != nil {
		err = db.wal.close()
		db.wal = nil
	}
	for _, t := range db.tables {
		t.unref()
	}
	db.tables = nil
	return err
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
