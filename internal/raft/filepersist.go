package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FilePersister stores Raft state durably in a directory:
//
//	hardstate  currentTerm + votedFor, replaced atomically (write tmp, fsync, rename)
//	log        append-only records [len u32][crc32 u32][entry]; first record is the sentinel
//	snapshot   [crc32 u32][meta + data], replaced atomically
//
// SaveState appends only the new suffix when the old on-disk log is a prefix
// of the new one, which by the Log Matching property is true exactly when the
// new log still contains the old last entry (same index and term). Otherwise
// (truncation after a conflict, or compaction) it rewrites the log file.
type FilePersister struct {
	mu  sync.Mutex
	dir string

	logFile *os.File
	logSize int64

	hasHard   bool
	hardTerm  uint64
	hardVote  NodeID
	diskBase  uint64
	diskLastI uint64
	diskLastT uint64
	diskEmpty bool
}

// OpenFilePersister opens (creating if needed) a persister in dir.
func OpenFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FilePersister{dir: dir, diskEmpty: true}, nil
}

func (p *FilePersister) path(name string) string { return filepath.Join(p.dir, name) }

// Close closes the log file.
func (p *FilePersister) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.logFile != nil {
		err := p.logFile.Close()
		p.logFile = nil
		return err
	}
	return nil
}

func (p *FilePersister) SaveState(st PersistentState) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.saveHardLocked(st); err != nil {
		return err
	}
	return p.saveLogLocked(st.Log, false)
}

func (p *FilePersister) SaveStateAndSnapshot(st PersistentState, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Snapshot first: if we crash before the log is rewritten, LoadState
	// reconciles the old log against the newer snapshot.
	meta := encodeEntry(nil, st.Log[0])
	buf := binary.AppendUvarint(nil, uint64(len(meta)))
	buf = append(buf, meta...)
	buf = append(buf, snapshot...)
	out := binary.LittleEndian.AppendUint32(nil, crc32.ChecksumIEEE(buf))
	out = append(out, buf...)
	if err := writeFileAtomic(p.path("snapshot"), out); err != nil {
		return err
	}
	if err := p.saveHardLocked(st); err != nil {
		return err
	}
	return p.saveLogLocked(st.Log, true)
}

func (p *FilePersister) saveHardLocked(st PersistentState) error {
	if p.hasHard && p.hardTerm == st.CurrentTerm && p.hardVote == st.VotedFor {
		return nil
	}
	b := binary.LittleEndian.AppendUint64(nil, st.CurrentTerm)
	b = binary.LittleEndian.AppendUint64(b, uint64(int64(st.VotedFor)))
	b = binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
	if err := writeFileAtomic(p.path("hardstate"), b); err != nil {
		return err
	}
	p.hasHard, p.hardTerm, p.hardVote = true, st.CurrentTerm, st.VotedFor
	return nil
}

func (p *FilePersister) saveLogLocked(log []LogEntry, forceRewrite bool) error {
	base := log[0].Index
	last := log[len(log)-1]
	canAppend := !forceRewrite && !p.diskEmpty && p.logFile != nil &&
		base == p.diskBase && p.diskLastI >= base && p.diskLastI <= last.Index &&
		log[p.diskLastI-base].Term == p.diskLastT
	if canAppend {
		if p.diskLastI == last.Index {
			return nil // nothing new
		}
		var buf []byte
		for _, e := range log[p.diskLastI-base+1:] {
			buf = appendRecord(buf, encodeEntry(nil, e))
		}
		if _, err := p.logFile.Write(buf); err != nil {
			return err
		}
		if err := p.logFile.Sync(); err != nil {
			return err
		}
		p.logSize += int64(len(buf))
		p.diskLastI, p.diskLastT = last.Index, last.Term
		return nil
	}

	var buf []byte
	for _, e := range log {
		buf = appendRecord(buf, encodeEntry(nil, e))
	}
	if p.logFile != nil {
		p.logFile.Close()
		p.logFile = nil
	}
	if err := writeFileAtomic(p.path("log"), buf); err != nil {
		return err
	}
	f, err := os.OpenFile(p.path("log"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	p.logFile = f
	p.logSize = int64(len(buf))
	p.diskBase, p.diskLastI, p.diskLastT, p.diskEmpty = base, last.Index, last.Term, false
	return nil
}

func (p *FilePersister) LoadState() (PersistentState, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var st PersistentState
	saved := false

	hb, err := os.ReadFile(p.path("hardstate"))
	switch {
	case err == nil:
		if len(hb) != 20 || crc32.ChecksumIEEE(hb[:16]) != binary.LittleEndian.Uint32(hb[16:]) {
			return st, false, errors.New("raft: corrupt hardstate file")
		}
		st.CurrentTerm = binary.LittleEndian.Uint64(hb[0:])
		st.VotedFor = NodeID(int64(binary.LittleEndian.Uint64(hb[8:])))
		p.hasHard, p.hardTerm, p.hardVote = true, st.CurrentTerm, st.VotedFor
		saved = true
	case errors.Is(err, os.ErrNotExist):
		st.VotedFor = None
	default:
		return st, false, err
	}

	log, err := p.readLogLocked()
	if err != nil {
		return st, false, err
	}
	if snapMeta, ok, err := p.readSnapshotMetaLocked(); err != nil {
		return st, false, err
	} else if ok && (len(log) == 0 || snapMeta.Index > log[0].Index) {
		// Crash between writing the snapshot and rewriting the log.
		var rest []LogEntry
		for i, e := range log {
			if e.Index == snapMeta.Index && e.Term == snapMeta.Term {
				rest = log[i+1:]
				break
			}
		}
		log = append([]LogEntry{snapMeta}, rest...)
		if err := p.saveLogLocked(log, true); err != nil {
			return st, false, err
		}
	}
	if len(log) > 0 {
		st.Log = log
		saved = true
	}
	return st, saved, nil
}

// readLogLocked reads all intact records, truncating a torn tail (an
// append that was never fsynced, so never acknowledged).
func (p *FilePersister) readLogLocked() ([]LogEntry, error) {
	f, err := os.OpenFile(p.path("log"), os.O_RDWR, 0o644)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var log []LogEntry
	var good int64
	rd := bufio.NewReader(f)
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(rd, hdr[:]); err != nil {
			break
		}
		n := binary.LittleEndian.Uint32(hdr[0:])
		payload := make([]byte, n)
		if _, err := io.ReadFull(rd, payload); err != nil {
			break
		}
		if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(hdr[4:]) {
			break
		}
		e, err := decodeEntry(payload)
		if err != nil {
			break
		}
		log = append(log, e)
		good += int64(8 + n)
	}
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	if p.logFile != nil {
		p.logFile.Close()
	}
	p.logFile, p.logSize = f, good
	if len(log) > 0 {
		p.diskBase, p.diskLastI, p.diskLastT, p.diskEmpty = log[0].Index, log[len(log)-1].Index, log[len(log)-1].Term, false
	}
	return log, nil
}

func (p *FilePersister) readSnapshotFileLocked() (meta LogEntry, data []byte, ok bool, err error) {
	b, err := os.ReadFile(p.path("snapshot"))
	if errors.Is(err, os.ErrNotExist) {
		return meta, nil, false, nil
	}
	if err != nil {
		return meta, nil, false, err
	}
	if len(b) < 4 || crc32.ChecksumIEEE(b[4:]) != binary.LittleEndian.Uint32(b) {
		return meta, nil, false, errors.New("raft: corrupt snapshot file")
	}
	b = b[4:]
	n, k := binary.Uvarint(b)
	if k <= 0 || uint64(len(b)-k) < n {
		return meta, nil, false, errors.New("raft: corrupt snapshot header")
	}
	meta, err = decodeEntry(b[k : k+int(n)])
	if err != nil {
		return meta, nil, false, err
	}
	return meta, b[k+int(n):], true, nil
}

func (p *FilePersister) readSnapshotMetaLocked() (LogEntry, bool, error) {
	m, _, ok, err := p.readSnapshotFileLocked()
	return m, ok, err
}

func (p *FilePersister) LoadSnapshot() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, data, _, err := p.readSnapshotFileLocked()
	return data, err
}

func (p *FilePersister) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int(p.logSize)
}

// ---- encoding ----

func appendRecord(buf, payload []byte) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(payload)))
	buf = binary.LittleEndian.AppendUint32(buf, crc32.ChecksumIEEE(payload))
	return append(buf, payload...)
}

func encodeEntry(buf []byte, e LogEntry) []byte {
	buf = binary.AppendUvarint(buf, e.Index)
	buf = binary.AppendUvarint(buf, e.Term)
	buf = append(buf, byte(e.Kind))
	buf = binary.AppendUvarint(buf, uint64(len(e.Command)))
	buf = append(buf, e.Command...)
	buf = binary.AppendUvarint(buf, uint64(len(e.Members)))
	for _, m := range e.Members {
		buf = binary.AppendVarint(buf, int64(m.ID))
		buf = binary.AppendUvarint(buf, uint64(len(m.Addr)))
		buf = append(buf, m.Addr...)
	}
	return buf
}

var errCorruptEntry = errors.New("raft: corrupt log entry")

func decodeEntry(b []byte) (LogEntry, error) {
	var e LogEntry
	uv := func() uint64 {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			panic(errCorruptEntry)
		}
		b = b[n:]
		return v
	}
	bytesN := func(n uint64) []byte {
		if uint64(len(b)) < n {
			panic(errCorruptEntry)
		}
		out := append([]byte(nil), b[:n]...)
		b = b[n:]
		return out
	}
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}
		}()
		e.Index = uv()
		e.Term = uv()
		e.Kind = EntryKind(bytesN(1)[0])
		if n := uv(); n > 0 {
			e.Command = bytesN(n)
		}
		nm := uv()
		for range nm {
			id, k := binary.Varint(b)
			if k <= 0 {
				panic(errCorruptEntry)
			}
			b = b[k:]
			addr := bytesN(uv())
			e.Members = append(e.Members, Member{ID: NodeID(id), Addr: string(addr)})
		}
	}()
	return e, err
}

// writeFileAtomic writes data to path via a temp file, fsyncing the file
// and the directory so the rename itself is durable.
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
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
