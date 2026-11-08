package raft

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func entries(from, to, term uint64) []LogEntry {
	var es []LogEntry
	for i := from; i <= to; i++ {
		es = append(es, LogEntry{Index: i, Term: term, Command: []byte{byte(i)}})
	}
	return es
}

func reopen(t *testing.T, dir string) (*FilePersister, PersistentState, []byte) {
	t.Helper()
	p, err := OpenFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, ok, err := p.LoadState()
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	snap, err := p.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	return p, st, snap
}

func TestFilePersisterAppendTruncateSnapshot(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenFilePersister(dir)
	if _, ok, _ := p.LoadState(); ok {
		t.Fatal("fresh dir reported saved state")
	}
	sentinel := LogEntry{Kind: EntryConfig, Members: []Member{{ID: 1, Addr: "a:1"}}}

	// Append path.
	log := append([]LogEntry{sentinel}, entries(1, 5, 1)...)
	if err := p.SaveState(PersistentState{CurrentTerm: 1, VotedFor: 1, Log: log}); err != nil {
		t.Fatal(err)
	}
	log = append(log, entries(6, 8, 2)...)
	if err := p.SaveState(PersistentState{CurrentTerm: 2, VotedFor: None, Log: log}); err != nil {
		t.Fatal(err)
	}
	p.Close()
	p, st, _ := reopen(t, dir)
	if st.CurrentTerm != 2 || st.VotedFor != None || !reflect.DeepEqual(st.Log, log) {
		t.Fatalf("after append: got %+v", st)
	}

	// Truncation (conflict at index 7) forces a rewrite.
	log = append(log[:7:7], entries(7, 7, 3)...)
	if err := p.SaveState(PersistentState{CurrentTerm: 3, VotedFor: 2, Log: log}); err != nil {
		t.Fatal(err)
	}
	p.Close()
	p, st, _ = reopen(t, dir)
	if !reflect.DeepEqual(st.Log, log) || st.VotedFor != 2 {
		t.Fatalf("after truncate: got %+v", st.Log)
	}

	// Snapshot at 5.
	snapLog := append([]LogEntry{{Index: 5, Term: 1, Kind: EntryConfig, Members: sentinel.Members}}, log[6:]...)
	if err := p.SaveStateAndSnapshot(PersistentState{CurrentTerm: 3, VotedFor: 2, Log: snapLog}, []byte("snap")); err != nil {
		t.Fatal(err)
	}
	p.Close()
	_, st, snap := reopen(t, dir)
	if !reflect.DeepEqual(st.Log, snapLog) || string(snap) != "snap" {
		t.Fatalf("after snapshot: got %+v %q", st.Log, snap)
	}
}

func TestFilePersisterTornTail(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenFilePersister(dir)
	log := append([]LogEntry{{Kind: EntryConfig}}, entries(1, 3, 1)...)
	if err := p.SaveState(PersistentState{CurrentTerm: 1, VotedFor: None, Log: log}); err != nil {
		t.Fatal(err)
	}
	p.Close()
	// Simulate a crash in the middle of appending a record.
	f, _ := os.OpenFile(filepath.Join(dir, "log"), os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{20, 0, 0, 0, 1, 2, 3})
	f.Close()

	p, st, _ := reopen(t, dir)
	if !reflect.DeepEqual(st.Log, log) {
		t.Fatalf("torn tail not discarded: %+v", st.Log)
	}
	// Appending after recovery works.
	log = append(log, entries(4, 4, 1)...)
	if err := p.SaveState(PersistentState{CurrentTerm: 1, VotedFor: None, Log: log}); err != nil {
		t.Fatal(err)
	}
	p.Close()
	_, st, _ = reopen(t, dir)
	if !reflect.DeepEqual(st.Log, log) {
		t.Fatalf("append after recovery: %+v", st.Log)
	}
}

func TestFilePersisterSnapshotCrashBeforeLogRewrite(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenFilePersister(dir)
	log := append([]LogEntry{{Kind: EntryConfig}}, entries(1, 6, 1)...)
	p.SaveState(PersistentState{CurrentTerm: 1, VotedFor: None, Log: log})
	// Write only the snapshot file, as if we crashed before rewriting the log.
	snapLog := append([]LogEntry{{Index: 4, Term: 1, Kind: EntryConfig}}, log[5:]...)
	// Emulate by saving to a scratch dir and copying the snapshot file over.
	scratch := t.TempDir()
	ps, _ := OpenFilePersister(scratch)
	ps.SaveStateAndSnapshot(PersistentState{CurrentTerm: 1, VotedFor: None, Log: snapLog}, []byte("S"))
	ps.Close()
	b, _ := os.ReadFile(filepath.Join(scratch, "snapshot"))
	os.WriteFile(filepath.Join(dir, "snapshot"), b, 0o644)
	p.Close()

	_, st, snap := reopen(t, dir)
	if !reflect.DeepEqual(st.Log, snapLog) || string(snap) != "S" {
		t.Fatalf("not reconciled: %+v", st.Log)
	}
}
