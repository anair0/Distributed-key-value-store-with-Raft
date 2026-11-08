package lsm

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func TestSkiplistSorted(t *testing.T) {
	s := newSkiplist()
	want := map[string]string{}
	for range 2000 {
		k := fmt.Sprintf("k%04d", rand.Intn(1000))
		v := fmt.Sprint(rand.Int())
		s.put(k, []byte(v), kindPut)
		want[k] = v
	}
	es := s.entries()
	if len(es) != len(want) {
		t.Fatalf("got %d entries, want %d", len(es), len(want))
	}
	if !sort.SliceIsSorted(es, func(i, j int) bool { return es[i].key < es[j].key }) {
		t.Fatal("entries not sorted")
	}
	for k, v := range want {
		e, ok := s.get(k)
		if !ok || string(e.value) != v {
			t.Fatalf("get(%s) = %q,%v want %q", k, e.value, ok, v)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	var hs []uint64
	for i := range 10000 {
		hs = append(hs, keyHash(fmt.Sprintf("in-%d", i)))
	}
	b := newBloom(hs, 10)
	for _, h := range hs {
		if !b.mayContain(h) {
			t.Fatal("false negative")
		}
	}
	fp := 0
	for i := range 10000 {
		if b.mayContain(keyHash(fmt.Sprintf("out-%d", i))) {
			fp++
		}
	}
	if rate := float64(fp) / 10000; rate > 0.02 {
		t.Fatalf("false positive rate %.3f > 2%%", rate)
	}
}

func TestSSTableRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db := &DB{dir: dir, opts: Options{}.withDefaults(), man: manifest{NextFile: 1}}
	var es []entry
	for i := range 5000 {
		kind := kindPut
		if i%7 == 0 {
			kind = kindDelete
		}
		es = append(es, entry{key: fmt.Sprintf("key-%06d", i), value: []byte(fmt.Sprintf("v%d", i)), kind: kind})
	}
	tb, err := db.writeTable(es, false)
	if err != nil {
		t.Fatal(err)
	}
	defer tb.unref()
	if len(tb.index) < 10 {
		t.Fatalf("expected many blocks, got %d", len(tb.index))
	}
	for _, e := range es {
		got, ok, err := tb.get(e.key, keyHash(e.key))
		if err != nil || !ok || got.kind != e.kind || string(got.value) != string(e.value) {
			t.Fatalf("get(%s) = %+v %v %v", e.key, got, ok, err)
		}
	}
	for _, k := range []string{"a", "key-", "key-0000005x", "zzz"} {
		if _, ok, _ := tb.get(k, keyHash(k)); ok {
			t.Fatalf("found missing key %q", k)
		}
	}
	i := 0
	for it := tb.iter(); it.valid(); it.next() {
		if it.current().key != es[i].key {
			t.Fatalf("iter[%d] = %s want %s", i, it.current().key, es[i].key)
		}
		i++
	}
	if i != len(es) {
		t.Fatalf("iterated %d of %d", i, len(es))
	}
}

func smallOpts() Options {
	return Options{MemtableSize: 4 << 10, CompactionTrigger: 3, BlockSize: 256}
}

func checkAgainst(t *testing.T, db *DB, model map[string]string) {
	t.Helper()
	for k, v := range model {
		got, ok, err := db.Get(k)
		if err != nil || !ok || string(got) != v {
			t.Fatalf("get(%s) = %q,%v,%v want %q", k, got, ok, err, v)
		}
	}
	var keys []string
	if err := db.Scan(func(k string, v []byte) bool {
		if model[k] != string(v) {
			t.Fatalf("scan %s = %q want %q", k, v, model[k])
		}
		keys = append(keys, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != len(model) {
		t.Fatalf("scan returned %d keys, want %d", len(keys), len(model))
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatal("scan out of order")
	}
}

// TestRandomAgainstMap drives the DB with random puts and deletes, closing
// and reopening it periodically, and compares it with a plain map.
func TestRandomAgainstMap(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	model := map[string]string{}
	for round := range 10 {
		for i := range 1000 {
			k := fmt.Sprintf("k%03d", rand.Intn(300))
			if rand.Intn(4) == 0 {
				if err := db.Delete(k); err != nil {
					t.Fatal(err)
				}
				delete(model, k)
			} else {
				v := fmt.Sprintf("r%d-%d", round, i)
				if err := db.Put(k, []byte(v)); err != nil {
					t.Fatal(err)
				}
				model[k] = v
			}
		}
		checkAgainst(t, db, model)
		if round%3 == 2 {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if db, err = Open(dir, smallOpts()); err != nil {
				t.Fatal(err)
			}
			checkAgainst(t, db, model)
		}
	}
	st := db.Stats()
	if st.Flushes == 0 || st.Compactions == 0 {
		t.Fatalf("expected flushes and compactions, got %+v", st)
	}
	db.Close()
}

// crash abandons the DB without a clean Close, like a killed process: the
// OS has everything written so far, but nothing is flushed or synced.
func crash(db *DB) {
	db.mu.Lock()
	db.waitIdleLocked()
	db.closed = true
	db.wal.f.Close()
	for _, tb := range db.tables {
		tb.f.Close()
	}
	db.mu.Unlock()
}

func TestCrashRecoveryAndTornWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	model := map[string]string{}
	for i := range 500 {
		k, v := fmt.Sprintf("k%d", i%100), fmt.Sprint(i)
		db.Put(k, []byte(v))
		model[k] = v
	}
	// An atomic batch.
	db.Write([]Op{{Key: "b1", Value: []byte("x")}, {Key: "b2", Value: []byte("y")}, {Key: "k1", Delete: true}})
	model["b1"], model["b2"] = "x", "y"
	delete(model, "k1")
	walPath := filepath.Join(dir, walName(db.walNum))
	crash(db)

	// A torn record at the end of the WAL must be ignored.
	f, _ := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{1, 2, 3, 4, 50, 0, 0, 0, 9})
	f.Close()

	db, err = Open(dir, smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checkAgainst(t, db, model)
}

func TestCompactionDropsTombstones(t *testing.T) {
	db, err := Open(t.TempDir(), smallOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := range 2000 {
		db.Put(fmt.Sprintf("k%05d", i), make([]byte, 100))
	}
	for i := range 2000 {
		db.Delete(fmt.Sprintf("k%05d", i))
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	checkAgainst(t, db, map[string]string{})
	db.mu.Lock()
	var size int64
	for _, tb := range db.tables {
		size += tb.size
	}
	db.mu.Unlock()
	if size > 4096 {
		t.Fatalf("tombstones and dead values not dropped: %d bytes of tables remain", size)
	}
}

func TestReset(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, smallOpts())
	for i := range 1000 {
		db.Put(fmt.Sprint(i), []byte("v"))
	}
	if err := db.Reset(); err != nil {
		t.Fatal(err)
	}
	db.Put("after", []byte("1"))
	checkAgainst(t, db, map[string]string{"after": "1"})
	db.Close()
	db, _ = Open(dir, smallOpts())
	defer db.Close()
	checkAgainst(t, db, map[string]string{"after": "1"})
}

func TestConcurrentReadersWriters(t *testing.T) {
	db, _ := Open(t.TempDir(), smallOpts())
	defer db.Close()
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				k := fmt.Sprintf("w%d-%d", w, i%50)
				if err := db.Put(k, []byte(fmt.Sprint(i))); err != nil {
					t.Error(err)
					return
				}
				if _, _, err := db.Get(k); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 50 {
			if err := db.Scan(func(string, []byte) bool { return true }); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	for w := range 4 {
		for i := 950; i < 1000; i++ {
			v, ok, _ := db.Get(fmt.Sprintf("w%d-%d", w, i%50))
			if !ok || string(v) != fmt.Sprint(i) {
				t.Fatalf("lost write w%d-%d: %q", w, i%50, v)
			}
		}
	}
}

func BenchmarkPut(b *testing.B) {
	db, _ := Open(b.TempDir(), Options{})
	defer db.Close()
	v := make([]byte, 100)
	for i := 0; b.Loop(); i++ {
		db.Put(fmt.Sprintf("key-%010d", i), v)
	}
}

func BenchmarkGet(b *testing.B) {
	db, _ := Open(b.TempDir(), Options{MemtableSize: 64 << 10})
	defer db.Close()
	for i := range 100000 {
		db.Put(fmt.Sprintf("key-%010d", i), make([]byte, 100))
	}
	db.Flush()
	for i := 0; b.Loop(); i++ {
		db.Get(fmt.Sprintf("key-%010d", i%200000)) // half are misses
	}
}
