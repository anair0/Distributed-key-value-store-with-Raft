package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"sync/atomic"
)

// SSTable file layout:
//
//	data block 0 | crc32 | data block 1 | crc32 | ... | index block | bloom filter | footer
//
//	data block entry: kind u8, key len uvarint, value len uvarint, key, value
//	index entry:      last key len uvarint, last key, block offset uvarint, block size uvarint
//	footer (48 bytes): index off, index len, bloom off, bloom len, entry count, magic (all u64 LE)
//
// Keys inside a table are sorted and unique. The index and the bloom filter
// are loaded into memory when the table is opened, so a point lookup costs a
// bloom check, a binary search over block boundaries, and at most one block
// read from disk.

const (
	footerSize = 48
	tableMagic = 0x4c534d5441424c45 // "LSMTABLE"
)

type blockHandle struct {
	lastKey string
	offset  uint64
	size    uint64
}

// tableWriter writes entries (in strictly increasing key order) to a file.
type tableWriter struct {
	f          *os.File
	w          *bufio.Writer
	off        uint64
	block      []byte
	blockKey   string // last key added to the current block
	index      []blockHandle
	hashes     []uint64
	blockSize  int
	bitsPerKey int
	count      uint64
}

func newTableWriter(path string, blockSize, bitsPerKey int) (*tableWriter, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &tableWriter{f: f, w: bufio.NewWriterSize(f, 256<<10), blockSize: blockSize, bitsPerKey: bitsPerKey}, nil
}

func (tw *tableWriter) add(e entry) error {
	tw.block = append(tw.block, e.kind)
	tw.block = binary.AppendUvarint(tw.block, uint64(len(e.key)))
	tw.block = binary.AppendUvarint(tw.block, uint64(len(e.value)))
	tw.block = append(tw.block, e.key...)
	tw.block = append(tw.block, e.value...)
	tw.blockKey = e.key
	tw.hashes = append(tw.hashes, keyHash(e.key))
	tw.count++
	if len(tw.block) >= tw.blockSize {
		return tw.flushBlock()
	}
	return nil
}

func (tw *tableWriter) write(b []byte) error {
	_, err := tw.w.Write(b)
	tw.off += uint64(len(b))
	return err
}

func (tw *tableWriter) flushBlock() error {
	if len(tw.block) == 0 {
		return nil
	}
	tw.index = append(tw.index, blockHandle{lastKey: tw.blockKey, offset: tw.off, size: uint64(len(tw.block))})
	if err := tw.write(tw.block); err != nil {
		return err
	}
	if err := tw.write(binary.LittleEndian.AppendUint32(nil, crc32.ChecksumIEEE(tw.block))); err != nil {
		return err
	}
	tw.block = tw.block[:0]
	return nil
}

// finish writes the index, bloom filter and footer and fsyncs the file.
func (tw *tableWriter) finish() error {
	if err := tw.flushBlock(); err != nil {
		return err
	}
	var idx []byte
	for _, h := range tw.index {
		idx = binary.AppendUvarint(idx, uint64(len(h.lastKey)))
		idx = append(idx, h.lastKey...)
		idx = binary.AppendUvarint(idx, h.offset)
		idx = binary.AppendUvarint(idx, h.size)
	}
	indexOff := tw.off
	if err := tw.write(idx); err != nil {
		return err
	}
	bl := newBloom(tw.hashes, tw.bitsPerKey)
	bloomOff := tw.off
	if err := tw.write(bl); err != nil {
		return err
	}
	var footer []byte
	for _, v := range []uint64{indexOff, uint64(len(idx)), bloomOff, uint64(len(bl)), tw.count, tableMagic} {
		footer = binary.LittleEndian.AppendUint64(footer, v)
	}
	if err := tw.write(footer); err != nil {
		return err
	}
	if err := tw.w.Flush(); err != nil {
		return err
	}
	if err := tw.f.Sync(); err != nil {
		return err
	}
	return tw.f.Close()
}

func (tw *tableWriter) abort() {
	tw.f.Close()
	os.Remove(tw.f.Name())
}

// table is an open, immutable SSTable. It is reference counted: the DB holds
// one reference while the table is live, and each reader holds one while it
// uses the table. When a compaction makes the table obsolete, its file is
// deleted once the last reader is done.
type table struct {
	num      uint64
	path     string
	f        *os.File
	index    []blockHandle
	bloom    bloom
	count    uint64
	size     int64
	refs     atomic.Int32
	obsolete atomic.Bool
}

var errCorruptTable = errors.New("lsm: corrupt sstable")

func openTable(path string, num uint64) (*table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	t := &table{num: num, path: path, f: f, size: fi.Size()}
	t.refs.Store(1)
	if err := t.load(); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return t, nil
}

func (t *table) load() error {
	if t.size < footerSize {
		return errCorruptTable
	}
	footer := make([]byte, footerSize)
	if _, err := t.f.ReadAt(footer, t.size-footerSize); err != nil {
		return err
	}
	u := func(i int) uint64 { return binary.LittleEndian.Uint64(footer[8*i:]) }
	if u(5) != tableMagic {
		return errCorruptTable
	}
	indexOff, indexLen, bloomOff, bloomLen := u(0), u(1), u(2), u(3)
	t.count = u(4)
	if indexOff+indexLen > uint64(t.size) || bloomOff+bloomLen > uint64(t.size) {
		return errCorruptTable
	}
	idx := make([]byte, indexLen)
	if _, err := t.f.ReadAt(idx, int64(indexOff)); err != nil {
		return err
	}
	for len(idx) > 0 {
		kl, n := binary.Uvarint(idx)
		if n <= 0 || uint64(len(idx)-n) < kl {
			return errCorruptTable
		}
		key := string(idx[n : n+int(kl)])
		idx = idx[n+int(kl):]
		off, n := binary.Uvarint(idx)
		if n <= 0 {
			return errCorruptTable
		}
		idx = idx[n:]
		sz, n := binary.Uvarint(idx)
		if n <= 0 {
			return errCorruptTable
		}
		idx = idx[n:]
		t.index = append(t.index, blockHandle{lastKey: key, offset: off, size: sz})
	}
	t.bloom = make(bloom, bloomLen)
	_, err := t.f.ReadAt(t.bloom, int64(bloomOff))
	return err
}

func (t *table) ref() { t.refs.Add(1) }

func (t *table) unref() {
	if t.refs.Add(-1) == 0 {
		t.f.Close()
		if t.obsolete.Load() {
			os.Remove(t.path)
		}
	}
}

// readBlock reads and checksums block i, returning its decoded entries.
func (t *table) readBlock(i int) ([]entry, error) {
	h := t.index[i]
	buf := make([]byte, h.size+4)
	if _, err := t.f.ReadAt(buf, int64(h.offset)); err != nil {
		return nil, err
	}
	data := buf[:h.size]
	if crc32.ChecksumIEEE(data) != binary.LittleEndian.Uint32(buf[h.size:]) {
		return nil, fmt.Errorf("%s block %d: %w", t.path, i, errCorruptTable)
	}
	var out []entry
	for len(data) > 0 {
		kind := data[0]
		data = data[1:]
		kl, n := binary.Uvarint(data)
		if n <= 0 {
			return nil, errCorruptTable
		}
		data = data[n:]
		vl, n := binary.Uvarint(data)
		if n <= 0 || uint64(len(data)-n) < kl+vl {
			return nil, errCorruptTable
		}
		data = data[n:]
		e := entry{key: string(data[:kl]), kind: kind}
		e.value = append([]byte(nil), data[kl:kl+vl]...)
		data = data[kl+vl:]
		out = append(out, e)
	}
	return out, nil
}

// get looks key up in the table.
func (t *table) get(key string, h uint64) (entry, bool, error) {
	if !t.bloom.mayContain(h) {
		return entry{}, false, nil
	}
	// First block whose last key is >= key.
	i := sort.Search(len(t.index), func(i int) bool { return t.index[i].lastKey >= key })
	if i == len(t.index) {
		return entry{}, false, nil
	}
	es, err := t.readBlock(i)
	if err != nil {
		return entry{}, false, err
	}
	j := sort.Search(len(es), func(j int) bool { return es[j].key >= key })
	if j < len(es) && es[j].key == key {
		return es[j], true, nil
	}
	return entry{}, false, nil
}

// tableIter iterates over a table block by block.
type tableIter struct {
	t     *table
	block int
	es    []entry
	pos   int
	err   error
}

func (t *table) iter() *tableIter {
	it := &tableIter{t: t, block: -1}
	it.nextBlock()
	return it
}

func (it *tableIter) nextBlock() {
	for {
		it.block++
		if it.block >= len(it.t.index) {
			it.es = nil
			return
		}
		it.es, it.err = it.t.readBlock(it.block)
		it.pos = 0
		if it.err != nil {
			it.es = nil
			return
		}
		if len(it.es) > 0 {
			return
		}
	}
}

func (it *tableIter) valid() bool    { return it.pos < len(it.es) }
func (it *tableIter) current() entry { return it.es[it.pos] }
func (it *tableIter) next() {
	it.pos++
	if it.pos >= len(it.es) {
		it.nextBlock()
	}
}
func (it *tableIter) error() error { return it.err }
