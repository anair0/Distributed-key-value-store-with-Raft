package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// The write-ahead log makes memtable contents survive a crash. Each Write
// batch is one record:
//
//	[crc32 u32][len u32][payload]
//	payload = count uvarint, then per op: kind u8, key len uvarint, key, value len uvarint, value
//
// A batch is atomic: on replay a torn or corrupt record (a write that was in
// progress during the crash) ends the log, so either the whole batch or
// none of it is recovered.
type wal struct {
	f    *os.File
	w    *bufio.Writer
	sync bool
}

func createWAL(path string, sync bool) (*wal, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{f: f, w: bufio.NewWriterSize(f, 64<<10), sync: sync}, nil
}

func encodeBatch(ops []Op) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(ops)))
	for _, op := range ops {
		kind := kindPut
		if op.Delete {
			kind = kindDelete
		}
		buf = append(buf, kind)
		buf = binary.AppendUvarint(buf, uint64(len(op.Key)))
		buf = append(buf, op.Key...)
		buf = binary.AppendUvarint(buf, uint64(len(op.Value)))
		buf = append(buf, op.Value...)
	}
	return buf
}

func (w *wal) append(ops []Op) error {
	payload := encodeBatch(ops)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(payload)))
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.w.Write(payload); err != nil {
		return err
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func (w *wal) close() error {
	if err := w.w.Flush(); err != nil {
		w.f.Close()
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

var errBadBatch = errors.New("lsm: corrupt batch")

func decodeBatch(b []byte) ([]entry, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return nil, errBadBatch
	}
	b = b[k:]
	out := make([]entry, 0, n)
	for range n {
		if len(b) < 1 {
			return nil, errBadBatch
		}
		kind := b[0]
		b = b[1:]
		kl, k := binary.Uvarint(b)
		if k <= 0 || uint64(len(b)-k) < kl {
			return nil, errBadBatch
		}
		key := string(b[k : k+int(kl)])
		b = b[k+int(kl):]
		vl, k := binary.Uvarint(b)
		if k <= 0 || uint64(len(b)-k) < vl {
			return nil, errBadBatch
		}
		val := append([]byte(nil), b[k:k+int(vl)]...)
		b = b[k+int(vl):]
		out = append(out, entry{key: key, value: val, kind: kind})
	}
	return out, nil
}

// replayWAL calls fn for every intact batch in the log at path.
func replayWAL(path string, fn func([]entry)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil // clean end or torn header
		}
		payload := make([]byte, binary.LittleEndian.Uint32(hdr[4:]))
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil // torn record
		}
		if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(hdr[0:]) {
			return nil // corrupt record ends the log
		}
		batch, err := decodeBatch(payload)
		if err != nil {
			return nil
		}
		fn(batch)
	}
}
