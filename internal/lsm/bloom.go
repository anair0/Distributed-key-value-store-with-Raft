package lsm

import (
	"hash/fnv"
	"math"
)

// bloom is a Bloom filter: a bit array plus k hash functions. mayContain
// never returns false for an added key; for other keys it returns true with
// probability about (1 - e^(-k*n/m))^k. At 10 bits per key and k = 7 that is
// under 1%, so most lookups for absent keys skip the SSTable entirely.
// Layout: bit array, then one byte holding k.
type bloom []byte

func keyHash(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func newBloom(hashes []uint64, bitsPerKey int) bloom {
	k := int(math.Round(float64(bitsPerKey) * math.Ln2)) // optimal k = (m/n) ln 2
	k = max(1, min(k, 30))
	nbits := max(len(hashes)*bitsPerKey, 64)
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8
	b := make(bloom, nbytes+1)
	b[nbytes] = byte(k)
	for _, h := range hashes {
		// Double hashing (Kirsch-Mitzenmacher): h_i = h1 + i*h2.
		h1, h2 := uint32(h), uint32(h>>32)|1
		for i := range k {
			bit := (h1 + uint32(i)*h2) % uint32(nbits)
			b[bit/8] |= 1 << (bit % 8)
		}
	}
	return b
}

func (b bloom) mayContain(h uint64) bool {
	if len(b) < 2 {
		return true
	}
	nbits := uint32(len(b)-1) * 8
	k := int(b[len(b)-1])
	h1, h2 := uint32(h), uint32(h>>32)|1
	for i := range k {
		bit := (h1 + uint32(i)*h2) % nbits
		if b[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}
