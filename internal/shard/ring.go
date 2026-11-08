// Package shard splits the key space across several independent Raft groups
// so the system can scale past what one group (one leader) can handle.
//
// Keys are assigned to groups with consistent hashing: each group owns many
// points ("virtual nodes") on a 64-bit hash ring, and a key belongs to the
// first point at or after its own hash. Virtual nodes even out the load, and
// adding or removing a group moves only about 1/N of the keys.
//
// Limitations: the shard map is static configuration. Moving data when
// groups are added (shard migration, as in MIT 6.5840 Lab 5) and
// transactions across shards are out of scope. Linearizability holds per key,
// which is all a key-value API promises.
package shard

import (
	"fmt"
	"hash/fnv"
	"sort"
)

// Ring is a consistent-hash ring of group IDs.
type Ring struct {
	points []uint64
	owner  map[uint64]int
}

func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	// fnv is weak at mixing short, similar strings; finalize with a
	// splitmix64-style avalanche so ring points spread evenly.
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// NewRing places vnodes points per group on the ring.
func NewRing(groups []int, vnodes int) *Ring {
	r := &Ring{owner: map[uint64]int{}}
	for _, g := range groups {
		for v := range vnodes {
			p := hash(fmt.Sprintf("group-%d-vnode-%d", g, v))
			if _, taken := r.owner[p]; taken {
				continue // astronomically unlikely collision
			}
			r.owner[p] = g
			r.points = append(r.points, p)
		}
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
	return r
}

// Lookup returns the group that owns key.
func (r *Ring) Lookup(key string) int {
	h := hash(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i == len(r.points) {
		i = 0 // wrap around
	}
	return r.owner[r.points[i]]
}
