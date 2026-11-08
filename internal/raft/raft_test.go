package raft

import "testing"

func TestNewStartsAsFollower(t *testing.T) {
	r := New(Config{ID: 0, Peers: []NodeID{0, 1, 2}}, nil, nil, make(chan ApplyMsg, 1))
	term, leader := r.State()
	if term != 0 || leader {
		t.Fatalf("got term=%d leader=%v, want 0,false", term, leader)
	}
}
