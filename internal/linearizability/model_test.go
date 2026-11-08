package linearizability

import "testing"

func TestCheckerDetectsStaleRead(t *testing.T) {
	var r Recorder
	// put(x,1) completes, then a later get(x) returns nothing: illegal.
	r.Record(0, Input{Op: Put, Key: "x", Value: "1"}, 0, Output{}, 10)
	r.Record(1, Input{Op: Get, Key: "x"}, 20, Output{}, 30)
	if err := r.Check(0); err == nil {
		t.Fatal("expected a linearizability violation")
	}
}

func TestCheckerAcceptsConcurrent(t *testing.T) {
	var r Recorder
	// Concurrent put and get: either outcome is fine.
	r.Record(0, Input{Op: Put, Key: "x", Value: "1"}, 0, Output{}, 100)
	r.Record(1, Input{Op: Get, Key: "x"}, 10, Output{}, 20)
	r.Record(2, Input{Op: Get, Key: "x"}, 30, Output{Value: "1", Found: true}, 40)
	r.Record(3, Input{Op: Delete, Key: "x"}, 110, Output{Found: true}, 120)
	if err := r.Check(0); err != nil {
		t.Fatal(err)
	}
}
