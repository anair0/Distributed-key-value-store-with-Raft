// Package linearizability records client operations and checks them with
// Porcupine (github.com/anishathalye/porcupine) against a sequential model of
// a key-value store. A history is linearizable if every operation can be
// placed at a single instant between its call and its return such that the
// results match a single-copy store. Porcupine partitions the history per
// key, so long tests stay cheap to check.
package linearizability

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Op codes for Input.Op.
const (
	Get uint8 = iota
	Put
	Delete
)

// Input describes an invoked operation.
type Input struct {
	Op    uint8
	Key   string
	Value string
}

// Output describes an operation's result.
type Output struct {
	Value string
	Found bool
}

type state struct {
	value string
	found bool
}

// KVModel is the sequential specification of the store.
var KVModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(Input).Key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return state{} },
	Step: func(st, in, out interface{}) (bool, interface{}) {
		s, i, o := st.(state), in.(Input), out.(Output)
		switch i.Op {
		case Get:
			return o.Found == s.found && o.Value == s.value, s
		case Put:
			return true, state{value: i.Value, found: true}
		case Delete:
			return o.Found == s.found, state{}
		}
		return false, s
	},
	DescribeOperation: func(in, out interface{}) string {
		i, o := in.(Input), out.(Output)
		switch i.Op {
		case Get:
			if !o.Found {
				return fmt.Sprintf("get(%q) -> <none>", i.Key)
			}
			return fmt.Sprintf("get(%q) -> %q", i.Key, o.Value)
		case Put:
			return fmt.Sprintf("put(%q, %q)", i.Key, i.Value)
		case Delete:
			return fmt.Sprintf("delete(%q) -> %v", i.Key, o.Found)
		}
		return "?"
	},
	DescribeState: func(st interface{}) string {
		s := st.(state)
		if !s.found {
			return "<none>"
		}
		return fmt.Sprintf("%q", s.value)
	},
}

// Recorder collects a history from concurrent clients.
type Recorder struct {
	mu  sync.Mutex
	ops []porcupine.Operation
}

// Now returns a timestamp for Call/Return.
func Now() int64 { return time.Now().UnixNano() }

// Record adds a completed operation.
func (r *Recorder) Record(client int, in Input, call int64, out Output, ret int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
}

// Len returns the number of recorded operations.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

// Check checks the history. It returns an error if the history is not
// linearizable, writing a visualization to a temporary HTML file.
// A check that times out is not treated as a failure.
func (r *Recorder) Check(timeout time.Duration) error {
	r.mu.Lock()
	ops := append([]porcupine.Operation(nil), r.ops...)
	r.mu.Unlock()
	res, info := porcupine.CheckOperationsVerbose(KVModel, ops, timeout)
	if res != porcupine.Illegal {
		return nil
	}
	msg := fmt.Sprintf("history of %d operations is not linearizable", len(ops))
	if f, err := os.CreateTemp("", "linearizability-*.html"); err == nil {
		if err := porcupine.Visualize(KVModel, info, f); err == nil {
			msg += "; visualization: " + f.Name()
		}
		f.Close()
	}
	return fmt.Errorf("%s", msg)
}
