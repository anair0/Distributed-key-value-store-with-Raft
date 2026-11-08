// Package metrics exports Prometheus metrics for a node. Raft-level gauges
// are read from raft.Status at scrape time, so the Raft package itself has
// no Prometheus dependency.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/kv"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
)

// Metrics holds the collectors that are updated as events happen.
type Metrics struct {
	Requests *prometheus.CounterVec
	Latency  *prometheus.HistogramVec
}

// Register registers all metrics on reg. status is called at scrape time.
func Register(reg prometheus.Registerer, status func() raft.Status) *Metrics {
	gauge := func(name, help string, f func(raft.Status) float64) {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 { return f(status()) }))
	}
	counter := func(name, help string, f func(raft.Status) float64) {
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, func() float64 { return f(status()) }))
	}
	gauge("raft_term", "Current Raft term.", func(s raft.Status) float64 { return float64(s.Term) })
	gauge("raft_is_leader", "1 if this node is the leader.", func(s raft.Status) float64 {
		if s.Role == raft.Leader {
			return 1
		}
		return 0
	})
	gauge("raft_role", "0=follower 1=candidate 2=leader.", func(s raft.Status) float64 { return float64(s.Role) })
	gauge("raft_commit_index", "Highest committed log index.", func(s raft.Status) float64 { return float64(s.CommitIndex) })
	gauge("raft_last_applied", "Highest applied log index.", func(s raft.Status) float64 { return float64(s.LastApplied) })
	gauge("raft_last_log_index", "Index of the last log entry.", func(s raft.Status) float64 { return float64(s.LastLogIndex) })
	gauge("raft_snapshot_index", "Index covered by the latest snapshot.", func(s raft.Status) float64 { return float64(s.SnapshotIndex) })
	gauge("raft_commit_lag", "Log entries not yet committed (last log index - commit index).", func(s raft.Status) float64 {
		return float64(s.LastLogIndex - min(s.CommitIndex, s.LastLogIndex))
	})
	gauge("raft_apply_lag", "Committed entries not yet applied.", func(s raft.Status) float64 {
		return float64(s.CommitIndex - min(s.LastApplied, s.CommitIndex))
	})
	gauge("raft_members", "Number of voting members in the configuration.", func(s raft.Status) float64 { return float64(len(s.Members)) })
	counter("raft_elections_total", "Elections started by this node.", func(s raft.Status) float64 { return float64(s.Elections) })
	counter("raft_leader_changes_total", "Leader changes observed by this node.", func(s raft.Status) float64 { return float64(s.LeaderChanges) })

	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kv_requests_total", Help: "Client requests by operation and outcome.",
		}, []string{"op", "result"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kv_request_duration_seconds", Help: "Client request latency.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
		}, []string{"op"}),
	}
	reg.MustRegister(m.Requests, m.Latency)
	return m
}

// Observe records one request; pass it as kv.ServerConfig.OnRequest.
func (m *Metrics) Observe(op kv.OpType, d time.Duration, err kv.Err) {
	result := "ok"
	if err != kv.OK {
		result = string(err)
	}
	m.Requests.WithLabelValues(op.String(), result).Inc()
	m.Latency.WithLabelValues(op.String()).Observe(d.Seconds())
}
