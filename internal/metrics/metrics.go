// Package metrics holds the Prometheus registries of the two binaries
// (blueprint §18). Each set lives on its own registry so tests can build
// any number of servers or agents without duplicate-registration panics,
// and Handler serves it in the exposition format.
//
// The server set is updated by the scheduler at every state transition;
// its two gauges (queue depth, runners by state) are read from the store
// at scrape time through SetSnapshot, so they are exact rather than
// tracked. The runner set is updated by the agent.
package metrics

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is the control plane's metric set.
type Server struct {
	reg *prometheus.Registry

	// JobsTotal counts state transitions by the state entered: queued,
	// running, succeeded, failed, cancelled, skipped.
	JobsTotal *prometheus.CounterVec
	// JobDuration is started→finished of every terminal attempt.
	JobDuration prometheus.Histogram
	// ClaimLatency is queued→running of every claim.
	ClaimLatency prometheus.Histogram
	// LeaseExpirations counts leases the monitor found expired.
	LeaseExpirations prometheus.Counter
	// LogBytes counts log bytes stored (after the per-attempt cap).
	LogBytes prometheus.Counter

	queueDepth *prometheus.Desc
	runners    *prometheus.Desc
	snapshot   func(context.Context) (Snapshot, error)
}

// Snapshot is what the two scrape-time gauges read from the store.
type Snapshot struct {
	// QueueDepth is queued jobs by their required labels, keyed as
	// LabelKey renders them.
	QueueDepth map[string]int
	// Runners is runners by state.
	Runners map[string]int
}

// LabelKey renders a job's required labels as the queue_depth label
// value: "k=v,k=v" in key order, "" for a job with no requirements.
func LabelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+labels[k])
	}
	return strings.Join(parts, ",")
}

// NewServer builds a server set on a fresh registry.
func NewServer() *Server {
	m := &Server{
		reg: prometheus.NewRegistry(),
		JobsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quarry_jobs_total", Help: "Job state transitions by the state entered.",
		}, []string{"state"}),
		JobDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "quarry_job_duration_seconds", Help: "Attempt duration from claim to terminal result.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1s .. ~34m
		}),
		ClaimLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "quarry_claim_latency_seconds", Help: "Time a job waited in the queue before a runner claimed it.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 12), // 100ms .. ~3.4m
		}),
		LeaseExpirations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "quarry_lease_expirations_total", Help: "Running attempts whose lease the monitor found expired.",
		}),
		LogBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "quarry_log_bytes_total", Help: "Log bytes stored across all attempts.",
		}),
		queueDepth: prometheus.NewDesc("quarry_queue_depth", "Queued jobs by required labels.", []string{"labels"}, nil),
		runners:    prometheus.NewDesc("quarry_runners", "Registered runners by state.", []string{"state"}, nil),
	}
	m.reg.MustRegister(m.JobsTotal, m.JobDuration, m.ClaimLatency, m.LeaseExpirations, m.LogBytes, m)
	return m
}

// SetSnapshot installs the store reader behind the queue_depth and runners
// gauges. Until it is set, neither gauge is exported.
func (m *Server) SetSnapshot(fn func(context.Context) (Snapshot, error)) { m.snapshot = fn }

// Handler serves the registry.
func (m *Server) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Describe implements prometheus.Collector for the snapshot gauges.
func (m *Server) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.queueDepth
	ch <- m.runners
}

// Collect implements prometheus.Collector: one store read per scrape. A
// failed read exports nothing for the two gauges rather than stale values.
func (m *Server) Collect(ch chan<- prometheus.Metric) {
	if m.snapshot == nil {
		return
	}
	snap, err := m.snapshot(context.Background())
	if err != nil {
		return
	}
	for labels, n := range snap.QueueDepth {
		ch <- prometheus.MustNewConstMetric(m.queueDepth, prometheus.GaugeValue, float64(n), labels)
	}
	for state, n := range snap.Runners {
		ch <- prometheus.MustNewConstMetric(m.runners, prometheus.GaugeValue, float64(n), state)
	}
}

// Runner is the agent's metric set.
type Runner struct {
	reg *prometheus.Registry

	// ActiveJobs is the number of attempts currently executing.
	ActiveJobs prometheus.Gauge
	// ExecutorErrors counts attempts the executor failed to run (reported
	// as infra), as opposed to jobs that ran and exited non-zero.
	ExecutorErrors prometheus.Counter
}

// NewRunner builds a runner set on a fresh registry.
func NewRunner() *Runner {
	m := &Runner{
		reg: prometheus.NewRegistry(),
		ActiveJobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "quarry_runner_active_jobs", Help: "Attempts currently executing on this runner.",
		}),
		ExecutorErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "quarry_runner_executor_errors_total", Help: "Attempts the executor could not run (infra failures).",
		}),
	}
	m.reg.MustRegister(m.ActiveJobs, m.ExecutorErrors)
	return m
}

// Handler serves the registry.
func (m *Runner) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
