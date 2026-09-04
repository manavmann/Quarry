package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"quarry/internal/store"
)

// jobsTotal reads quarry_jobs_total{state}.
func jobsTotal(s *Scheduler, state string) float64 {
	return testutil.ToFloat64(s.Metrics().JobsTotal.WithLabelValues(state))
}

func snapshot(t *testing.T, s *Scheduler) (map[string]int, map[string]int) {
	t.Helper()
	snap, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return snap.QueueDepth, snap.Runners
}

// Every transition the scheduler makes is counted once, by the state
// entered, and the queue/runner gauges follow the store.
func TestMetricsTraceTransitions(t *testing.T) {
	s, st, clk := newScheduler(t, 30*time.Second)
	linux := map[string]string{"os": "linux"}
	dag(t, st, []string{"build", "test", "lint"}, map[string]spec{
		"build": {labels: linux}, "test": {needs: []string{"build"}}, "lint": {needs: []string{"build"}, labels: linux},
	})
	if depth, _ := snapshot(t, s); depth["os=linux"] != 1 || len(depth) != 1 {
		t.Fatalf("queue depth before claim = %v", depth)
	}

	clk.ms.Add(2000) // the build job waits ~2 s in the queue
	build := claim(t, s, "r1", linux)
	if got := jobsTotal(s, store.JobRunning); got != 1 {
		t.Fatalf("running = %v, want 1", got)
	}
	if n := testutil.CollectAndCount(s.Metrics().ClaimLatency); n != 1 {
		t.Fatalf("claim latency series = %d, want 1", n)
	}
	depth, runners := snapshot(t, s)
	if len(depth) != 0 || runners["online"] != 1 {
		t.Fatalf("after claim: depth %v runners %v", depth, runners)
	}

	clk.ms.Add(5000) // and runs ~5 s
	complete(t, s, build, ok)
	if got := jobsTotal(s, store.JobSucceeded); got != 1 {
		t.Fatalf("succeeded = %v, want 1", got)
	}
	// Two dependents entered queued on advancement, in two label groups.
	if got := jobsTotal(s, store.JobQueued); got != 2 {
		t.Fatalf("queued = %v, want 2", got)
	}
	if depth, _ := snapshot(t, s); depth["os=linux"] != 1 || depth[""] != 1 {
		t.Fatalf("queue depth after advance = %v", depth)
	}
	if n := testutil.CollectAndCount(s.Metrics().JobDuration); n != 1 {
		t.Fatalf("duration series = %d, want 1", n)
	}

	// A failing job skips its dependent: both are counted.
	s2, st2, _ := newScheduler(t, 30*time.Second)
	dag(t, st2, []string{"a", "b"}, map[string]spec{"a": {}, "b": {needs: []string{"a"}}})
	complete(t, s2, claim(t, s2, "r1", nil), Result{Status: store.JobFailed, FailureKind: store.FailureExitCode})
	if f, sk := jobsTotal(s2, store.JobFailed), jobsTotal(s2, store.JobSkipped); f != 1 || sk != 1 {
		t.Fatalf("failed = %v skipped = %v, want 1 and 1", f, sk)
	}

	// Cancelling a run counts every job it cancels.
	s3, st3, _ := newScheduler(t, 30*time.Second)
	runID, _ := dag(t, st3, []string{"a", "b"}, map[string]spec{"a": {}, "b": {needs: []string{"a"}}})
	if _, err := s3.CancelRun(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	if got := jobsTotal(s3, store.JobCancelled); got != 2 {
		t.Fatalf("cancelled = %v, want 2", got)
	}
}

// An expired lease is counted once, whether it requeues or fails the job.
func TestMetricsCountLeaseExpirations(t *testing.T) {
	const ttl = 30 * time.Second
	s, st, clk := newScheduler(t, ttl)
	dag(t, st, []string{"a"}, map[string]spec{"a": {maxAtt: 2}})
	claim(t, s, "r1", nil)
	expirations := func() float64 { return testutil.ToFloat64(s.Metrics().LeaseExpirations) }

	tick(t, s)
	if got := expirations(); got != 0 {
		t.Fatalf("before expiry = %v", got)
	}
	clk.ms.Add(ttl.Milliseconds())
	tick(t, s)
	if got := expirations(); got != 1 {
		t.Fatalf("after first expiry = %v, want 1", got)
	}
	if got := jobsTotal(s, store.JobQueued); got != 1 {
		t.Fatalf("requeued counted as queued = %v, want 1", got)
	}
	tick(t, s) // idempotent: nothing new
	if got := expirations(); got != 1 {
		t.Fatalf("idempotent tick = %v, want 1", got)
	}

	claim(t, s, "r2", nil) // attempt 2, the last one
	clk.ms.Add(ttl.Milliseconds())
	tick(t, s)
	if got := expirations(); got != 2 {
		t.Fatalf("after second expiry = %v, want 2", got)
	}
	if got := jobsTotal(s, store.JobFailed); got != 1 {
		t.Fatalf("lost_runner failure = %v, want 1", got)
	}
}

// Stored bytes are counted after the cap and after dedup, so a redelivered
// or truncated chunk never inflates the total.
func TestMetricsCountStoredLogBytes(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	s = New(st, Config{LogCapBytes: 10, Metrics: s.Metrics()})
	dag(t, st, []string{"a"}, map[string]spec{"a": {}})
	j := claim(t, s, "r1", nil)
	bytes := func() float64 { return testutil.ToFloat64(s.Metrics().LogBytes) }

	if err := appendLogs(t, s, j, chunk(1, "hello ")); err != nil {
		t.Fatal(err)
	}
	if got := bytes(); got != 6 {
		t.Fatalf("after first chunk = %v, want 6", got)
	}
	if err := appendLogs(t, s, j, chunk(1, "hello "), chunk(2, "world!!!")); err != nil {
		t.Fatal(err)
	}
	// seq 1 is a duplicate; seq 2 is cut from 8 to the 4 bytes of room left.
	if got := bytes(); got != 10 {
		t.Fatalf("after redelivery + cap = %v, want 10", got)
	}
	if err := appendLogs(t, s, j, chunk(3, "dropped")); err != nil {
		t.Fatal(err)
	}
	if got := bytes(); got != 10 {
		t.Fatalf("past cap = %v, want 10", got)
	}
}
