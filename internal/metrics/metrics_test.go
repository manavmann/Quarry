package metrics

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func body(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw)
}

func TestServerExposesEverySeries(t *testing.T) {
	m := NewServer()
	m.JobsTotal.WithLabelValues("queued").Add(2)
	m.JobsTotal.WithLabelValues("succeeded").Inc()
	m.JobDuration.Observe(3)
	m.ClaimLatency.Observe(0.5)
	m.LeaseExpirations.Inc()
	m.LogBytes.Add(1024)
	m.SetSnapshot(func(context.Context) (Snapshot, error) {
		return Snapshot{
			QueueDepth: map[string]int{"os=linux,pool=build": 2, "": 1},
			Runners:    map[string]int{"online": 3, "offline": 1},
		}, nil
	})
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	out := body(t, srv)
	for _, want := range []string{
		`quarry_jobs_total{state="queued"} 2`,
		`quarry_jobs_total{state="succeeded"} 1`,
		`quarry_job_duration_seconds_count 1`,
		`quarry_claim_latency_seconds_count 1`,
		`quarry_lease_expirations_total 1`,
		`quarry_log_bytes_total 1024`,
		`quarry_queue_depth{labels="os=linux,pool=build"} 2`,
		`quarry_queue_depth{labels=""} 1`,
		`quarry_runners{state="online"} 3`,
		`quarry_runners{state="offline"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// A failed snapshot leaves the two gauges out rather than exporting stale
// or zero values; the counters are unaffected.
func TestServerSnapshotErrorOmitsGauges(t *testing.T) {
	m := NewServer()
	m.LeaseExpirations.Inc()
	m.SetSnapshot(func(context.Context) (Snapshot, error) { return Snapshot{}, errors.New("db down") })
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	out := body(t, srv)
	if strings.Contains(out, "quarry_queue_depth{") || strings.Contains(out, "quarry_runners{") {
		t.Fatalf("gauges exported on snapshot error:\n%s", out)
	}
	if !strings.Contains(out, "quarry_lease_expirations_total 1") {
		t.Fatalf("counter missing:\n%s", out)
	}
}

func TestRunnerExposesEverySeries(t *testing.T) {
	m := NewRunner()
	m.ActiveJobs.Set(2)
	m.ExecutorErrors.Inc()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	out := body(t, srv)
	for _, want := range []string{"quarry_runner_active_jobs 2", "quarry_runner_executor_errors_total 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestLabelKey(t *testing.T) {
	for _, tc := range []struct {
		in   map[string]string
		want string
	}{
		{nil, ""},
		{map[string]string{"os": "linux"}, "os=linux"},
		{map[string]string{"pool": "build", "os": "linux"}, "os=linux,pool=build"},
	} {
		if got := LabelKey(tc.in); got != tc.want {
			t.Errorf("LabelKey(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
