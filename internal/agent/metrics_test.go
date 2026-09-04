package agent

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"quarry/internal/executor"
	"quarry/internal/metrics"
)

// syncBuffer is a log sink the test can read while the agent writes.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// The active-jobs gauge follows attempts in flight, an executor error is
// counted once, and every attempt line carries the §18 identifiers.
func TestAgentMetricsAndLogFields(t *testing.T) {
	s := newStub(t)
	s.enqueue("hang", 0)
	s.enqueue("broken", 0)
	f := executor.NewFake()
	f.Script("hang", executor.Outcome{Hang: true})
	f.Script("broken", executor.Outcome{Err: executor.ErrInfra})
	m := metrics.NewRunner()
	var logs syncBuffer
	cfg := testConfig(s, 2)
	cfg.Metrics = m
	cfg.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	a, err := New(cfg, f)
	if err != nil {
		t.Fatal(err)
	}
	runAgent(t, a)
	active := func() float64 { return testutil.ToFloat64(m.ActiveJobs) }

	waitFor(t, func() bool { return len(s.completed()) == 1 }, "the broken job to report")
	waitFor(t, func() bool { return active() == 1 }, "one attempt in flight")
	if got := testutil.ToFloat64(m.ExecutorErrors); got != 1 {
		t.Fatalf("executor errors = %v, want 1", got)
	}
	f.Release("hang")
	waitFor(t, func() bool { return len(s.completed()) == 2 }, "the hanging job to report")
	waitFor(t, func() bool { return active() == 0 }, "no attempt in flight")
	if got := testutil.ToFloat64(m.ExecutorErrors); got != 1 {
		t.Fatalf("executor errors after a clean exit = %v, want still 1", got)
	}

	// The executor-error line names the attempt and the runner.
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("not JSON: %q", line)
		}
		if rec["msg"] != "executor error" {
			continue
		}
		found = true
		for _, k := range []string{"runner_id", "run_id", "job_id", "attempt"} {
			if _, ok := rec[k]; !ok {
				t.Errorf("executor error line lacks %s: %s", k, line)
			}
		}
	}
	if !found {
		t.Fatalf("no executor error line in:\n%s", logs.String())
	}
}
