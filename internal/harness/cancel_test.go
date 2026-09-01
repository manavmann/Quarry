package harness

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"quarry/internal/executor"
	"quarry/internal/store"
)

// countTypes is how many events of type typ run id has.
func countTypes(types []string, typ string) int {
	n := 0
	for _, t := range types {
		if t == typ {
			n++
		}
	}
	return n
}

// A user cancel reaches the running attempts on their next heartbeat: the
// executors are killed with a cancelled context, each attempt reports
// failed(cancelled) and lands as a cancelled job with no second attempt,
// the job that had not started is cancelled at once, and the run ends
// cancelled. Executor cancellation must happen within one heartbeat
// interval; log flushing and run finalization are checked separately.
func TestCancelPropagatesWithinOneHeartbeat(t *testing.T) {
	const hb = time.Second
	h := New(t, Opts{Capacity: 2, HeartbeatInterval: hb})
	h.Script("build", executor.Outcome{Hang: true})
	h.Script("lint", executor.Outcome{Hang: true})
	run := h.Submit(`name: cancel
jobs:
  - name: build
    image: alpine
    steps: ["make"]
  - name: lint
    image: alpine
    steps: ["make lint"]
  - name: test
    image: alpine
    steps: ["make test"]
    needs: [build]
`)
	waitAttempt(t, h, run.Run.ID, "build", 1)
	waitAttempt(t, h, run.Run.ID, "lint", 1)
	WaitFor(t, func() bool { return len(h.Executions()) == 2 }, "both executors to start")

	start := time.Now()
	deadline := time.NewTimer(hb)
	defer deadline.Stop()
	d, err := h.Client().Cancel(run.Run.ID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if d.Run.State != store.RunRunning || d.Job("test").State != store.JobCancelled || d.Job("build").State != store.JobRunning {
		t.Fatalf("right after cancel: run %s, build %s, test %s", d.Run.State, d.Job("build").State, d.Job("test").State)
	}
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		execs := h.Executions()
		if len(execs) == 2 && errors.Is(execs[0].Err, context.Canceled) && errors.Is(execs[1].Err, context.Canceled) {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("cancel did not reach both executors within one heartbeat (%v): %+v", hb, execs)
		case <-poll.C:
		}
	}
	if took := time.Since(start); took > hb {
		t.Fatalf("cancel took %v, more than one heartbeat of %v", took, hb)
	}
	final := h.WaitRun(run.Run.ID)
	if final.Run.State != store.RunCancelled || final.Run.FinishedAt == 0 {
		t.Fatalf("run = %+v, want cancelled", final.Run)
	}
	for _, name := range []string{"build", "lint", "test"} {
		j := final.Job(name)
		if j.State != store.JobCancelled || j.FailureKind != store.FailureCancelled {
			t.Errorf("%s = %+v, want cancelled", name, j)
		}
	}
	if b := final.Job("build"); b.Attempt != 1 || !strings.Contains(b.Error, "cancelled by user") {
		t.Errorf("build = %+v, want attempt 1 cancelled by user", b)
	}
	execs := h.Executions()
	if len(execs) != 2 {
		t.Fatalf("executions = %d, want exactly build and lint once", len(execs))
	}
	for _, e := range execs {
		if !errors.Is(e.Err, context.Canceled) {
			t.Errorf("%s executor ended with %v, want context.Canceled", e.Spec.Job.Name, e.Err)
		}
	}
	types := h.EventTypes(run.Run.ID)
	if countTypes(types, "run.cancel_requested") != 1 || countTypes(types, "job.cancel_requested") != 2 ||
		countTypes(types, "job.cancelled") != 3 || countTypes(types, "job.requeued") != 0 ||
		types[len(types)-1] != "run.finished" {
		t.Fatalf("events = %v", types)
	}

	// Cancelling a finished run is a conflict; the CLI surfaces it as such.
	if _, err := h.Client().Cancel(run.Run.ID); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("second cancel err = %v, want 409", err)
	}
}

// A run cancelled before any runner claimed it is terminal in the cancel
// call itself and nothing ever runs.
func TestCancelUnclaimedRunIsImmediate(t *testing.T) {
	h := New(t, Opts{Labels: func(int) map[string]string { return nil }})
	run := h.Submit(`name: never
jobs:
  - name: build
    image: alpine
    steps: ["make"]
    labels: {os: plan9}
`)
	d, err := h.Client().Cancel(run.Run.ID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if d.Run.State != store.RunCancelled || d.Job("build").State != store.JobCancelled {
		t.Fatalf("after cancel: %+v", d)
	}
	if n := len(h.Executions()); n != 0 {
		t.Fatalf("%d executions, want none", n)
	}
}

// The runner enforces the job's timeout through the executor context:
// the fake sees DeadlineExceeded, the attempt is reported failed(timeout),
// never retried, dependents are skipped and the run fails.
func TestJobTimeoutIsEnforcedByRunner(t *testing.T) {
	h := New(t, Opts{})
	h.Script("build", executor.Outcome{Hang: true})
	run := h.Submit(`name: timeout
jobs:
  - name: build
    image: alpine
    steps: ["sleep 60"]
    timeout: 200ms
  - name: test
    image: alpine
    steps: ["make test"]
    needs: [build]
`)
	final := h.WaitRun(run.Run.ID)
	b := final.Job("build")
	if b.State != store.JobFailed || b.FailureKind != store.FailureTimeout || b.Attempt != 1 {
		t.Fatalf("build = %+v, want failed(timeout) at attempt 1", b)
	}
	if final.Job("test").State != store.JobSkipped || final.Run.State != store.RunFailed {
		t.Fatalf("test %s, run %s; want skipped, failed", final.Job("test").State, final.Run.State)
	}
	execs := h.Executions()
	if len(execs) != 1 || !errors.Is(execs[0].Err, context.DeadlineExceeded) {
		t.Fatalf("executions = %+v, want one ending in DeadlineExceeded", execs)
	}
	if types := h.EventTypes(run.Run.ID); countTypes(types, "job.failed") != 1 || countTypes(types, "job.requeued") != 0 {
		t.Fatalf("events = %v", types)
	}
}

// The server backstop: when the store clock says a running job is past
// its timeout plus the grace and the runner has not ended it (here the
// runner's own real-time timeout is far away), the monitor issues a cancel
// directive; the runner kills the attempt and reports cancelled, which
// the server records as failed(timeout) because it asked for the timeout.
func TestTimeoutBackstopCancelsThroughHeartbeat(t *testing.T) {
	const timeout = 10 * time.Minute
	h := New(t, Opts{LeaseTTL: 2 * time.Hour, RunnerOfflineAfter: 2 * time.Hour})
	h.Script("build", executor.Outcome{Hang: true})
	run := h.Submit(`name: backstop
jobs:
  - name: build
    image: alpine
    steps: ["sleep 60"]
    timeout: 10m
  - name: test
    image: alpine
    steps: ["make test"]
    needs: [build]
`)
	waitAttempt(t, h, run.Run.ID, "build", 1)
	h.Clock().Advance(timeout + 31*time.Second)

	final := h.WaitRun(run.Run.ID)
	b := final.Job("build")
	if b.State != store.JobFailed || b.FailureKind != store.FailureTimeout || b.Attempt != 1 || !strings.Contains(b.Error, "backstop") {
		t.Fatalf("build = %+v, want failed(timeout) by the backstop at attempt 1", b)
	}
	if final.Job("test").State != store.JobSkipped || final.Run.State != store.RunFailed {
		t.Fatalf("test %s, run %s; want skipped, failed", final.Job("test").State, final.Run.State)
	}
	execs := h.Executions()
	if len(execs) != 1 || !errors.Is(execs[0].Err, context.Canceled) {
		t.Fatalf("executions = %+v, want one killed with context.Canceled", execs)
	}
	types := h.EventTypes(run.Run.ID)
	if countTypes(types, "job.cancel_requested") != 1 || countTypes(types, "job.failed") != 1 ||
		countTypes(types, "job.cancelled") != 0 || countTypes(types, "job.requeued") != 0 {
		t.Fatalf("events = %v", types)
	}
}
