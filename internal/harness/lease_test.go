package harness

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"quarry/internal/executor"
	"quarry/internal/store"
)

const leaseTTL = 30 * time.Second

// waitAttempt polls until job name of run id is running its n-th attempt.
func waitAttempt(t *testing.T, h *Harness, id, name string, n int) *Job {
	t.Helper()
	var last *Job
	WaitFor(t, func() bool {
		d, err := h.Client().GetRun(id)
		if err != nil {
			return false
		}
		last = d.Job(name)
		return last != nil && last.State == store.JobRunning && last.Attempt == n
	}, fmt.Sprintf("job %s attempt %d to be running", name, n))
	return last
}

// runnerState polls until runner id reports state.
func waitRunnerState(t *testing.T, h *Harness, id, state string) {
	t.Helper()
	WaitFor(t, func() bool {
		rs, err := h.Client().Runners()
		if err != nil {
			return false
		}
		for _, r := range rs {
			if r.ID == id {
				return r.State == state
			}
		}
		return false
	}, "runner "+id+" to be "+state)
}

// A runner that stops heartbeating loses its lease once the clock passes
// the TTL: the monitor requeues the job, another runner claims attempt 2
// and the run succeeds. The lost runner is marked offline and its late
// completion for attempt 1 is fenced with 409.
func TestLostRunnerJobIsReassigned(t *testing.T) {
	h := New(t, Opts{Agents: 2, Capacity: 1, LeaseTTL: leaseTTL})
	h.Script("build", executor.Outcome{Hang: true})

	run := h.Submit(diamond)
	first := waitAttempt(t, h, run.Run.ID, "build", 1)
	lost, other := 0, 1
	if first.RunnerID == h.AgentID(1) {
		lost, other = 1, 0
	}
	h.MuteHeartbeats(lost, true)

	// Attempt 2 also hangs (so it can be observed running), then is released.
	h.Clock().Advance(leaseTTL + time.Second)
	second := waitAttempt(t, h, run.Run.ID, "build", 2)
	if second.RunnerID != h.AgentID(other) {
		t.Fatalf("attempt 2 ran on %q, want %q (the other runner)", second.RunnerID, h.AgentID(other))
	}
	h.Exec(other).Release("build")
	done := h.WaitRun(run.Run.ID)
	if done.Run.State != store.RunSucceeded {
		t.Fatalf("run state = %s", done.Run.State)
	}
	if j := done.Job("build"); j.State != store.JobSucceeded || j.Attempt != 2 || j.RunnerID != h.AgentID(other) {
		t.Fatalf("build = %+v", j)
	}

	// The requeue is recorded as lost_runner against the first runner.
	var requeued int
	for _, e := range h.Events(run.Run.ID) {
		if e.Type != "job.requeued" {
			continue
		}
		requeued++
		var d struct {
			Attempt     int    `json:"attempt"`
			FailureKind string `json:"failure_kind"`
			RunnerID    string `json:"runner_id"`
		}
		if err := json.Unmarshal(e.Detail, &d); err != nil || d.Attempt != 1 || d.FailureKind != "lost_runner" || d.RunnerID != h.AgentID(lost) {
			t.Fatalf("job.requeued detail = %s (%v)", e.Detail, err)
		}
	}
	if requeued != 1 {
		t.Fatalf("job.requeued events = %d, want 1", requeued)
	}

	// The silent runner is offline; the one that kept claiming is not.
	waitRunnerState(t, h, h.AgentID(lost), store.RunnerOffline)
	waitRunnerState(t, h, h.AgentID(other), store.RunnerOnline)

	// A stale completion for attempt 1 is fenced even after attempt 2 won.
	body := fmt.Sprintf(`{"runner_id":%q,"attempt":1,"status":"succeeded"}`, h.AgentID(lost))
	req, _ := http.NewRequest(http.MethodPost, h.URL()+"/api/runner/jobs/"+done.Job("build").ID+"/complete", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale complete status = %d, want 409", resp.StatusCode)
	}
	if d, _ := h.Client().GetRun(run.Run.ID); d.Job("build").Attempt != 2 || d.Job("build").State != store.JobSucceeded {
		t.Fatalf("build after stale complete = %+v", d.Job("build"))
	}
}

// Losing the lease on every attempt exhausts max_attempts: the job fails
// as lost_runner, dependents are skipped and the run fails.
func TestLostRunnerExhaustsAttempts(t *testing.T) {
	h := New(t, Opts{Agents: 1, Capacity: 3, LeaseTTL: leaseTTL, MaxAttempts: 2})
	h.Script("build", executor.Outcome{Hang: true})
	h.MuteHeartbeats(0, true)

	run := h.Submit(diamond)
	waitAttempt(t, h, run.Run.ID, "build", 1)
	h.Clock().Advance(leaseTTL + time.Second)
	waitAttempt(t, h, run.Run.ID, "build", 2)
	h.Clock().Advance(leaseTTL + time.Second)

	done := h.WaitRun(run.Run.ID)
	if done.Run.State != store.RunFailed {
		t.Fatalf("run state = %s", done.Run.State)
	}
	if j := done.Job("build"); j.State != store.JobFailed || j.FailureKind != "lost_runner" || j.Attempt != 2 || j.ExitCode != nil {
		t.Fatalf("build = %+v", j)
	}
	for _, name := range []string{"test", "lint", "deploy"} {
		if j := done.Job(name); j.State != store.JobSkipped {
			t.Errorf("%s = %s, want skipped", name, j.State)
		}
	}
	types := h.EventTypes(run.Run.ID)
	if count(types, "job.requeued") != 1 || count(types, "job.failed") != 1 || count(types, "job.skipped") != 3 {
		t.Fatalf("event types = %v", types)
	}
	if got := countByName(h); got["build"] != 2 {
		t.Fatalf("build executions = %d, want 2", got["build"])
	}
	// The runner kept polling for its free slots, so it is still online.
	waitRunnerState(t, h, h.AgentID(0), store.RunnerOnline)
}

// A runner that stops talking to the server altogether goes offline after
// RunnerOfflineAfter of silence and comes back on its next contact.
func TestSilentRunnerGoesOffline(t *testing.T) {
	h := New(t, Opts{Agents: 1})
	id := h.AgentID(0)
	waitRunnerState(t, h, id, store.RunnerOnline)

	h.Kill(0)
	h.Clock().Advance(31 * time.Second)
	waitRunnerState(t, h, id, store.RunnerOffline)

	h.Restart(0)
	waitRunnerState(t, h, id, store.RunnerOnline)
	if h.AgentID(0) != id {
		t.Fatalf("runner id after restart = %q, want %q", h.AgentID(0), id)
	}
}
