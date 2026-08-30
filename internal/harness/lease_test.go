package harness

import (
	"context"
	"encoding/json"
	"errors"
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
	// Mute before anything is claimed: whichever runner wins attempt 1 must
	// never get a heartbeat through. Muting after the claim would race a
	// heartbeat already past the transport, which the server would stamp
	// with the advanced clock and so extend the lease past the expiry the
	// test is about to force.
	h.MuteHeartbeats(0, true)
	h.MuteHeartbeats(1, true)

	run := h.Submit(diamond)
	first := waitAttempt(t, h, run.Run.ID, "build", 1)
	lost, other := 0, 1
	if first.RunnerID == h.AgentID(1) {
		lost, other = 1, 0
	}
	// The idle runner has no attempt and so no heartbeat in flight; it may
	// talk again so attempt 2 stays leased.
	h.MuteHeartbeats(other, false)

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

// zombiePipeline is build → {test, lint}: build leaves an artifact so a
// superseded attempt has something to duplicate, and the two dependents
// let a test observe that both runners still have a free slot afterwards.
const zombiePipeline = `name: zombie
jobs:
  - name: build
    image: alpine
    steps: ["make"]
    artifacts: ["dist"]
  - name: test
    image: alpine
    steps: ["echo test"]
    needs: [build]
  - name: lint
    image: alpine
    steps: ["echo lint"]
    needs: [build]
`

// A runner that stalls past the lease TTL while still holding a container
// is a zombie once another runner has finished the job: when it resumes,
// its heartbeat for the old attempt comes back "abort", it kills the
// container, reports nothing, and its capacity slot is free again. The
// job's final state and artifacts are the ones the second runner
// produced — the zombie's never reach the store.
func TestZombieRunnerAbortsSupersededAttempt(t *testing.T) {
	h := New(t, Opts{Agents: 2, Capacity: 1, LeaseTTL: leaseTTL})
	// Each runner's build leaves a different file, so the listing says
	// whose artifact survived.
	for i := 0; i < 2; i++ {
		h.Exec(i).Script("build", executor.Outcome{Hang: true, Artifacts: map[string]string{
			"dist/app.bin": "built by " + h.AgentName(i),
		}})
	}
	// test and lint hang so the test can see both running at once.
	h.Script("test", executor.Outcome{Hang: true})
	h.Script("lint", executor.Outcome{Hang: true})
	// Mute before anything is claimed (see TestLostRunnerJobIsReassigned).
	h.MuteHeartbeats(0, true)
	h.MuteHeartbeats(1, true)

	run := h.Submit(zombiePipeline)
	first := waitAttempt(t, h, run.Run.ID, "build", 1)
	zombie, other := 0, 1
	if first.RunnerID == h.AgentID(1) {
		zombie, other = 1, 0
	}
	h.MuteHeartbeats(other, false)

	// The zombie stalls past the TTL; the other runner takes attempt 2 and
	// finishes it, artifact included.
	h.Clock().Advance(leaseTTL + time.Second)
	if second := waitAttempt(t, h, run.Run.ID, "build", 2); second.RunnerID != h.AgentID(other) {
		t.Fatalf("attempt 2 ran on %q, want %q", second.RunnerID, h.AgentID(other))
	}
	h.Exec(other).Release("build")
	build := h.WaitJob(run.Run.ID, "build", store.JobSucceeded)
	if build.Attempt != 2 || build.RunnerID != h.AgentID(other) {
		t.Fatalf("build = %+v, want attempt 2 on %q", build, h.AgentID(other))
	}

	// The zombie resumes: its heartbeat still names attempt 1, the fence
	// fails, the directive is abort and the executor is killed by context.
	h.MuteHeartbeats(zombie, false)
	WaitFor(t, func() bool {
		for _, x := range h.Exec(zombie).Executions() {
			if x.Spec.Job.Name == "build" && x.Err != nil {
				return true
			}
		}
		return false
	}, "zombie's build container to be killed")
	var zombieBuilds int
	for _, x := range h.Exec(zombie).Executions() {
		if x.Spec.Job.Name != "build" {
			continue
		}
		zombieBuilds++
		if !errors.Is(x.Err, context.Canceled) {
			t.Fatalf("zombie build err = %v, want context.Canceled (killed, not finished)", x.Err)
		}
	}
	if zombieBuilds != 1 {
		t.Fatalf("zombie ran build %d times, want 1", zombieBuilds)
	}

	// Nothing the zombie did is visible: build is still attempt 2 on the
	// other runner, and the store holds exactly the other runner's
	// artifact, once, under attempt 2.
	if j, _ := h.Client().GetRun(run.Run.ID); j.Job("build").Attempt != 2 || j.Job("build").State != store.JobSucceeded || j.Job("build").RunnerID != h.AgentID(other) {
		t.Fatalf("build after zombie abort = %+v", j.Job("build"))
	}
	arts, err := h.Client().Artifacts(build.ID)
	if err != nil || len(arts) != 1 || arts[0].Path != "dist/app.bin" {
		t.Fatalf("artifacts = %+v, %v; want just dist/app.bin", arts, err)
	}
	want := "built by " + h.AgentName(other)
	if got, err := h.Client().Download(build.ID, "dist/app.bin"); err != nil || string(got) != want {
		t.Fatalf("dist/app.bin = %q, %v; want %q", got, err, want)
	}
	keys, err := h.ArtifactStore().List(t.Context(), "runs/")
	if err != nil || len(keys) != 1 || !strings.HasSuffix(keys[0], "/2/dist/app.bin") {
		t.Fatalf("store keys = %v, %v; want one key under attempt 2", keys, err)
	}

	// The zombie's slot is free again: with capacity 1 per runner, test
	// and lint can only both be running if the zombie claimed one.
	waitAttempt(t, h, run.Run.ID, "test", 1)
	waitAttempt(t, h, run.Run.ID, "lint", 1)
	d, _ := h.Client().GetRun(run.Run.ID)
	runners := map[string]bool{d.Job("test").RunnerID: true, d.Job("lint").RunnerID: true}
	if !runners[h.AgentID(zombie)] || !runners[h.AgentID(other)] {
		t.Fatalf("test on %q, lint on %q; want one on each runner", d.Job("test").RunnerID, d.Job("lint").RunnerID)
	}
	h.Release("test")
	h.Release("lint")
	if done := h.WaitRun(run.Run.ID); done.Run.State != store.RunSucceeded {
		t.Fatalf("run state = %s", done.Run.State)
	}
	// The abort is not a failure of the run: exactly one requeue, no
	// job.failed, and the zombie is back online.
	types := h.EventTypes(run.Run.ID)
	if count(types, "job.requeued") != 1 || count(types, "job.failed") != 0 {
		t.Fatalf("event types = %v", types)
	}
	waitRunnerState(t, h, h.AgentID(zombie), store.RunnerOnline)
}
