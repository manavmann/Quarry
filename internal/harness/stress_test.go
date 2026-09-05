package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"quarry/internal/executor"
	"quarry/internal/store"
)

// stressWorkers is the width of the fan-out: src → 48 workers → sink is
// the 50-job pipeline of the C19 stress test.
const stressWorkers = 48

// stressPipeline is src → w-00..w-47 → sink.
func stressPipeline() string {
	var b strings.Builder
	b.WriteString("name: stress\njobs:\n  - name: src\n    image: alpine\n    steps: [\"echo src\"]\n")
	var all []string
	for i := 0; i < stressWorkers; i++ {
		name := fmt.Sprintf("w-%02d", i)
		all = append(all, name)
		fmt.Fprintf(&b, "  - name: %s\n    image: alpine\n    steps: [\"echo %s\"]\n    needs: [src]\n", name, name)
	}
	fmt.Fprintf(&b, "  - name: sink\n    image: alpine\n    steps: [\"echo sink\"]\n    needs: [%s]\n", strings.Join(all, ", "))
	return b.String()
}

// expireDeadLeases moves the fake clock one second at a time until no
// running attempt of run id is held by a dead runner, waiting after every
// step until each live runner has talked to the server at the new time.
// A live runner renews its leases every HeartbeatInterval of real time,
// so one fake second per renewal keeps its lag far below the TTL and
// only the dead runners' leases expire — the way a wall clock ticks
// under a heartbeating fleet, compressed. It walks past the TTL by a few
// steps because a dead runner's last request may have been in flight at
// the kill and landed after the first step.
func expireDeadLeases(t *testing.T, h *Harness, ttl time.Duration, runID string, dead map[string]bool, live []string) {
	t.Helper()
	for step := 0; step <= int(ttl/time.Second)+5; step++ {
		h.Clock().Advance(time.Second)
		now := h.Clock().Now()
		WaitFor(t, func() bool {
			rs, err := h.Client().Runners()
			if err != nil {
				return false
			}
			seen := map[string]int64{}
			for _, r := range rs {
				seen[r.ID] = r.LastSeenAt
			}
			for _, id := range live {
				if seen[id] < now {
					return false
				}
			}
			return true
		}, "live runners to contact the server after the clock step")
		d, err := h.Client().GetRun(runID)
		if err != nil {
			t.Fatal(err)
		}
		held := false
		for _, j := range d.Jobs {
			if j.State == store.JobRunning && dead[j.RunnerID] {
				held = true
			}
		}
		if !held {
			return
		}
	}
	t.Fatal("dead runners still hold running attempts 5 s past the lease TTL")
}

// waitRunningOn polls until some job of run id is running on runner id.
func waitRunningOn(t *testing.T, h *Harness, runID, runnerID string) {
	t.Helper()
	WaitFor(t, func() bool {
		d, err := h.Client().GetRun(runID)
		if err != nil {
			return false
		}
		for _, j := range d.Jobs {
			if j.State == store.JobRunning && j.RunnerID == runnerID {
				return true
			}
		}
		return false
	}, "an attempt to be running on runner "+runnerID)
}

// Fifty jobs fan out and back in across three runners while two of them
// die mid-run without a word to the server: their leases expire, the
// monitor requeues what they held, the survivor runs everything to the
// end. The run succeeds, no (job, attempt) is executed more than once
// anywhere in the fleet, and the only requeues are lost_runner ones for
// attempts the dead runners held.
func TestStressFanOutFanInWithRunnerLoss(t *testing.T) {
	h := New(t, Opts{Agents: 3, Capacity: 2, LeaseTTL: leaseTTL})
	// Workers take long enough that a kill lands on running attempts.
	for i := 0; i < stressWorkers; i++ {
		h.Script(fmt.Sprintf("w-%02d", i), executor.Outcome{Delay: 100 * time.Millisecond, LogBytes: 256})
	}
	ids := []string{h.AgentID(0), h.AgentID(1), h.AgentID(2)}
	dead := map[string]bool{ids[0]: true, ids[1]: true}
	survivor := ids[2]

	run := h.Submit(stressPipeline())
	if len(run.Jobs) != stressWorkers+2 {
		t.Fatalf("submitted %d jobs, want %d", len(run.Jobs), stressWorkers+2)
	}
	// Kill each runner the moment it holds a running attempt; HardKill
	// does not block, so the second kill follows the first at once.
	for i := 0; i < 2; i++ {
		waitRunningOn(t, h, run.Run.ID, ids[i])
		h.HardKill(i)
	}
	expireDeadLeases(t, h, leaseTTL, run.Run.ID, dead, []string{survivor})

	done := h.WaitRun(run.Run.ID)
	if done.Run.State != store.RunSucceeded {
		t.Fatalf("run state = %s", done.Run.State)
	}

	// Terminal states: every job succeeded within max_attempts, and any
	// job that needed a second attempt got it from the survivor.
	byID := map[string]Job{}
	for _, j := range done.Jobs {
		byID[j.ID] = j
		if j.State != store.JobSucceeded || j.Attempt < 1 || j.Attempt > 3 || j.FailureKind != "" {
			t.Errorf("job %s = %+v", j.Name, j)
		}
		if j.Attempt > 1 && j.RunnerID != survivor {
			t.Errorf("job %s attempt %d finished on %s, want the survivor %s", j.Name, j.Attempt, j.RunnerID, survivor)
		}
	}

	// Once per attempt: no (job, attempt) ran twice anywhere, and every
	// job's final attempt ran exactly once, on the runner the server
	// recorded. Executions on the survivor all completed; only dead
	// runners have killed ones.
	type attempt struct {
		job string
		n   int
	}
	execs := map[attempt][]Execution{}
	for _, x := range h.Executions() {
		execs[attempt{x.Spec.JobID, x.Spec.Attempt}] = append(execs[attempt{x.Spec.JobID, x.Spec.Attempt}], x)
	}
	for k, xs := range execs {
		if len(xs) != 1 {
			t.Errorf("job %s attempt %d executed %d times", byID[k.job].Name, k.n, len(xs))
		}
		for _, x := range xs {
			if !dead[ids[x.Agent]] && x.Err != nil {
				t.Errorf("survivor's execution of %s attempt %d failed: %v", byID[k.job].Name, k.n, x.Err)
			}
			if x.Err != nil && !errors.Is(x.Err, context.Canceled) {
				t.Errorf("execution of %s attempt %d: err = %v, want nil or context.Canceled", byID[k.job].Name, k.n, x.Err)
			}
		}
	}
	for _, j := range done.Jobs {
		xs := execs[attempt{j.ID, j.Attempt}]
		if len(xs) != 1 || ids[xs[0].Agent] != j.RunnerID || xs[0].Err != nil {
			t.Errorf("final attempt %d of %s: executions %+v, runner %s", j.Attempt, j.Name, xs, j.RunnerID)
		}
	}

	// Events: one claim per attempt that was run; every requeue is a
	// lost_runner one for an attempt a dead runner held; nothing failed
	// or was skipped; the run finished exactly once.
	events := h.Events(run.Run.ID)
	var types []string
	claimed := map[attempt]int{}
	var requeued int
	for _, e := range events {
		types = append(types, e.Type)
		var d struct {
			Attempt     int    `json:"attempt"`
			FailureKind string `json:"failure_kind"`
			RunnerID    string `json:"runner_id"`
		}
		switch e.Type {
		case "job.claimed":
			if err := json.Unmarshal(e.Detail, &d); err != nil {
				t.Fatalf("job.claimed detail %s: %v", e.Detail, err)
			}
			claimed[attempt{e.JobID, d.Attempt}]++
		case "job.requeued":
			requeued++
			if err := json.Unmarshal(e.Detail, &d); err != nil || d.FailureKind != "lost_runner" || !dead[d.RunnerID] {
				t.Errorf("job.requeued for %s = %s (%v); want lost_runner on a dead runner", byID[e.JobID].Name, e.Detail, err)
			}
			for _, x := range execs[attempt{e.JobID, d.Attempt}] {
				if !dead[ids[x.Agent]] {
					t.Errorf("requeued attempt %d of %s was executed by the survivor", d.Attempt, byID[e.JobID].Name)
				}
			}
		}
	}
	for k, n := range claimed {
		if n != 1 {
			t.Errorf("job %s attempt %d claimed %d times", byID[k.job].Name, k.n, n)
		}
	}
	for k := range execs {
		if claimed[k] != 1 {
			t.Errorf("job %s attempt %d executed without a claim", byID[k.job].Name, k.n)
		}
	}
	if requeued == 0 {
		t.Error("no job.requeued events: the kills landed on idle runners")
	}
	if len(claimed) != len(done.Jobs)+requeued {
		t.Errorf("%d claimed attempts, want %d jobs + %d requeues", len(claimed), len(done.Jobs), requeued)
	}
	if count(types, "job.succeeded") != len(done.Jobs) || count(types, "job.failed") != 0 || count(types, "job.skipped") != 0 ||
		count(types, "run.finished") != 1 || count(types, "run.started") != 1 {
		t.Errorf("event counts: succeeded=%d failed=%d skipped=%d run.started=%d run.finished=%d",
			count(types, "job.succeeded"), count(types, "job.failed"), count(types, "job.skipped"),
			count(types, "run.started"), count(types, "run.finished"))
	}
	if types[len(types)-1] != "run.finished" {
		t.Errorf("last event = %s, want run.finished", types[len(types)-1])
	}

	// The fleet's view: dead runners silent past RunnerOfflineAfter are
	// offline, the survivor is online.
	waitRunnerState(t, h, ids[0], store.RunnerOffline)
	waitRunnerState(t, h, ids[1], store.RunnerOffline)
	waitRunnerState(t, h, survivor, store.RunnerOnline)
	t.Logf("requeued %d attempts; %d claims for %d jobs", requeued, len(claimed), len(done.Jobs))
}
