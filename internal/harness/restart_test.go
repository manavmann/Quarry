package harness

import (
	"testing"
	"time"

	"quarry/internal/executor"
	"quarry/internal/store"
)

const restartPipeline = `name: restart
jobs:
  - name: a
    image: alpine
    steps: ["echo a"]
  - name: b
    image: alpine
    steps: ["echo b"]
  - name: final
    image: alpine
    steps: ["echo final"]
    needs: [a, b]
`

func TestServerRestartMidRun(t *testing.T)        { testServerRestart(t, leaseTTL/2, false) }
func TestServerRestartAfterLeaseTTL(t *testing.T) { testServerRestart(t, 2*leaseTTL, true) }

func testServerRestart(t *testing.T, downtime time.Duration, grace bool) {
	h := New(t, Opts{Agents: 2, Capacity: 1})
	for _, name := range []string{"a", "b"} {
		h.Script(name, executor.Outcome{Hang: true, LogBytes: 100})
	}
	run := h.Submit(restartPipeline)
	before := map[string]*Job{}
	for _, name := range []string{"a", "b"} {
		before[name] = h.WaitJob(run.Run.ID, name, store.JobRunning)
	}
	WaitFor(t, func() bool { return len(h.Executions()) == 2 }, "both executors entered")
	oldStore, oldURL := h.Store(), h.URL()
	h.StopServer()
	if err := oldStore.Reader().QueryRowContext(t.Context(), "SELECT 1").Scan(new(int)); err == nil {
		t.Fatal("SQLite connection was not closed")
	}
	h.Clock().Advance(downtime)
	if grace {
		for i := 0; i < 2; i++ {
			h.MuteHeartbeats(i, true)
		}
	}
	h.StartServer()
	if h.Store() == oldStore || h.URL() != oldURL {
		t.Fatal("restart did not reopen SQLite at the same address")
	}
	if grace {
		// Offline transitions prove the NEW monitor committed a tick while
		// both leases were expired and neither runner could renew them.
		for i := 0; i < 2; i++ {
			waitRunnerState(t, h, h.AgentID(i), store.RunnerOffline)
		}
		d, err := h.Client().GetRun(run.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		for name, old := range before {
			if j := d.Job(name); j.State != store.JobRunning || j.Attempt != 1 || j.RunnerID != old.RunnerID {
				t.Fatalf("reassigned during grace: %+v", j)
			}
		}
		// Renew near the end of grace, then cross its boundary. A single
		// initial skip (instead of a full TTL) cannot pass the unit boundary test.
		h.Clock().Advance(leaseTTL - time.Millisecond)
		for i := 0; i < 2; i++ {
			h.MuteHeartbeats(i, false)
		}
		WaitFor(t, func() bool {
			for _, old := range before {
				j, err := h.Store().GetJob(t.Context(), h.Store().Reader(), old.ID)
				if err != nil || j.LeaseExpiresAt < h.Clock().Now()+leaseTTL.Milliseconds() {
					return false
				}
			}
			return true
		}, "both original runners to renew")
		h.Clock().Advance(time.Millisecond)
	}
	for name := range before {
		h.Release(name)
	}
	done := h.WaitRun(run.Run.ID)
	if done.Run.State != store.RunSucceeded {
		t.Fatalf("run: %+v", done)
	}
	for name, old := range before {
		j := done.Job(name)
		if j.Attempt != 1 || j.RunnerID != old.RunnerID {
			t.Fatalf("job re-executed: %+v", j)
		}
		if got := len(h.LogText(j.ID)); got != 100 {
			t.Fatalf("log bytes = %d", got)
		}
	}
	for name, n := range countByName(h) {
		if n != 1 {
			t.Fatalf("%s executed %d times", name, n)
		}
	}
	if len(h.Executions()) != 3 || count(h.EventTypes(run.Run.ID), "job.requeued") != 0 {
		t.Fatal("restart re-executed work")
	}
}
