package scheduler

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"quarry/internal/store"
)

func tick(t *testing.T, s *Scheduler) TickReport {
	t.Helper()
	rep, err := s.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	return rep
}

func TestRetryableIsDecidedByFailureKindOnly(t *testing.T) {
	want := map[string]bool{
		store.FailureInfra: true, store.FailureLostRunner: true,
		store.FailureExitCode: false, store.FailureTimeout: false, store.FailureCancelled: false, "": false,
	}
	for kind, w := range want {
		if got := retryable(kind); got != w {
			t.Errorf("retryable(%q) = %v, want %v", kind, got, w)
		}
	}
	// A runner cannot report lost_runner itself: only the monitor decides it.
	s, st, _ := newScheduler(t, 30*time.Second)
	dag(t, st, []string{"a"}, map[string]spec{"a": {maxAtt: 3}})
	j := claim(t, s, "r1", nil)
	var inv *InvalidResultError
	if _, err := s.Complete(context.Background(), j.ID, j.Attempt, failed(store.FailureLostRunner)); !errors.As(err, &inv) {
		t.Fatalf("Complete(lost_runner) err = %v, want InvalidResultError", err)
	}
}

func TestTickRequeuesExpiredLeaseAndFencesTheOldAttempt(t *testing.T) {
	const ttl = 30 * time.Second
	s, st, clk := newScheduler(t, ttl)
	runID, ids := dag(t, st, []string{"a", "b"}, map[string]spec{"a": {maxAtt: 3}, "b": {needs: []string{"a"}}})
	a1 := claim(t, s, "r1", nil)

	// Before expiry a tick changes nothing.
	if rep := tick(t, s); len(rep.Requeued)+len(rep.Failed) != 0 {
		t.Fatalf("tick before expiry: %+v", rep)
	}
	wantState(t, st, ids["a"], store.JobRunning)

	clk.ms.Add(ttl.Milliseconds())
	rep := tick(t, s)
	if !slices.Equal(rep.Requeued, []string{ids["a"]}) || len(rep.Failed) != 0 {
		t.Fatalf("tick after expiry: %+v", rep)
	}
	j := getJob(t, st, ids["a"])
	if j.State != store.JobQueued || j.RunnerID != "" || j.LeaseExpiresAt != 0 || j.Attempt != 1 || j.FailureKind != "" {
		t.Fatalf("after expiry: %+v", j)
	}
	wantState(t, st, ids["b"], store.JobPending)
	if types := eventTypes(t, st, runID); !slices.Equal(types, []string{"run.started", "job.claimed", "job.requeued"}) {
		t.Fatalf("events = %v", types)
	}

	// Idempotent: the same tick again finds nothing.
	if rep := tick(t, s); len(rep.Requeued)+len(rep.Failed)+len(rep.RunnersOffline) != 0 {
		t.Fatalf("second tick: %+v", rep)
	}

	// The lost runner's late writes are fenced, its heartbeat says abort.
	if _, err := s.Complete(context.Background(), a1.ID, a1.Attempt, ok); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale complete err = %v, want ErrFenced", err)
	}
	ds, err := s.Heartbeat(context.Background(), "r1", []JobRef{{JobID: a1.ID, Attempt: a1.Attempt}})
	if err != nil || len(ds) != 1 || ds[0].Directive != DirectiveAbort {
		t.Fatalf("heartbeat = %+v, %v; want abort", ds, err)
	}

	// The next claim is attempt 2 and can finish the run.
	a2 := claim(t, s, "r2", nil)
	if a2 == nil || a2.ID != ids["a"] || a2.Attempt != 2 {
		t.Fatalf("reclaim = %+v, want attempt 2 of a", a2)
	}
	complete(t, s, a2, ok)
	complete(t, s, claim(t, s, "r2", nil), ok)
	if r := getRun(t, st, runID); r.State != store.RunSucceeded {
		t.Fatalf("run = %s, want succeeded", r.State)
	}
}

func TestTickFailsLostRunnerAtMaxAttemptsAndCascades(t *testing.T) {
	const ttl = 30 * time.Second
	s, st, clk := newScheduler(t, ttl)
	runID, ids := dag(t, st, []string{"a", "b", "c"}, map[string]spec{
		"a": {maxAtt: 2}, "b": {needs: []string{"a"}}, "c": {maxAtt: 3},
	})
	claim(t, s, "r1", nil) // a, attempt 1
	c := claim(t, s, "r1", nil)
	clk.ms.Add(ttl.Milliseconds())
	rep := tick(t, s)
	if !slices.Equal(rep.Requeued, []string{ids["a"], ids["c"]}) || len(rep.Failed) != 0 {
		t.Fatalf("first expiry: %+v", rep)
	}
	// Both are reclaimed; c is kept alive by heartbeats, a expires again.
	got := claimAll(t, s, "r2")
	c = got["c"]
	if a2 := got["a"]; c == nil || a2 == nil || a2.Attempt != 2 || c.Attempt != 2 {
		t.Fatalf("reclaim = %+v", got)
	}
	clk.ms.Add(ttl.Milliseconds() / 2)
	if _, err := s.Heartbeat(context.Background(), "r2", []JobRef{{JobID: c.ID, Attempt: c.Attempt}}); err != nil {
		t.Fatal(err)
	}
	clk.ms.Add(ttl.Milliseconds() / 2)
	rep = tick(t, s)
	if len(rep.Requeued) != 0 || !slices.Equal(rep.Failed, []string{ids["a"]}) {
		t.Fatalf("second expiry: %+v", rep)
	}
	j := getJob(t, st, ids["a"])
	if j.State != store.JobFailed || j.FailureKind != store.FailureLostRunner || j.Attempt != 2 || j.ExitCode != nil {
		t.Fatalf("a after exhaustion: %+v", j)
	}
	wantState(t, st, ids["b"], store.JobSkipped)
	wantState(t, st, ids["c"], store.JobRunning)
	if r := getRun(t, st, runID); r.State != store.RunRunning {
		t.Fatalf("run = %s, want running (c still holds its lease)", r.State)
	}
	complete(t, s, c, ok)
	if r := getRun(t, st, runID); r.State != store.RunFailed {
		t.Fatalf("run = %s, want failed", r.State)
	}
	types := eventTypes(t, st, runID)
	if n := slices.Index(types, "job.failed"); n < 0 || !slices.Contains(types, "job.skipped") || !slices.Contains(types, "run.finished") {
		t.Fatalf("events = %v", types)
	}
}

func TestTickMarksSilentRunnersOfflineOnce(t *testing.T) {
	s, st, clk := newScheduler(t, 30*time.Second)
	dag(t, st, []string{"a"}, map[string]spec{"a": {maxAtt: 3}})
	claim(t, s, "r1", nil)
	claim(t, s, "r2", nil) // nothing left, but registers r2

	if rep := tick(t, s); len(rep.RunnersOffline) != 0 {
		t.Fatalf("tick before silence: %+v", rep)
	}
	clk.ms.Add(DefaultRunnerOfflineAfter.Milliseconds())
	// r2 keeps polling: an empty claim still refreshes last_seen_at.
	claim(t, s, "r2", nil)
	clk.ms.Add(time.Second.Milliseconds())
	rep := tick(t, s)
	if !slices.Equal(rep.RunnersOffline, []string{"r1"}) {
		t.Fatalf("offline = %v, want [r1]", rep.RunnersOffline)
	}
	if r, _ := st.GetRunner(context.Background(), st.Reader(), "r1"); r.State != store.RunnerOffline {
		t.Fatalf("r1 state = %s", r.State)
	}
	if r, _ := st.GetRunner(context.Background(), st.Reader(), "r2"); r.State != store.RunnerOnline {
		t.Fatalf("r2 state = %s", r.State)
	}
	if rep := tick(t, s); len(rep.RunnersOffline) != 0 {
		t.Fatalf("second tick reported r1 again: %+v", rep)
	}
	// Any contact brings the runner back.
	if _, err := s.Heartbeat(context.Background(), "r1", nil); err != nil {
		t.Fatal(err)
	}
	if r, _ := st.GetRunner(context.Background(), st.Reader(), "r1"); r.State != store.RunnerOnline {
		t.Fatalf("r1 after heartbeat = %s", r.State)
	}
}
