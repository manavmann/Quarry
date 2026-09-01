package scheduler

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"quarry/internal/store"
)

// ---- cancel ---------------------------------------------------------------

func cancelRun(t *testing.T, s *Scheduler, runID string) *store.Run {
	t.Helper()
	r, err := s.CancelRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("CancelRun(%s): %v", runID, err)
	}
	return r
}

// A run nothing has claimed yet is cancelled outright: every job goes to
// cancelled, the run is terminal in the same transaction.
func TestCancelRunBeforeAnyClaimFinalizesAtOnce(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	runID, ids := dag(t, st, []string{"a", "b"}, map[string]spec{"a": {}, "b": {needs: []string{"a"}}})

	r := cancelRun(t, s, runID)
	if r.State != store.RunCancelled || r.FinishedAt == 0 {
		t.Fatalf("run = %+v, want cancelled and finished", r)
	}
	for _, name := range []string{"a", "b"} {
		j := getJob(t, st, ids[name])
		if j.State != store.JobCancelled || j.FailureKind != store.FailureCancelled || j.FinishedAt == 0 {
			t.Fatalf("job %s = %+v, want cancelled", name, j)
		}
	}
	if types := eventTypes(t, st, runID); !slices.Equal(types, []string{"job.cancelled", "job.cancelled", "run.cancel_requested", "run.finished"}) {
		t.Fatalf("events = %v", types)
	}
	// Nothing is claimable afterwards, and cancelling again is a conflict.
	if j := claim(t, s, "r1", nil); j != nil {
		t.Fatalf("claimed %s from a cancelled run", j.Name)
	}
	if _, err := s.CancelRun(context.Background(), runID); !errors.Is(err, ErrRunFinished) {
		t.Fatalf("second cancel err = %v, want ErrRunFinished", err)
	}
	if _, err := s.CancelRun(context.Background(), "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown run err = %v, want ErrNotFound", err)
	}
}

// With an attempt in flight the cancel is a directive: the running job's
// next heartbeat says cancel (lease still extended), its failed(cancelled)
// report lands as a cancelled job, and only then is the run finalized.
// Pending and queued siblings are cancelled immediately.
func TestCancelRunWithRunningJobCancelsViaHeartbeat(t *testing.T) {
	s, st, clk := newScheduler(t, 30*time.Second)
	runID, ids := dag(t, st, []string{"a", "b", "c"}, map[string]spec{
		"a": {}, "b": {}, "c": {needs: []string{"a"}},
	})
	a := claim(t, s, "r1", nil) // b stays queued, c pending

	r := cancelRun(t, s, runID)
	if r.State != store.RunRunning {
		t.Fatalf("run = %s, want still running while a winds down", r.State)
	}
	wantState(t, st, ids["b"], store.JobCancelled)
	wantState(t, st, ids["c"], store.JobCancelled)
	ja := getJob(t, st, ids["a"])
	if ja.State != store.JobRunning || ja.CancelRequestedAt == 0 || ja.CancelReason != store.CancelReasonUser {
		t.Fatalf("a = %+v, want running with a user cancel request", ja)
	}
	// Repeating the cancel while winding down is a no-op, not an error.
	before := eventTypes(t, st, runID)
	cancelRun(t, s, runID)
	if after := eventTypes(t, st, runID); !slices.Equal(before, after) {
		t.Fatalf("repeated cancel added events: before %v, after %v", before, after)
	}

	clk.ms.Add(1_000)
	ds, err := s.Heartbeat(context.Background(), "r1", []JobRef{{a.ID, a.Attempt}})
	if err != nil || len(ds) != 1 || ds[0].Directive != DirectiveCancel {
		t.Fatalf("heartbeat = %+v, %v; want cancel", ds, err)
	}
	if got := getJob(t, st, ids["a"]).LeaseExpiresAt; got <= a.LeaseExpiresAt {
		t.Fatalf("lease not extended alongside cancel: %d <= %d", got, a.LeaseExpiresAt)
	}
	// A foreign runner or a stale attempt still gets abort, never cancel.
	if ds, _ = s.Heartbeat(context.Background(), "r2", []JobRef{{a.ID, a.Attempt}}); ds[0].Directive != DirectiveAbort {
		t.Fatalf("foreign heartbeat = %s, want abort", ds[0].Directive)
	}

	out := complete(t, s, a, Result{Status: store.JobFailed, FailureKind: store.FailureCancelled, Error: "job cancelled by user"})
	if out.State != store.JobCancelled || out.FailureKind != store.FailureCancelled || out.Attempt != 1 {
		t.Fatalf("a after report = %+v, want cancelled at attempt 1", out)
	}
	if r := getRun(t, st, runID); r.State != store.RunCancelled || r.FinishedAt == 0 {
		t.Fatalf("run = %+v, want cancelled", r)
	}
	types := eventTypes(t, st, runID)
	if count(types, "job.cancel_requested") != 1 || count(types, "run.cancel_requested") != 1 ||
		count(types, "job.cancelled") != 3 || types[len(types)-1] != "run.finished" {
		t.Fatalf("events = %v", types)
	}
}

// A job that already failed keeps the run failed even when the rest is
// cancelled: the run result stays a pure function of job states.
func TestCancelRunAfterAFailureStaysFailed(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	runID, ids := dag(t, st, []string{"a", "b"}, map[string]spec{"a": {}, "b": {}})
	a := claim(t, s, "r1", nil)
	code := 2
	complete(t, s, a, Result{Status: store.JobFailed, FailureKind: store.FailureExitCode, ExitCode: &code})
	cancelRun(t, s, runID)
	wantState(t, st, ids["b"], store.JobCancelled)
	if r := getRun(t, st, runID); r.State != store.RunFailed {
		t.Fatalf("run = %s, want failed", r.State)
	}
}

// Once a cancel is requested a job never gets another attempt: an infra
// failure that would otherwise requeue is terminal.
func TestCancelRequestedJobIsNotRequeued(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	runID, ids := dag(t, st, []string{"a"}, map[string]spec{"a": {maxAtt: 3}})
	a := claim(t, s, "r1", nil)
	cancelRun(t, s, runID)
	out := complete(t, s, a, failed(store.FailureInfra))
	if out.State != store.JobFailed || out.FailureKind != store.FailureInfra {
		t.Fatalf("a = %+v, want terminal failed(infra), not requeued", out)
	}
	wantState(t, st, ids["a"], store.JobFailed)
	if r := getRun(t, st, runID); r.State != store.RunFailed {
		t.Fatalf("run = %s, want failed", r.State)
	}
}

// ---- timeout backstop -------------------------------------------------------

// Losing a runner after either kind of cancel request must not start a
// new attempt. Lease expiry remains lost_runner, and finalizes the run.
func TestCancelRequestedLeaseExpiryNeverRequeues(t *testing.T) {
	for _, reason := range []string{store.CancelReasonUser, store.CancelReasonTimeout} {
		t.Run(reason, func(t *testing.T) {
			const ttl = time.Minute
			s, st, clk := newScheduler(t, ttl)
			runID, ids := dag(t, st, []string{"a", "b"}, map[string]spec{
				"a": {maxAtt: 3, timeout: 10 * time.Second}, "b": {needs: []string{"a"}},
			})
			a := claim(t, s, "r1", nil)
			if reason == store.CancelReasonUser {
				cancelRun(t, s, runID)
			} else {
				clk.ms.Add((41 * time.Second).Milliseconds())
				if rep := tick(t, s); !slices.Equal(rep.TimedOut, []string{a.ID}) {
					t.Fatalf("timeout tick = %+v", rep)
				}
			}
			clk.ms.Add(ttl.Milliseconds())
			if rep := tick(t, s); len(rep.Requeued) != 0 || !slices.Equal(rep.Failed, []string{a.ID}) {
				t.Fatalf("expiry tick = %+v, want failed with no requeue", rep)
			}
			j := getJob(t, st, ids["a"])
			if j.State != store.JobFailed || j.FailureKind != store.FailureLostRunner || j.Attempt != 1 {
				t.Fatalf("job = %+v, want failed(lost_runner) at attempt 1", j)
			}
			if r := getRun(t, st, runID); r.State != store.RunFailed || r.FinishedAt == 0 {
				t.Fatalf("run = %+v, want terminal failed", r)
			}
			if j := claim(t, s, "r2", nil); j != nil {
				t.Fatalf("claimed %+v after cancellation", j)
			}
			if rep := tick(t, s); len(rep.Requeued)+len(rep.Failed)+len(rep.TimedOut) != 0 {
				t.Fatalf("second tick = %+v, want no job changes", rep)
			}
			if types := eventTypes(t, st, runID); count(types, "job.requeued") != 0 || count(types, "run.finished") != 1 {
				t.Fatalf("events = %v", types)
			}
		})
	}
}

// A running job past timeout+grace gets a cancel request from the monitor
// (once), its heartbeat turns to cancel, and the runner's cancelled report
// is recorded as failed(timeout) because the server asked for the timeout.
func TestTickBackstopsTimeoutAsCancelDirective(t *testing.T) {
	const ttl, timeout, grace = time.Hour, 10 * time.Minute, DefaultTimeoutGrace
	s, st, clk := newScheduler(t, ttl)
	runID, ids := dag(t, st, []string{"a", "b"}, map[string]spec{
		"a": {timeout: timeout, maxAtt: 3}, "b": {needs: []string{"a"}},
	})
	a := claim(t, s, "r1", nil)

	// Past the timeout but within the grace: nothing yet.
	clk.ms.Add(timeout.Milliseconds() + grace.Milliseconds()/2)
	if rep := tick(t, s); len(rep.TimedOut) != 0 {
		t.Fatalf("tick inside grace: %+v", rep)
	}
	clk.ms.Add(grace.Milliseconds())
	rep := tick(t, s)
	if !slices.Equal(rep.TimedOut, []string{ids["a"]}) || len(rep.Requeued)+len(rep.Failed) != 0 {
		t.Fatalf("tick past grace: %+v", rep)
	}
	j := getJob(t, st, ids["a"])
	if j.State != store.JobRunning || j.CancelReason != store.CancelReasonTimeout {
		t.Fatalf("a = %+v, want running with a timeout cancel request", j)
	}
	if rep := tick(t, s); len(rep.TimedOut) != 0 {
		t.Fatalf("second tick asked again: %+v", rep)
	}

	ds, err := s.Heartbeat(context.Background(), "r1", []JobRef{{a.ID, a.Attempt}})
	if err != nil || ds[0].Directive != DirectiveCancel {
		t.Fatalf("heartbeat = %+v, %v; want cancel", ds, err)
	}
	out := complete(t, s, a, Result{Status: store.JobFailed, FailureKind: store.FailureCancelled, Error: "job cancelled by user"})
	if out.State != store.JobFailed || out.FailureKind != store.FailureTimeout || out.Attempt != 1 {
		t.Fatalf("a = %+v, want failed(timeout) at attempt 1, no retry", out)
	}
	wantState(t, st, ids["b"], store.JobSkipped)
	if r := getRun(t, st, runID); r.State != store.RunFailed {
		t.Fatalf("run = %s, want failed", r.State)
	}
	types := eventTypes(t, st, runID)
	if count(types, "job.cancel_requested") != 1 || count(types, "job.failed") != 1 || count(types, "job.cancelled") != 0 {
		t.Fatalf("events = %v", types)
	}
}
