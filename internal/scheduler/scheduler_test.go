package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"quarry/internal/pipeline"
	"quarry/internal/store"
)

// fakeClock is a manually advanced Clock; every read ticks by 1 ms so
// ordering stays strict without any wall-clock dependency.
type fakeClock struct{ ms atomic.Int64 }

func (c *fakeClock) now() int64 { return c.ms.Add(1) }

func newScheduler(t *testing.T, ttl time.Duration) (*Scheduler, *store.Store, *fakeClock) {
	t.Helper()
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "quarry.db"), store.WithClock(clk.now))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st, Config{LeaseTTL: ttl}), st, clk
}

// spec is one job of a test DAG.
type spec struct {
	needs  []string
	labels map[string]string
	maxAtt int
}

// dag inserts a run whose jobs are given in declaration order and returns
// the run id plus name→job id. Roots start queued, like submitRun does.
func dag(t *testing.T, st *store.Store, order []string, specs map[string]spec) (string, map[string]string) {
	t.Helper()
	ctx := context.Background()
	run := &store.Run{ID: "run-" + t.Name(), PipelineYAML: "name: t", Trigger: "test"}
	ids := map[string]string{}
	var jobs []store.Job
	var deps []store.JobDep
	for _, name := range order {
		sp := specs[name]
		ids[name] = run.ID + "-" + name
		pj := pipeline.Job{Name: name, Image: "alpine", Steps: []string{"true"}, Needs: sp.needs, Labels: sp.labels}
		raw, _ := json.Marshal(pj)
		state := store.JobPending
		if len(sp.needs) == 0 {
			state = store.JobQueued
		}
		jobs = append(jobs, store.Job{ID: ids[name], Name: name, SpecJSON: raw, State: state, MaxAttempts: sp.maxAtt})
	}
	for _, name := range order {
		for _, n := range specs[name].needs {
			deps = append(deps, store.JobDep{JobID: ids[name], NeedsJobID: ids[n]})
		}
	}
	if err := st.Tx(ctx, func(tx *sql.Tx) error { return st.CreateRun(ctx, tx, run, jobs, deps) }); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return run.ID, ids
}

func getJob(t *testing.T, st *store.Store, id string) *store.Job {
	t.Helper()
	j, err := st.GetJob(context.Background(), st.Reader(), id)
	if err != nil {
		t.Fatalf("GetJob %s: %v", id, err)
	}
	return j
}

func getRun(t *testing.T, st *store.Store, id string) *store.Run {
	t.Helper()
	r, err := st.GetRun(context.Background(), st.Reader(), id)
	if err != nil {
		t.Fatalf("GetRun %s: %v", id, err)
	}
	return r
}

func wantState(t *testing.T, st *store.Store, id, want string) {
	t.Helper()
	if got := getJob(t, st, id).State; got != want {
		t.Fatalf("job %s: state %q, want %q", id, got, want)
	}
}

func claim(t *testing.T, s *Scheduler, runner string, labels map[string]string) *store.Job {
	t.Helper()
	j, err := s.Claim(context.Background(), RunnerInfo{ID: runner, Labels: labels})
	if err != nil {
		t.Fatalf("Claim(%s): %v", runner, err)
	}
	return j
}

func complete(t *testing.T, s *Scheduler, j *store.Job, res Result) *store.Job {
	t.Helper()
	out, err := s.Complete(context.Background(), j.ID, j.Attempt, res)
	if err != nil {
		t.Fatalf("Complete(%s, %d): %v", j.Name, j.Attempt, err)
	}
	return out
}

var ok = Result{Status: store.JobSucceeded}

func failed(kind string) Result { return Result{Status: store.JobFailed, FailureKind: kind} }

// claimAll drains the queue for a runner and returns name→job.
func claimAll(t *testing.T, s *Scheduler, runner string) map[string]*store.Job {
	t.Helper()
	out := map[string]*store.Job{}
	for {
		j := claim(t, s, runner, nil)
		if j == nil {
			return out
		}
		out[j.Name] = j
	}
}

func eventTypes(t *testing.T, st *store.Store, runID string) []string {
	t.Helper()
	evs, err := st.ListEvents(context.Background(), st.Reader(), runID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range evs {
		out = append(out, e.Type)
	}
	return out
}

func count(types []string, want string) int {
	n := 0
	for _, tp := range types {
		if tp == want {
			n++
		}
	}
	return n
}

// ---- claim ---------------------------------------------------------------

func TestClaimConcurrentExactlyOneWins(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	runID, ids := dag(t, st, []string{"only"}, map[string]spec{"only": {}})

	const n = 20
	var wg sync.WaitGroup
	wins := make(chan string, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			j, err := s.Claim(context.Background(), RunnerInfo{ID: "r" + string(rune('a'+i))})
			if err != nil {
				errs <- err
				return
			}
			if j != nil {
				wins <- j.RunnerID
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	close(errs)
	for err := range errs {
		t.Errorf("Claim: %v", err)
	}
	var winners []string
	for w := range wins {
		winners = append(winners, w)
	}
	if len(winners) != 1 {
		t.Fatalf("winners = %v, want exactly one", winners)
	}
	j := getJob(t, st, ids["only"])
	if j.State != store.JobRunning || j.Attempt != 1 || j.RunnerID != winners[0] || j.LeaseExpiresAt == 0 {
		t.Fatalf("job after claim = %+v", j)
	}
	if r := getRun(t, st, runID); r.State != store.RunRunning || r.StartedAt == 0 {
		t.Fatalf("run after claim = %+v", r)
	}
	ev := eventTypes(t, st, runID)
	if count(ev, "job.claimed") != 1 || count(ev, "run.started") != 1 {
		t.Fatalf("events = %v", ev)
	}
}

func TestClaimMatchesLabelsAndOrdersByQueuedAt(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	_, ids := dag(t, st, []string{"gpu", "plain"}, map[string]spec{
		"gpu":   {labels: map[string]string{"gpu": "true"}},
		"plain": {},
	})
	if j := claim(t, s, "cpu-runner", map[string]string{"os": "linux"}); j == nil || j.ID != ids["plain"] {
		t.Fatalf("cpu runner claimed %+v, want plain", j)
	}
	if j := claim(t, s, "cpu-runner", map[string]string{"os": "linux"}); j != nil {
		t.Fatalf("cpu runner claimed %s, want nothing", j.Name)
	}
	if j := claim(t, s, "gpu-runner", map[string]string{"gpu": "true", "os": "linux"}); j == nil || j.ID != ids["gpu"] {
		t.Fatalf("gpu runner claimed %+v, want gpu", j)
	}
	rs, err := st.ListRunners(context.Background(), st.Reader())
	if err != nil || len(rs) != 2 {
		t.Fatalf("runners = %v, %v", rs, err)
	}
}

func TestClaimLeaseUsesTTL(t *testing.T) {
	s, st, clk := newScheduler(t, 10*time.Second)
	dag(t, st, []string{"a"}, map[string]spec{"a": {}})
	before := clk.ms.Load()
	j := claim(t, s, "r1", nil)
	if j.LeaseExpiresAt <= before+10_000 || j.LeaseExpiresAt > before+10_000+10 {
		t.Fatalf("lease_expires_at = %d, want ~%d", j.LeaseExpiresAt, before+10_000)
	}
}

// ---- heartbeat -----------------------------------------------------------

func TestHeartbeatExtendsOnlyLiveAttempts(t *testing.T) {
	s, st, clk := newScheduler(t, 10*time.Second)
	_, ids := dag(t, st, []string{"a", "b"}, map[string]spec{"a": {}, "b": {}})
	a := claim(t, s, "r1", nil)
	b := claim(t, s, "r1", nil)
	clk.ms.Add(5_000)

	ds, err := s.Heartbeat(context.Background(), "r1", []JobRef{
		{a.ID, a.Attempt}, {b.ID, b.Attempt + 1}, {"nope", 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{DirectiveContinue, DirectiveAbort, DirectiveAbort}
	for i, d := range ds {
		if d.Directive != want[i] {
			t.Errorf("directive[%d] = %s, want %s", i, d.Directive, want[i])
		}
	}
	if got := getJob(t, st, ids["a"]).LeaseExpiresAt; got <= a.LeaseExpiresAt {
		t.Fatalf("lease for a not extended: %d <= %d", got, a.LeaseExpiresAt)
	}
	if got := getJob(t, st, ids["b"]).LeaseExpiresAt; got != b.LeaseExpiresAt {
		t.Fatalf("lease for b changed on a stale attempt: %d != %d", got, b.LeaseExpiresAt)
	}
	// Another runner heartbeating our attempt is fenced too.
	ds, _ = s.Heartbeat(context.Background(), "r2", []JobRef{{a.ID, a.Attempt}})
	if ds[0].Directive != DirectiveAbort {
		t.Fatalf("foreign runner got %s, want abort", ds[0].Directive)
	}
	r, err := st.GetRunner(context.Background(), st.Reader(), "r2")
	if err != nil || r.State != store.RunnerOnline {
		t.Fatalf("heartbeat should register unknown runner: %v %v", r, err)
	}
}

// ---- complete: fencing ---------------------------------------------------

func TestCompleteStaleAttemptIsFenced(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	_, ids := dag(t, st, []string{"a"}, map[string]spec{"a": {maxAtt: 3}})
	a1 := claim(t, s, "r1", nil)
	// Infra failure requeues; the next claim is attempt 2.
	complete(t, s, a1, failed(store.FailureInfra))
	wantState(t, st, ids["a"], store.JobQueued)

	// Complete on a requeued (queued) job → fenced.
	if _, err := s.Complete(context.Background(), a1.ID, a1.Attempt, ok); !errors.Is(err, ErrFenced) {
		t.Fatalf("complete on queued job: err = %v, want ErrFenced", err)
	}
	a2 := claim(t, s, "r2", nil)
	if a2.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", a2.Attempt)
	}
	// The old runner's late write carries attempt 1 → fenced.
	if _, err := s.Complete(context.Background(), a1.ID, 1, ok); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale attempt: err = %v, want ErrFenced", err)
	}
	if _, err := s.Complete(context.Background(), a1.ID, 3, ok); !errors.Is(err, ErrFenced) {
		t.Fatalf("future attempt: err = %v, want ErrFenced", err)
	}
	wantState(t, st, ids["a"], store.JobRunning)
	complete(t, s, a2, ok)
	wantState(t, st, ids["a"], store.JobSucceeded)
}

func TestCompleteDuplicateIsNoop(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	runID, ids := dag(t, st, []string{"a", "b"}, map[string]spec{"a": {}, "b": {needs: []string{"a"}}})
	a := claim(t, s, "r1", nil)
	code := 0
	complete(t, s, a, Result{Status: store.JobSucceeded, ExitCode: &code})
	wantState(t, st, ids["b"], store.JobQueued)

	// Same attempt, again — even with a different result — changes nothing.
	out, err := s.Complete(context.Background(), a.ID, a.Attempt, failed(store.FailureExitCode))
	if err != nil {
		t.Fatalf("duplicate complete: %v", err)
	}
	if out.State != store.JobSucceeded {
		t.Fatalf("duplicate changed state to %s", out.State)
	}
	ev := eventTypes(t, st, runID)
	if count(ev, "job.succeeded") != 1 || count(ev, "job.queued") != 1 {
		t.Fatalf("events = %v, want b queued exactly once", ev)
	}
	if _, err := s.Complete(context.Background(), "missing", 1, ok); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown job: err = %v, want ErrNotFound", err)
	}
}

func TestCompleteRejectsBadResult(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	dag(t, st, []string{"a"}, map[string]spec{"a": {}})
	a := claim(t, s, "r1", nil)
	for _, r := range []Result{{Status: "done"}, {Status: store.JobFailed}, {Status: store.JobFailed, FailureKind: "lost_runner"}} {
		if _, err := s.Complete(context.Background(), a.ID, a.Attempt, r); err == nil {
			t.Errorf("Complete(%+v) accepted", r)
		}
	}
}

// ---- complete: advancement -----------------------------------------------

// diamond: a → {b, c} → d.
func diamond(t *testing.T, st *store.Store) (string, map[string]string) {
	return dag(t, st, []string{"a", "b", "c", "d"}, map[string]spec{
		"a": {}, "b": {needs: []string{"a"}}, "c": {needs: []string{"a"}}, "d": {needs: []string{"b", "c"}},
	})
}

func TestDiamondAdvancementAndFanIn(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	runID, ids := diamond(t, st)

	a := claim(t, s, "r1", nil)
	if claim(t, s, "r1", nil) != nil {
		t.Fatal("b/c claimable before a finished")
	}
	complete(t, s, a, ok)
	wantState(t, st, ids["b"], store.JobQueued)
	wantState(t, st, ids["c"], store.JobQueued)
	wantState(t, st, ids["d"], store.JobPending)

	bc := claimAll(t, s, "r1")
	if len(bc) != 2 {
		t.Fatalf("claimed %d jobs, want b and c", len(bc))
	}
	complete(t, s, bc["b"], ok)
	wantState(t, st, ids["d"], store.JobPending) // fan-in waits for c
	if r := getRun(t, st, runID); r.State != store.RunRunning {
		t.Fatalf("run finalized early: %s", r.State)
	}
	complete(t, s, bc["c"], ok)
	wantState(t, st, ids["d"], store.JobQueued)

	d := claim(t, s, "r1", nil)
	complete(t, s, d, ok)
	r := getRun(t, st, runID)
	if r.State != store.RunSucceeded || r.FinishedAt == 0 {
		t.Fatalf("run = %+v, want succeeded", r)
	}
	ev := eventTypes(t, st, runID)
	if count(ev, "job.queued") != 3 || count(ev, "run.finished") != 1 {
		t.Fatalf("events = %v", ev)
	}
}

func TestFailureCascadeSkipsTransitively(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	runID, ids := diamond(t, st)
	a := claim(t, s, "r1", nil)
	code := 2
	complete(t, s, a, Result{Status: store.JobFailed, FailureKind: store.FailureExitCode, ExitCode: &code})

	j := getJob(t, st, ids["a"])
	if j.State != store.JobFailed || j.FailureKind != store.FailureExitCode || j.ExitCode == nil || *j.ExitCode != 2 {
		t.Fatalf("a = %+v", j)
	}
	for _, n := range []string{"b", "c", "d"} {
		wantState(t, st, ids[n], store.JobSkipped)
	}
	if r := getRun(t, st, runID); r.State != store.RunFailed {
		t.Fatalf("run = %s, want failed", r.State)
	}
	if claim(t, s, "r1", nil) != nil {
		t.Fatal("a skipped job was claimable")
	}
	if count(eventTypes(t, st, runID), "job.skipped") != 3 {
		t.Fatalf("events = %v", eventTypes(t, st, runID))
	}
}

func TestFailureOnOneBranchSkipsFanInButNotSibling(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	runID, ids := diamond(t, st)
	complete(t, s, claim(t, s, "r1", nil), ok) // a
	bc := claimAll(t, s, "r1")
	complete(t, s, bc["b"], failed(store.FailureTimeout))
	wantState(t, st, ids["d"], store.JobSkipped) // d cannot run, decided before c finishes
	wantState(t, st, ids["c"], store.JobRunning)
	if r := getRun(t, st, runID); r.State != store.RunRunning {
		t.Fatalf("run terminal while c is running: %s", r.State)
	}
	complete(t, s, bc["c"], ok)
	if r := getRun(t, st, runID); r.State != store.RunFailed {
		t.Fatalf("run = %s, want failed", r.State)
	}
}

func TestInfraFailureRetriesUntilMaxAttempts(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	runID, ids := dag(t, st, []string{"a", "b"}, map[string]spec{"a": {maxAtt: 2}, "b": {needs: []string{"a"}}})
	a1 := claim(t, s, "r1", nil)
	complete(t, s, a1, Result{Status: store.JobFailed, FailureKind: store.FailureInfra, Error: "pull failed"})
	j := getJob(t, st, ids["a"])
	if j.State != store.JobQueued || j.RunnerID != "" || j.LeaseExpiresAt != 0 || j.Attempt != 1 {
		t.Fatalf("after infra failure: %+v", j)
	}
	wantState(t, st, ids["b"], store.JobPending)

	a2 := claim(t, s, "r1", nil)
	complete(t, s, a2, failed(store.FailureInfra)) // attempt 2 == max → failed
	j = getJob(t, st, ids["a"])
	if j.State != store.JobFailed || j.FailureKind != store.FailureInfra {
		t.Fatalf("after second infra failure: %+v", j)
	}
	wantState(t, st, ids["b"], store.JobSkipped)
	if r := getRun(t, st, runID); r.State != store.RunFailed {
		t.Fatalf("run = %s", r.State)
	}
}

func TestExitCodeFailureDoesNotRetry(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	_, ids := dag(t, st, []string{"a"}, map[string]spec{"a": {maxAtt: 3}})
	complete(t, s, claim(t, s, "r1", nil), failed(store.FailureExitCode))
	wantState(t, st, ids["a"], store.JobFailed)
}

func TestRunWithCancelledSiblingStillTerminates(t *testing.T) {
	s, st, _ := newScheduler(t, 30*time.Second)
	runID, ids := dag(t, st, []string{"a", "b", "c"}, map[string]spec{
		"a": {}, "b": {}, "c": {needs: []string{"b"}},
	})
	a := claim(t, s, "r1", nil)
	// Simulate a user cancel of b (the cancel entry owns the real path).
	ctx := context.Background()
	if err := st.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE jobs SET state = ?, failure_kind = ? WHERE id = ?`,
			store.JobCancelled, store.FailureCancelled, ids["b"])
		return err
	}); err != nil {
		t.Fatal(err)
	}
	complete(t, s, a, ok)
	wantState(t, st, ids["c"], store.JobSkipped)
	r := getRun(t, st, runID)
	if r.State != store.RunCancelled || r.FinishedAt == 0 {
		t.Fatalf("run = %+v, want cancelled and finished", r)
	}
}
