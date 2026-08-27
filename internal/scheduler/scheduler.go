// Package scheduler owns every job state transition a runner can cause:
// claiming, heartbeating and completing. Each method is one store
// transaction, and every runner-side write is fenced on
// (state='running', attempt) so a runner whose lease was reassigned can
// never touch the newer attempt. docs/protocol.md is the contract.
//
// The package never reads the wall clock: timestamps come from the
// store's injected Clock.
package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"quarry/internal/pipeline"
	"quarry/internal/store"
)

// DefaultLeaseTTL is how long a claim or heartbeat keeps a job leased.
const DefaultLeaseTTL = 30 * time.Second

// DefaultLogCapBytes bounds one attempt's stored log; output past it is
// truncated and a job.logs_truncated event is recorded once.
const DefaultLogCapBytes = 10 << 20

// ErrFenced is returned when a runner-side write fails the
// (state='running', attempt) check: the attempt is stale, the job was
// requeued, or it is not running. Callers map it to 409.
var ErrFenced = errors.New("scheduler: attempt is not the running attempt")

// InvalidResultError is returned by Complete when the reported status or
// failure kind is not one the protocol allows, and by AppendLogs for a bad
// chunk seq. Callers map it to 400.
type InvalidResultError struct{ Msg string }

func (e *InvalidResultError) Error() string { return "scheduler: invalid request: " + e.Msg }

// Heartbeat directives, one per reported job.
const (
	DirectiveContinue = "continue"
	DirectiveCancel   = "cancel" // reserved for user cancellation
	DirectiveAbort    = "abort"
)

// Config tunes the scheduler; zero values take the defaults.
type Config struct {
	LeaseTTL    time.Duration
	LogCapBytes int64
}

// Scheduler applies the runner protocol over a store.
type Scheduler struct {
	st     *store.Store
	ttl    int64 // milliseconds
	logCap int64
}

// New builds a Scheduler over st.
func New(st *store.Store, cfg Config) *Scheduler {
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultLeaseTTL
	}
	if cfg.LogCapBytes <= 0 {
		cfg.LogCapBytes = DefaultLogCapBytes
	}
	return &Scheduler{st: st, ttl: cfg.LeaseTTL.Milliseconds(), logCap: cfg.LogCapBytes}
}

// LeaseTTL is the configured lease duration.
func (s *Scheduler) LeaseTTL() time.Duration { return time.Duration(s.ttl) * time.Millisecond }

// RunnerInfo is what a runner presents when it claims.
type RunnerInfo struct {
	ID       string
	Name     string
	Labels   map[string]string
	Capacity int
	Version  string
}

// Claim registers/refreshes the runner and hands it the oldest queued job
// whose labels it satisfies, or nil when nothing matches. The candidate
// selection and the guarded update share one BEGIN IMMEDIATE transaction,
// so concurrent claimers cannot both win the same job.
func (s *Scheduler) Claim(ctx context.Context, r RunnerInfo) (*store.Job, error) {
	if r.ID == "" {
		return nil, errors.New("scheduler: Claim: runner id is required")
	}
	if r.Name == "" {
		r.Name = r.ID
	}
	var claimed *store.Job
	err := s.st.Tx(ctx, func(tx *sql.Tx) error {
		if err := s.st.UpsertRunner(ctx, tx, &store.Runner{
			ID: r.ID, Name: r.Name, Labels: r.Labels, Capacity: r.Capacity, Version: r.Version,
		}); err != nil {
			return err
		}
		queued, err := s.st.ListQueuedJobs(ctx, tx)
		if err != nil {
			return err
		}
		for i := range queued {
			job := &queued[i]
			need, err := specLabels(job)
			if err != nil {
				return err
			}
			if !labelsSubset(need, r.Labels) {
				continue
			}
			now := s.st.Now()
			attempt, ok, err := s.st.ClaimJob(ctx, tx, job.ID, r.ID, now+s.ttl)
			if err != nil {
				return err
			}
			if !ok {
				// Lost a race we cannot lose under one connection; be safe.
				continue
			}
			if ok, err := s.st.SetRunState(ctx, tx, job.RunID, store.RunPending, store.RunRunning); err != nil {
				return err
			} else if ok {
				if err := s.event(ctx, tx, job.RunID, "", "run.started", nil); err != nil {
					return err
				}
			}
			if err := s.event(ctx, tx, job.RunID, job.ID, "job.claimed",
				map[string]any{"runner_id": r.ID, "attempt": attempt}); err != nil {
				return err
			}
			claimed, err = s.st.GetJob(ctx, tx, job.ID)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// JobRef names one attempt a runner believes it holds.
type JobRef struct {
	JobID   string
	Attempt int
}

// Directive is the heartbeat reply for one JobRef.
type Directive struct {
	JobID     string
	Attempt   int
	Directive string
}

// Heartbeat refreshes the runner's last_seen_at and extends the lease of
// every listed attempt that is still (running, attempt, this runner).
// Attempts that fail the fence get abort; the runner must kill them.
func (s *Scheduler) Heartbeat(ctx context.Context, runnerID string, jobs []JobRef) ([]Directive, error) {
	if runnerID == "" {
		return nil, errors.New("scheduler: Heartbeat: runner id is required")
	}
	out := make([]Directive, 0, len(jobs))
	err := s.st.Tx(ctx, func(tx *sql.Tx) error {
		if err := s.st.TouchRunner(ctx, tx, runnerID); err != nil {
			return err
		}
		out = out[:0]
		for _, j := range jobs {
			ok, err := s.st.ExtendLease(ctx, tx, j.JobID, j.Attempt, runnerID, s.st.Now()+s.ttl)
			if err != nil {
				return err
			}
			d := DirectiveAbort
			if ok {
				d = DirectiveContinue
			}
			out = append(out, Directive{JobID: j.JobID, Attempt: j.Attempt, Directive: d})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Result is what a runner reports when an attempt ends.
type Result struct {
	Status      string // store.JobSucceeded or store.JobFailed
	FailureKind string // one of store.Failure* when Status is failed
	ExitCode    *int
	Error       string
}

// Complete records an attempt's result and, in the same transaction,
// advances the run's DAG and finalizes the run. A duplicate report for an
// attempt that already finished is a no-op; any other fence failure is
// ErrFenced. ErrNotFound is returned for an unknown job.
func (s *Scheduler) Complete(ctx context.Context, jobID string, attempt int, res Result) (*store.Job, error) {
	switch res.Status {
	case store.JobSucceeded:
	case store.JobFailed:
		switch res.FailureKind {
		case store.FailureExitCode, store.FailureTimeout, store.FailureInfra, store.FailureCancelled:
		default:
			return nil, &InvalidResultError{Msg: fmt.Sprintf("invalid failure_kind %q", res.FailureKind)}
		}
	default:
		return nil, &InvalidResultError{Msg: fmt.Sprintf("invalid status %q", res.Status)}
	}
	var out *store.Job
	err := s.st.Tx(ctx, func(tx *sql.Tx) error {
		job, err := s.st.GetJob(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.State != store.JobRunning || job.Attempt != attempt {
			if job.Attempt == attempt && isTerminal(job.State) {
				out = job // duplicate delivery: the first result stands
				return nil
			}
			return ErrFenced
		}

		switch {
		case res.Status == store.JobSucceeded:
			if _, err := s.st.FinishJob(ctx, tx, jobID, attempt, store.JobSucceeded, "", res.ExitCode, ""); err != nil {
				return err
			}
			if err := s.event(ctx, tx, job.RunID, jobID, "job.succeeded", map[string]any{"attempt": attempt}); err != nil {
				return err
			}
		case res.FailureKind == store.FailureInfra && attempt < job.MaxAttempts:
			if _, err := s.st.RequeueJob(ctx, tx, jobID, attempt, res.Error); err != nil {
				return err
			}
			if err := s.event(ctx, tx, job.RunID, jobID, "job.requeued",
				map[string]any{"attempt": attempt, "failure_kind": res.FailureKind, "error": res.Error}); err != nil {
				return err
			}
		default:
			if _, err := s.st.FinishJob(ctx, tx, jobID, attempt, store.JobFailed, res.FailureKind, res.ExitCode, res.Error); err != nil {
				return err
			}
			if err := s.event(ctx, tx, job.RunID, jobID, "job.failed",
				map[string]any{"attempt": attempt, "failure_kind": res.FailureKind, "exit_code": res.ExitCode, "error": res.Error}); err != nil {
				return err
			}
		}

		if err := s.advance(ctx, tx, job.RunID); err != nil {
			return err
		}
		out, err = s.st.GetJob(ctx, tx, jobID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// advance queues every pending job whose needs all succeeded, skips every
// pending job with a failed/cancelled/skipped need (transitively, by
// iterating to a fixpoint), then finalizes the run if no job can still
// change state. It runs inside the completion transaction.
func (s *Scheduler) advance(ctx context.Context, tx *sql.Tx, runID string) error {
	jobs, err := s.st.ListJobs(ctx, tx, runID)
	if err != nil {
		return err
	}
	deps, err := s.st.ListJobDeps(ctx, tx, runID)
	if err != nil {
		return err
	}
	state := make(map[string]string, len(jobs))
	for i := range jobs {
		state[jobs[i].ID] = jobs[i].State
	}
	needs := make(map[string][]string, len(jobs))
	for _, d := range deps {
		needs[d.JobID] = append(needs[d.JobID], d.NeedsJobID)
	}

	for changed := true; changed; {
		changed = false
		for i := range jobs {
			j := &jobs[i]
			if state[j.ID] != store.JobPending {
				continue
			}
			// A failed need decides the job even while other needs are
			// still in flight; otherwise every need must have succeeded.
			var blocked, waiting bool
			for _, n := range needs[j.ID] {
				switch state[n] {
				case store.JobSucceeded:
				case store.JobFailed, store.JobCancelled, store.JobSkipped:
					blocked = true
				default:
					waiting = true
				}
			}
			next := store.JobQueued
			switch {
			case blocked:
				next = store.JobSkipped
			case waiting:
				continue
			}
			ok, err := s.st.TransitionPendingJob(ctx, tx, j.ID, next)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("scheduler: job %s left pending under us", j.ID)
			}
			state[j.ID] = next
			changed = true
			typ := "job.queued"
			if next == store.JobSkipped {
				typ = "job.skipped"
			}
			if err := s.event(ctx, tx, runID, j.ID, typ, nil); err != nil {
				return err
			}
		}
	}

	// Finalize: a run is terminal iff no job is pending, queued or running.
	final := store.RunSucceeded
	for _, js := range state {
		switch js {
		case store.JobPending, store.JobQueued, store.JobRunning:
			return nil
		case store.JobFailed:
			final = store.RunFailed
		case store.JobCancelled:
			if final != store.RunFailed {
				final = store.RunCancelled
			}
		}
	}
	run, err := s.st.GetRun(ctx, tx, runID)
	if err != nil {
		return err
	}
	if run.State != store.RunPending && run.State != store.RunRunning {
		return nil // already terminal
	}
	if _, err := s.st.SetRunState(ctx, tx, runID, run.State, final); err != nil {
		return err
	}
	return s.event(ctx, tx, runID, "", "run.finished", map[string]any{"state": final})
}

func (s *Scheduler) event(ctx context.Context, tx *sql.Tx, runID, jobID, typ string, detail map[string]any) error {
	var raw []byte
	if detail != nil {
		var err error
		if raw, err = json.Marshal(detail); err != nil {
			return fmt.Errorf("scheduler: encode %s event: %w", typ, err)
		}
	}
	return s.st.AppendEvent(ctx, tx, &store.Event{RunID: runID, JobID: jobID, Type: typ, DetailJSON: raw})
}

func isTerminal(state string) bool {
	switch state {
	case store.JobSucceeded, store.JobFailed, store.JobCancelled, store.JobSkipped:
		return true
	}
	return false
}

// specLabels reads the labels a job requires from its stored spec.
func specLabels(j *store.Job) (map[string]string, error) {
	var spec pipeline.Job
	if err := json.Unmarshal(j.SpecJSON, &spec); err != nil {
		return nil, fmt.Errorf("scheduler: decode spec of job %s: %w", j.ID, err)
	}
	return spec.Labels, nil
}

// labelsSubset reports whether every required label is present with the
// same value in have.
func labelsSubset(need, have map[string]string) bool {
	for k, v := range need {
		if have[k] != v {
			return false
		}
	}
	return true
}
