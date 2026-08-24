package store

import (
	"context"
	"database/sql"
	"fmt"
)

// The mutations below are the guarded primitives the scheduler composes
// inside one Tx. Each takes the state it expects to find as a WHERE clause
// and reports whether exactly one row changed, so callers never race a
// concurrent transition: the fence lives in the UPDATE, not in Go.

// ListQueuedJobs returns every claimable job, oldest queued first, so Claim
// can pick the first one whose labels a runner satisfies.
func (s *Store) ListQueuedJobs(ctx context.Context, q Querier) ([]Job, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE state = ? ORDER BY queued_at, rowid`, JobQueued)
	if err != nil {
		return nil, fmt.Errorf("store: list queued jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list queued jobs: %w", err)
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

// ClaimJob moves a queued job to running under runnerID, bumping attempt
// and setting the lease. It returns the new attempt, or ok=false when the
// job was no longer queued (someone else won).
func (s *Store) ClaimJob(ctx context.Context, tx *sql.Tx, jobID, runnerID string, leaseExpiresAt int64) (attempt int, ok bool, err error) {
	now := s.now()
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs SET state = ?, runner_id = ?, attempt = attempt + 1, lease_expires_at = ?, started_at = ?
		 WHERE id = ? AND state = ?`,
		JobRunning, runnerID, leaseExpiresAt, now, jobID, JobQueued)
	if err != nil {
		return 0, false, fmt.Errorf("store: claim job %s: %w", jobID, err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return 0, false, nil
	}
	if err := tx.QueryRowContext(ctx, `SELECT attempt FROM jobs WHERE id = ?`, jobID).Scan(&attempt); err != nil {
		return 0, false, fmt.Errorf("store: claim job %s: %w", jobID, err)
	}
	return attempt, true, nil
}

// ExtendLease pushes lease_expires_at out for a job the runner still holds.
// ok=false means the fence (running, attempt, runner) no longer matches.
func (s *Store) ExtendLease(ctx context.Context, tx *sql.Tx, jobID string, attempt int, runnerID string, leaseExpiresAt int64) (bool, error) {
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs SET lease_expires_at = ? WHERE id = ? AND state = ? AND attempt = ? AND runner_id = ?`,
		leaseExpiresAt, jobID, JobRunning, attempt, runnerID)
	if err != nil {
		return false, fmt.Errorf("store: extend lease %s: %w", jobID, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// FinishJob moves a running job at the given attempt to a terminal state,
// recording failure details and clearing the lease. ok=false means the
// fence (running, attempt) no longer matches.
func (s *Store) FinishJob(ctx context.Context, tx *sql.Tx, jobID string, attempt int, state, failureKind string, exitCode *int, errMsg string) (bool, error) {
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs SET state = ?, failure_kind = ?, exit_code = ?, error = ?, lease_expires_at = NULL, finished_at = ?
		 WHERE id = ? AND state = ? AND attempt = ?`,
		state, nullStr(failureKind), exitCode, nullStr(errMsg), s.now(), jobID, JobRunning, attempt)
	if err != nil {
		return false, fmt.Errorf("store: finish job %s: %w", jobID, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// RequeueJob puts a running job at the given attempt back at the queue
// tail for another claim: state queued, no runner, no lease, attempt kept
// (the next claim increments it). ok=false means the fence no longer
// matches.
func (s *Store) RequeueJob(ctx context.Context, tx *sql.Tx, jobID string, attempt int, errMsg string) (bool, error) {
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs SET state = ?, runner_id = NULL, lease_expires_at = NULL, started_at = NULL, error = ?, queued_at = ?
		 WHERE id = ? AND state = ? AND attempt = ?`,
		JobQueued, nullStr(errMsg), s.now(), jobID, JobRunning, attempt)
	if err != nil {
		return false, fmt.Errorf("store: requeue job %s: %w", jobID, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// TransitionPendingJob moves a pending job to queued (refreshing queued_at)
// or skipped (recording finished_at). ok=false means it was not pending.
func (s *Store) TransitionPendingJob(ctx context.Context, tx *sql.Tx, jobID, to string) (bool, error) {
	now := s.now()
	var res sql.Result
	var err error
	switch to {
	case JobQueued:
		res, err = tx.ExecContext(ctx, `UPDATE jobs SET state = ?, queued_at = ? WHERE id = ? AND state = ?`,
			JobQueued, now, jobID, JobPending)
	case JobSkipped:
		res, err = tx.ExecContext(ctx, `UPDATE jobs SET state = ?, finished_at = ? WHERE id = ? AND state = ?`,
			JobSkipped, now, jobID, JobPending)
	default:
		return false, fmt.Errorf("store: transition pending job %s: unsupported target %q", jobID, to)
	}
	if err != nil {
		return false, fmt.Errorf("store: transition pending job %s: %w", jobID, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// SetRunState moves a run from one state to another, stamping started_at
// on entry to running and finished_at on any terminal state. ok=false
// means the run was not in the expected state.
func (s *Store) SetRunState(ctx context.Context, tx *sql.Tx, runID, from, to string) (bool, error) {
	now := s.now()
	var started, finished any
	if to == RunRunning {
		started = now
	}
	if to != RunRunning && to != RunPending {
		finished = now
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE runs SET state = ?, started_at = COALESCE(?, started_at), finished_at = COALESCE(?, finished_at)
		 WHERE id = ? AND state = ?`,
		to, started, finished, runID, from)
	if err != nil {
		return false, fmt.Errorf("store: set run %s state: %w", runID, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// TouchRunner refreshes last_seen_at and marks the runner online. An
// unknown runner is registered with its id as name and no labels.
func (s *Store) TouchRunner(ctx context.Context, tx *sql.Tx, runnerID string) error {
	now := s.now()
	res, err := tx.ExecContext(ctx, `UPDATE runners SET last_seen_at = ?, state = ? WHERE id = ?`, now, RunnerOnline, runnerID)
	if err != nil {
		return fmt.Errorf("store: touch runner %s: %w", runnerID, err)
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	return s.UpsertRunner(ctx, tx, &Runner{ID: runnerID, Name: runnerID})
}
