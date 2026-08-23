package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Querier is the read side shared by *sql.DB and *sql.Tx, so getters work
// both inside a Tx and against the bare store via Reader.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Reader returns a Querier for reads outside any transaction.
func (s *Store) Reader() Querier { return s.db }

// nullMillis maps the zero timestamp to NULL.
func nullMillis(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

// nullStr maps "" to NULL.
func nullStr(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// CreateRun inserts a run, its jobs and their dependency edges in one shot.
// Missing states default to pending, missing CreatedAt/QueuedAt to now and
// missing MaxAttempts to 1. Every dep must reference two jobs of this run.
func (s *Store) CreateRun(ctx context.Context, tx *sql.Tx, run *Run, jobs []Job, deps []JobDep) error {
	if run.ID == "" {
		return errors.New("store: CreateRun: run.ID is required")
	}
	if run.PipelineYAML == "" {
		return fmt.Errorf("store: CreateRun: run %s: PipelineYAML is required", run.ID)
	}
	now := s.now()
	if run.State == "" {
		run.State = RunPending
	}
	if run.CreatedAt == 0 {
		run.CreatedAt = now
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO runs (id, source_key, pipeline_yaml, commit_sha, ref, trigger, state, created_at, started_at, finished_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		run.ID, run.SourceKey, run.PipelineYAML, nullStr(run.CommitSHA), nullStr(run.Ref), run.Trigger,
		run.State, run.CreatedAt, nullMillis(run.StartedAt), nullMillis(run.FinishedAt),
	); err != nil {
		return fmt.Errorf("store: insert run %s: %w", run.ID, err)
	}

	ids := make(map[string]bool, len(jobs))
	for i := range jobs {
		j := &jobs[i]
		if j.ID == "" || j.Name == "" {
			return fmt.Errorf("store: CreateRun: job #%d needs ID and Name", i)
		}
		j.RunID = run.ID
		if j.State == "" {
			j.State = JobPending
		}
		if j.QueuedAt == 0 {
			j.QueuedAt = now
		}
		if j.MaxAttempts <= 0 {
			j.MaxAttempts = 1
		}
		ids[j.ID] = true
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO jobs (id, run_id, name, spec_json, state, attempt, max_attempts, runner_id, lease_expires_at,
			                   failure_kind, exit_code, error, queued_at, started_at, finished_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			j.ID, j.RunID, j.Name, string(j.SpecJSON), j.State, j.Attempt, j.MaxAttempts, nullStr(j.RunnerID),
			nullMillis(j.LeaseExpiresAt), nullStr(j.FailureKind), j.ExitCode, nullStr(j.Error),
			j.QueuedAt, nullMillis(j.StartedAt), nullMillis(j.FinishedAt),
		); err != nil {
			return fmt.Errorf("store: insert job %s: %w", j.Name, err)
		}
	}

	for _, d := range deps {
		if !ids[d.JobID] || !ids[d.NeedsJobID] {
			return fmt.Errorf("store: CreateRun: dep %s -> %s references a job outside run %s", d.JobID, d.NeedsJobID, run.ID)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO job_deps (job_id, needs_job_id) VALUES (?, ?)`, d.JobID, d.NeedsJobID,
		); err != nil {
			return fmt.Errorf("store: insert dep %s -> %s: %w", d.JobID, d.NeedsJobID, err)
		}
	}
	return nil
}

const runCols = `id, source_key, pipeline_yaml, commit_sha, ref, trigger, state, created_at, started_at, finished_at`

func scanRun(row interface{ Scan(...any) error }) (*Run, error) {
	var r Run
	var sha, ref sql.NullString
	var started, finished sql.NullInt64
	if err := row.Scan(&r.ID, &r.SourceKey, &r.PipelineYAML, &sha, &ref, &r.Trigger, &r.State,
		&r.CreatedAt, &started, &finished); err != nil {
		return nil, err
	}
	r.CommitSHA, r.Ref = sha.String, ref.String
	r.StartedAt, r.FinishedAt = started.Int64, finished.Int64
	return &r, nil
}

// GetRun returns the run with the given id or ErrNotFound.
func (s *Store) GetRun(ctx context.Context, q Querier, id string) (*Run, error) {
	r, err := scanRun(q.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get run %s: %w", id, err)
	}
	return r, nil
}

// ListRuns returns up to limit runs, newest first. limit <= 0 means all.
func (s *Store) ListRuns(ctx context.Context, q Querier, limit int) ([]Run, error) {
	if limit <= 0 {
		limit = -1
	}
	rows, err := q.QueryContext(ctx, `SELECT `+runCols+` FROM runs ORDER BY created_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list runs: %w", err)
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list runs: %w", err)
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

const jobCols = `id, run_id, name, spec_json, state, attempt, max_attempts, runner_id, lease_expires_at,
	failure_kind, exit_code, error, queued_at, started_at, finished_at`

func scanJob(row interface{ Scan(...any) error }) (*Job, error) {
	var j Job
	var spec string
	var runner, failure, errMsg sql.NullString
	var lease, exit, started, finished sql.NullInt64
	if err := row.Scan(&j.ID, &j.RunID, &j.Name, &spec, &j.State, &j.Attempt, &j.MaxAttempts, &runner, &lease,
		&failure, &exit, &errMsg, &j.QueuedAt, &started, &finished); err != nil {
		return nil, err
	}
	j.SpecJSON = []byte(spec)
	j.RunnerID, j.FailureKind, j.Error = runner.String, failure.String, errMsg.String
	j.LeaseExpiresAt, j.StartedAt, j.FinishedAt = lease.Int64, started.Int64, finished.Int64
	if exit.Valid {
		code := int(exit.Int64)
		j.ExitCode = &code
	}
	return &j, nil
}

// GetJob returns the job with the given id or ErrNotFound.
func (s *Store) GetJob(ctx context.Context, q Querier, id string) (*Job, error) {
	j, err := scanJob(q.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get job %s: %w", id, err)
	}
	return j, nil
}

// ListJobs returns a run's jobs in insertion (rowid) order, which is the
// order CreateRun received them.
func (s *Store) ListJobs(ctx context.Context, q Querier, runID string) ([]Job, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE run_id = ? ORDER BY rowid`, runID)
	if err != nil {
		return nil, fmt.Errorf("store: list jobs for %s: %w", runID, err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list jobs for %s: %w", runID, err)
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

// ListJobDeps returns every dependency edge among a run's jobs.
func (s *Store) ListJobDeps(ctx context.Context, q Querier, runID string) ([]JobDep, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT d.job_id, d.needs_job_id FROM job_deps d JOIN jobs j ON j.id = d.job_id
		 WHERE j.run_id = ? ORDER BY d.job_id, d.needs_job_id`, runID)
	if err != nil {
		return nil, fmt.Errorf("store: list deps for %s: %w", runID, err)
	}
	defer rows.Close()
	var out []JobDep
	for rows.Next() {
		var d JobDep
		if err := rows.Scan(&d.JobID, &d.NeedsJobID); err != nil {
			return nil, fmt.Errorf("store: list deps for %s: %w", runID, err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
