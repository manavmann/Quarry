package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// UpsertRunner registers r or refreshes its name, labels, capacity, state,
// version and last_seen_at. registered_at is set on first sight only. An
// empty State defaults to online. On return r.RegisteredAt and r.LastSeenAt
// reflect the stored row.
func (s *Store) UpsertRunner(ctx context.Context, tx *sql.Tx, r *Runner) error {
	if r.ID == "" {
		return errors.New("store: UpsertRunner: runner.ID is required")
	}
	if r.Capacity <= 0 {
		r.Capacity = 1
	}
	if r.State == "" {
		r.State = RunnerOnline
	}
	labels := r.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	lb, err := json.Marshal(labels)
	if err != nil {
		return fmt.Errorf("store: encode labels for runner %s: %w", r.ID, err)
	}
	now := s.now()
	r.LastSeenAt = now
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO runners (id, name, labels_json, capacity, state, version, last_seen_at, registered_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   name = excluded.name, labels_json = excluded.labels_json, capacity = excluded.capacity,
		   state = excluded.state, version = excluded.version, last_seen_at = excluded.last_seen_at`,
		r.ID, r.Name, string(lb), r.Capacity, r.State, r.Version, now, now,
	); err != nil {
		return fmt.Errorf("store: upsert runner %s: %w", r.ID, err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT registered_at FROM runners WHERE id = ?`, r.ID).Scan(&r.RegisteredAt); err != nil {
		return fmt.Errorf("store: upsert runner %s: %w", r.ID, err)
	}
	return nil
}

const runnerCols = `id, name, labels_json, capacity, state, version, last_seen_at, registered_at`

func scanRunner(row interface{ Scan(...any) error }) (*Runner, error) {
	var r Runner
	var labels string
	if err := row.Scan(&r.ID, &r.Name, &labels, &r.Capacity, &r.State, &r.Version, &r.LastSeenAt, &r.RegisteredAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(labels), &r.Labels); err != nil {
		return nil, fmt.Errorf("decode labels: %w", err)
	}
	return &r, nil
}

// GetRunner returns the runner with the given id or ErrNotFound.
func (s *Store) GetRunner(ctx context.Context, q Querier, id string) (*Runner, error) {
	r, err := scanRunner(q.QueryRowContext(ctx, `SELECT `+runnerCols+` FROM runners WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get runner %s: %w", id, err)
	}
	return r, nil
}

// ListRunners returns every registered runner ordered by id.
func (s *Store) ListRunners(ctx context.Context, q Querier) ([]Runner, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+runnerCols+` FROM runners ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("store: list runners: %w", err)
	}
	defer rows.Close()
	var out []Runner
	for rows.Next() {
		r, err := scanRunner(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list runners: %w", err)
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// GetRunnerByName returns the runner registered under name or ErrNotFound.
// Names are unique (migration 0002), so this is how register resolves a
// name to its runner_id.
func (s *Store) GetRunnerByName(ctx context.Context, q Querier, name string) (*Runner, error) {
	r, err := scanRunner(q.QueryRowContext(ctx, `SELECT `+runnerCols+` FROM runners WHERE name = ?`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get runner by name %q: %w", name, err)
	}
	return r, nil
}
