package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// AppendEvent records ev for its run, assigning ID and CreatedAt. Empty
// DetailJSON is stored as "{}".
func (s *Store) AppendEvent(ctx context.Context, tx *sql.Tx, ev *Event) error {
	if ev.RunID == "" || ev.Type == "" {
		return errors.New("store: AppendEvent: RunID and Type are required")
	}
	detail := string(ev.DetailJSON)
	if detail == "" {
		detail = "{}"
	}
	ev.CreatedAt = s.now()
	res, err := tx.ExecContext(ctx,
		`INSERT INTO events (run_id, job_id, type, detail_json, created_at) VALUES (?, ?, ?, ?, ?)`,
		ev.RunID, nullStr(ev.JobID), ev.Type, detail, ev.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("store: append event %s for run %s: %w", ev.Type, ev.RunID, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("store: append event: %w", err)
	}
	ev.ID = id
	ev.DetailJSON = []byte(detail)
	return nil
}

// ListEvents returns a run's events with ID > afterID in ascending ID
// order, at most limit rows (limit <= 0 means all). Clients page by
// passing the last ID they saw.
func (s *Store) ListEvents(ctx context.Context, q Querier, runID string, afterID int64, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = -1
	}
	rows, err := q.QueryContext(ctx,
		`SELECT id, run_id, job_id, type, detail_json, created_at FROM events
		 WHERE run_id = ? AND id > ? ORDER BY id LIMIT ?`, runID, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list events for %s: %w", runID, err)
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var ev Event
		var jobID sql.NullString
		var detail string
		if err := rows.Scan(&ev.ID, &ev.RunID, &jobID, &ev.Type, &detail, &ev.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: list events for %s: %w", runID, err)
		}
		ev.JobID = jobID.String
		ev.DetailJSON = []byte(detail)
		out = append(out, ev)
	}
	return out, rows.Err()
}
