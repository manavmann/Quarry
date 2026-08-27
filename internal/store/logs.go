package store

import (
	"context"
	"database/sql"
	"fmt"
)

// LogChunk is one contiguous piece of an attempt's output. Seq is assigned
// by the runner and is 1-based; chunks are totally ordered by it.
type LogChunk struct {
	Seq  int64
	Data []byte
}

// InsertLogChunk stores one chunk for (jobID, attempt). A chunk whose seq
// is already stored is ignored and inserted=false is returned, which is
// what makes redelivery of a batch idempotent. The fence on
// (state='running', attempt) is the caller's job (scheduler.AppendLogs).
func (s *Store) InsertLogChunk(ctx context.Context, tx *sql.Tx, jobID string, attempt int, seq int64, data []byte) (inserted bool, err error) {
	if data == nil {
		data = []byte{} // NOT NULL column; an empty chunk is a valid row
	}
	res, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO log_chunks (job_id, attempt, seq, data) VALUES (?, ?, ?, ?)`,
		jobID, attempt, seq, data)
	if err != nil {
		return false, fmt.Errorf("store: insert log chunk %s/%d/%d: %w", jobID, attempt, seq, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// LogBytes is the total stored size of an attempt's log, for the cap.
func (s *Store) LogBytes(ctx context.Context, q Querier, jobID string, attempt int) (int64, error) {
	var n int64
	err := q.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(LENGTH(data)), 0) FROM log_chunks WHERE job_id = ? AND attempt = ?`,
		jobID, attempt).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: log bytes %s/%d: %w", jobID, attempt, err)
	}
	return n, nil
}

// ListLogChunks returns an attempt's chunks with seq > afterSeq in seq
// order. Cursor semantics are strictly greater-than: passing the last seq
// a caller saw never returns that chunk again.
func (s *Store) ListLogChunks(ctx context.Context, q Querier, jobID string, attempt int, afterSeq int64) ([]LogChunk, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT seq, data FROM log_chunks WHERE job_id = ? AND attempt = ? AND seq > ? ORDER BY seq`,
		jobID, attempt, afterSeq)
	if err != nil {
		return nil, fmt.Errorf("store: list log chunks %s/%d: %w", jobID, attempt, err)
	}
	defer rows.Close()
	var out []LogChunk
	for rows.Next() {
		var c LogChunk
		if err := rows.Scan(&c.Seq, &c.Data); err != nil {
			return nil, fmt.Errorf("store: list log chunks %s/%d: %w", jobID, attempt, err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
