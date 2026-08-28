package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Artifact is one uploaded file of an attempt. Path is the slash-relative
// name inside the attempt's prefix; the object itself lives in the
// artifact store under runs/<run>/jobs/<job>/<attempt>/<path>.
type Artifact struct {
	JobID       string
	Attempt     int
	Path        string
	SizeBytes   int64
	SHA256      string
	ContentType string
	CreatedAt   int64
}

const artifactCols = `job_id, attempt, path, size_bytes, sha256, content_type, created_at`

// UpsertArtifact records an artifact after its bytes are stored. A row
// for the same (job, attempt, path) is replaced, so a redelivered upload
// (which rewrote the same object) lands on the same row. The fence on
// (state='running', attempt) is the caller's job. CreatedAt is set here.
func (s *Store) UpsertArtifact(ctx context.Context, tx *sql.Tx, a *Artifact) error {
	a.CreatedAt = s.now()
	_, err := tx.ExecContext(ctx,
		`INSERT INTO artifacts (`+artifactCols+`) VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (job_id, attempt, path) DO UPDATE SET
		   size_bytes = excluded.size_bytes, sha256 = excluded.sha256,
		   content_type = excluded.content_type, created_at = excluded.created_at`,
		a.JobID, a.Attempt, a.Path, a.SizeBytes, a.SHA256, a.ContentType, a.CreatedAt)
	if err != nil {
		return fmt.Errorf("store: upsert artifact %s/%d/%s: %w", a.JobID, a.Attempt, a.Path, err)
	}
	return nil
}

// GetArtifact returns one artifact row, or ErrNotFound.
func (s *Store) GetArtifact(ctx context.Context, q Querier, jobID string, attempt int, path string) (*Artifact, error) {
	var a Artifact
	err := q.QueryRowContext(ctx,
		`SELECT `+artifactCols+` FROM artifacts WHERE job_id = ? AND attempt = ? AND path = ?`,
		jobID, attempt, path).Scan(&a.JobID, &a.Attempt, &a.Path, &a.SizeBytes, &a.SHA256, &a.ContentType, &a.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get artifact %s/%d/%s: %w", jobID, attempt, path, err)
	}
	return &a, nil
}

// ListArtifacts returns an attempt's artifacts in path order.
func (s *Store) ListArtifacts(ctx context.Context, q Querier, jobID string, attempt int) ([]Artifact, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT `+artifactCols+` FROM artifacts WHERE job_id = ? AND attempt = ? ORDER BY path`, jobID, attempt)
	if err != nil {
		return nil, fmt.Errorf("store: list artifacts %s/%d: %w", jobID, attempt, err)
	}
	defer rows.Close()
	var out []Artifact
	for rows.Next() {
		var a Artifact
		if err := rows.Scan(&a.JobID, &a.Attempt, &a.Path, &a.SizeBytes, &a.SHA256, &a.ContentType, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: list artifacts %s/%d: %w", jobID, attempt, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
