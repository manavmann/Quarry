package scheduler

import (
	"context"
	"database/sql"
	"fmt"

	"quarry/internal/store"
)

// EventLogsTruncated is recorded once per attempt when its log hits the cap.
const EventLogsTruncated = "job.logs_truncated"

// AppendLogs stores a batch of chunks for one attempt inside one
// transaction, fenced on (state='running', attempt): a batch from a stale,
// requeued or finished attempt is ErrFenced (409), and an unknown job is
// store.ErrNotFound. Within the fence, a chunk whose seq is already stored
// is ignored, so redelivery of a batch is idempotent and out-of-order
// delivery reads back in seq order.
//
// The cap: once an attempt holds LogCapBytes, the chunk that crosses it is
// cut to fit, later chunks are stored empty (their seq is kept so a
// redelivery is still recognised as a duplicate), and EventLogsTruncated is
// recorded the first time output is lost.
func (s *Scheduler) AppendLogs(ctx context.Context, jobID string, attempt int, chunks []store.LogChunk) error {
	for _, c := range chunks {
		if c.Seq < 1 {
			return &InvalidResultError{Msg: fmt.Sprintf("log chunk seq %d must be >= 1", c.Seq)}
		}
	}
	var stored int64
	err := s.st.Tx(ctx, func(tx *sql.Tx) error {
		stored = 0
		job, err := s.st.GetJob(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.State != store.JobRunning || job.Attempt != attempt {
			return ErrFenced
		}
		total, err := s.st.LogBytes(ctx, tx, jobID, attempt)
		if err != nil {
			return err
		}
		lost := false
		for _, c := range chunks {
			data := c.Data
			if room := s.logCap - total; int64(len(data)) > room {
				if room < 0 {
					room = 0
				}
				data = data[:room]
			}
			inserted, err := s.st.InsertLogChunk(ctx, tx, jobID, attempt, c.Seq, data)
			if err != nil {
				return err
			}
			if !inserted {
				continue // duplicate delivery; the first copy stands
			}
			total += int64(len(data))
			stored += int64(len(data))
			if len(data) < len(c.Data) {
				lost = true
			}
		}
		if !lost {
			return nil
		}
		has, err := s.st.HasJobEvent(ctx, tx, job.RunID, jobID, EventLogsTruncated, attempt)
		if err != nil || has {
			return err
		}
		return s.event(ctx, tx, job.RunID, jobID, EventLogsTruncated,
			map[string]any{"attempt": attempt, "cap_bytes": s.logCap})
	})
	if err == nil {
		s.m.LogBytes.Add(float64(stored))
	}
	return err
}
