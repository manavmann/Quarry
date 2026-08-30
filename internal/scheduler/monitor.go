package scheduler

import (
	"context"
	"database/sql"
	"log"
	"time"

	"quarry/internal/store"
)

// TickReport says what one monitor pass changed.
type TickReport struct {
	Requeued       []string // job ids sent back to the queue
	Failed         []string // job ids failed as lost_runner
	RunnersOffline []string // runner ids newly marked offline
}

// Tick runs one lease-monitor pass in a single transaction: every running
// job whose lease expired before the store's now is requeued (attempt
// below max_attempts) or failed as lost_runner with its DAG advanced, and
// every online runner silent for RunnerOfflineAfter is marked offline.
// A tick is idempotent: nothing it changes is selected by the next one.
func (s *Scheduler) Tick(ctx context.Context) (TickReport, error) {
	var rep TickReport
	err := s.st.Tx(ctx, func(tx *sql.Tx) error {
		rep = TickReport{}
		now := s.st.Now()
		expired, err := s.st.ListExpiredLeases(ctx, tx, now)
		if err != nil {
			return err
		}
		for i := range expired {
			job := &expired[i]
			detail := map[string]any{"attempt": job.Attempt, "failure_kind": store.FailureLostRunner,
				"runner_id": job.RunnerID, "lease_expires_at": job.LeaseExpiresAt}
			msg := "lease expired"
			if job.RunnerID != "" {
				msg += " (runner " + job.RunnerID + ")"
			}
			if retryable(store.FailureLostRunner) && job.Attempt < job.MaxAttempts {
				if _, err := s.st.RequeueJob(ctx, tx, job.ID, job.Attempt, msg); err != nil {
					return err
				}
				if err := s.event(ctx, tx, job.RunID, job.ID, "job.requeued", detail); err != nil {
					return err
				}
				rep.Requeued = append(rep.Requeued, job.ID)
				continue
			}
			if _, err := s.st.FinishJob(ctx, tx, job.ID, job.Attempt, store.JobFailed, store.FailureLostRunner, nil, msg); err != nil {
				return err
			}
			if err := s.event(ctx, tx, job.RunID, job.ID, "job.failed", detail); err != nil {
				return err
			}
			if err := s.advance(ctx, tx, job.RunID); err != nil {
				return err
			}
			rep.Failed = append(rep.Failed, job.ID)
		}
		rep.RunnersOffline, err = s.st.MarkRunnersOffline(ctx, tx, now-s.offlineMS)
		return err
	})
	return rep, err
}

// RunMonitor calls Tick every MonitorInterval until ctx ends. The caller
// owns the goroutine it runs on; the ticker only paces the loop, every
// timestamp comes from the store's clock. A failed tick is logged and the
// next one retries it: nothing is lost, only delayed.
func (s *Scheduler) RunMonitor(ctx context.Context, logger *log.Logger) {
	if logger == nil {
		logger = log.Default()
	}
	t := time.NewTicker(s.monitorEach)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rep, err := s.Tick(ctx)
		if err != nil {
			if ctx.Err() == nil {
				logger.Printf("lease monitor: %v", err)
			}
			continue
		}
		if len(rep.Requeued)+len(rep.Failed)+len(rep.RunnersOffline) > 0 {
			logger.Printf("lease monitor: requeued %v, failed %v, runners offline %v", rep.Requeued, rep.Failed, rep.RunnersOffline)
		}
	}
}
