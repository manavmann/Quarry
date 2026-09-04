package scheduler

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"quarry/internal/store"
)

// TickReport says what one monitor pass changed.
type TickReport struct {
	Requeued       []string // job ids sent back to the queue
	Failed         []string // job ids failed as lost_runner
	TimedOut       []string // job ids asked to cancel by the timeout backstop
	RunnersOffline []string // runner ids newly marked offline
}

// Tick runs one lease-monitor pass in a single transaction: every running
// job whose lease expired before the store's now is requeued (attempt
// below max_attempts and no cancel pending) or failed as lost_runner with its DAG advanced;
// every running job past its timeout plus TimeoutGrace gets a cancel
// request (the runner enforces the timeout itself, this only backstops
// one that did not); and every online runner silent for
// RunnerOfflineAfter is marked offline. A tick is idempotent: nothing it
// changes is selected by the next one.
func (s *Scheduler) Tick(ctx context.Context) (TickReport, error) {
	var rep TickReport
	var tr transitions
	var expiredN int
	err := s.st.Tx(ctx, func(tx *sql.Tx) error {
		rep, tr, expiredN = TickReport{}, transitions{}, 0
		now := s.st.Now()
		// Even leases already expired during downtime get one full TTL for
		// live runners to renew. Timeout and offline checks still run.
		var expired []store.Job
		if now >= s.leaseGraceUntil {
			var err error
			expired, err = s.st.ListExpiredLeases(ctx, tx, now)
			if err != nil {
				return err
			}
		}
		for i := range expired {
			job := &expired[i]
			expiredN++
			detail := map[string]any{"attempt": job.Attempt, "failure_kind": store.FailureLostRunner,
				"runner_id": job.RunnerID, "lease_expires_at": job.LeaseExpiresAt}
			msg := "lease expired"
			if job.RunnerID != "" {
				msg += " (runner " + job.RunnerID + ")"
			}
			if retryable(store.FailureLostRunner) && job.Attempt < job.MaxAttempts && job.CancelRequestedAt == 0 {
				if _, err := s.st.RequeueJob(ctx, tx, job.ID, job.Attempt, msg); err != nil {
					return err
				}
				tr.enter(store.JobQueued)
				if err := s.event(ctx, tx, job.RunID, job.ID, "job.requeued", detail); err != nil {
					return err
				}
				rep.Requeued = append(rep.Requeued, job.ID)
				continue
			}
			if _, err := s.st.FinishJob(ctx, tx, job.ID, job.Attempt, store.JobFailed, store.FailureLostRunner, nil, msg); err != nil {
				return err
			}
			tr.finished(store.JobFailed, job.StartedAt, now)
			if err := s.event(ctx, tx, job.RunID, job.ID, "job.failed", detail); err != nil {
				return err
			}
			if err := s.advance(ctx, tx, job.RunID, &tr); err != nil {
				return err
			}
			rep.Failed = append(rep.Failed, job.ID)
		}
		running, err := s.st.ListRunningJobs(ctx, tx)
		if err != nil {
			return err
		}
		for i := range running {
			job := &running[i]
			spec, err := jobSpec(job)
			if err != nil {
				return err
			}
			if spec.Timeout <= 0 || job.StartedAt == 0 || job.StartedAt+spec.Timeout.Milliseconds()+s.graceMS >= now {
				continue
			}
			if ok, err := s.st.RequestJobCancel(ctx, tx, job.ID, store.CancelReasonTimeout); err != nil {
				return err
			} else if !ok {
				continue
			}
			if err := s.event(ctx, tx, job.RunID, job.ID, "job.cancel_requested", map[string]any{
				"attempt": job.Attempt, "runner_id": job.RunnerID, "reason": store.CancelReasonTimeout,
				"timeout_ms": spec.Timeout.Milliseconds(), "started_at": job.StartedAt}); err != nil {
				return err
			}
			rep.TimedOut = append(rep.TimedOut, job.ID)
		}
		rep.RunnersOffline, err = s.st.MarkRunnersOffline(ctx, tx, now-s.offlineMS)
		return err
	})
	if err == nil {
		tr.apply(s.m)
		s.m.LeaseExpirations.Add(float64(expiredN))
	}
	return rep, err
}

// RunMonitor calls Tick every MonitorInterval until ctx ends. The caller
// owns the goroutine it runs on; the ticker only paces the loop, every
// timestamp comes from the store's clock. A failed tick is logged and the
// next one retries it: nothing is lost, only delayed.
func (s *Scheduler) RunMonitor(ctx context.Context, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
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
				logger.Error("lease monitor tick failed", "err", err)
			}
			continue
		}
		if len(rep.Requeued)+len(rep.Failed)+len(rep.TimedOut)+len(rep.RunnersOffline) > 0 {
			logger.Info("lease monitor", "requeued", rep.Requeued, "failed", rep.Failed, "timed_out", rep.TimedOut, "runners_offline", rep.RunnersOffline)
		}
	}
}
