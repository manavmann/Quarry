-- 0005_cancel: a cancel request outlives the HTTP call that made it and is
-- delivered to the runner on its next heartbeat. cancel_reason says who
-- asked: 'user' (POST /api/runs/{id}/cancel) or 'timeout' (the lease
-- monitor's backstop for a runner that failed to enforce the job timeout).
-- NULL means no cancel was requested. Timestamps are Unix ms from Go.
ALTER TABLE jobs ADD COLUMN cancel_requested_at INTEGER;
ALTER TABLE jobs ADD COLUMN cancel_reason TEXT;
