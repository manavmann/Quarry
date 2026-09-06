-- 0003_log_chunks: one row per shipped log chunk. The primary key makes
-- ingest idempotent (INSERT OR IGNORE on a duplicate (job, attempt, seq))
-- and gives cursor reads (WHERE seq > ?) their order.
CREATE TABLE log_chunks (
    job_id  TEXT    NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL,
    seq     INTEGER NOT NULL,
    data    BLOB    NOT NULL,
    PRIMARY KEY (job_id, attempt, seq)
);
