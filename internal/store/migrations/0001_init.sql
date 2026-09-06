-- 0001_init: runs, jobs, job_deps, runners, events. log_chunks and
-- artifacts have their own migrations (0003, 0004).
-- All timestamps are Unix milliseconds generated in Go; SQL never
-- produces a timestamp.

CREATE TABLE runs (
    id            TEXT    PRIMARY KEY,
    source_key    TEXT    NOT NULL,
    pipeline_yaml TEXT    NOT NULL,
    commit_sha    TEXT,
    ref           TEXT,
    trigger       TEXT    NOT NULL,
    state         TEXT    NOT NULL,
    created_at    INTEGER NOT NULL,
    started_at    INTEGER,
    finished_at   INTEGER
);

CREATE INDEX runs_created_idx ON runs(created_at);

CREATE TABLE jobs (
    id               TEXT    PRIMARY KEY,
    run_id           TEXT    NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    name             TEXT    NOT NULL,
    spec_json        TEXT    NOT NULL,
    state            TEXT    NOT NULL,
    attempt          INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 1,
    runner_id        TEXT,
    lease_expires_at INTEGER,
    -- exit_code | timeout | infra | lost_runner | cancelled; NULL until failed.
    failure_kind     TEXT,
    exit_code        INTEGER,
    error            TEXT,
    queued_at        INTEGER NOT NULL,
    started_at       INTEGER,
    finished_at      INTEGER,
    UNIQUE (run_id, name)
);

CREATE INDEX jobs_state_idx ON jobs(state);
CREATE INDEX jobs_run_idx   ON jobs(run_id);

-- job_id depends on needs_job_id; both belong to the same run.
CREATE TABLE job_deps (
    job_id       TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    needs_job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, needs_job_id)
);

CREATE INDEX job_deps_needs_idx ON job_deps(needs_job_id);

CREATE TABLE runners (
    id            TEXT    PRIMARY KEY,
    name          TEXT    NOT NULL,
    labels_json   TEXT    NOT NULL DEFAULT '{}',
    capacity      INTEGER NOT NULL DEFAULT 1,
    state         TEXT    NOT NULL,
    version       TEXT    NOT NULL DEFAULT '',
    last_seen_at  INTEGER NOT NULL,
    registered_at INTEGER NOT NULL
);

CREATE TABLE events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT    NOT NULL,
    job_id      TEXT,
    type        TEXT    NOT NULL,
    detail_json TEXT    NOT NULL DEFAULT '{}',
    created_at  INTEGER NOT NULL
);

CREATE INDEX events_run_idx ON events(run_id, id);
