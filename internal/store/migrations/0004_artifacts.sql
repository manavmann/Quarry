-- 0004_artifacts: one row per uploaded artifact file. A row exists only
-- once the bytes are in the ArtifactStore under
-- runs/<run>/jobs/<job>/<attempt>/<path>; the primary key makes a
-- redelivered upload an overwrite of the same row.
CREATE TABLE artifacts (
    job_id       TEXT    NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt      INTEGER NOT NULL,
    path         TEXT    NOT NULL,
    size_bytes   INTEGER NOT NULL,
    sha256       TEXT    NOT NULL,
    content_type TEXT    NOT NULL,
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (job_id, attempt, path)
);
