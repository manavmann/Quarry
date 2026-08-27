# Progress

Read this first each session. Newest entry on top.

## C09 · cli: quarry run/runs/status/watch/logs/cancel/runners/events — done

- Landed: `internal/cli` (cobra, approved stack; `go.mod` gains
  `spf13/cobra` + indirect `pflag`, `mousetrap`) and `cmd/quarry` calling
  `cli.Main()`. `client.go`: own wire types (never links `api`/`store`),
  bearer auth, capped exponential retry on transport errors and
  502/503/504 (5 retries from 200 ms), `APIError` with request id.
  `bundle.go`: `Bundle(w, dir)` tars the workspace with slash-relative
  names in lexical order, skips any `.git` entry at any depth, records
  symlinks as symlink entries (never followed). `render.go`: `dagOrder`
  (Kahn over `spec.needs`, declaration tie-break — same walk as
  `pipeline.TopoOrder`, which can't be reused on spec JSON), `renderJobs`
  (JOB/STATE/ATTEMPT/RUNNER/DURATION, `failed (exit N|kind)`, `a/max`
  when max>1), runs/runners/events tables; durations from an injected
  Unix-ms `now`. Commands: `run` (bundle → temp tar → multipart
  `pipeline`+`source` streamed per attempt, `--wait`, `-C`, `-f`), `runs
  -n`, `status`, `watch` (redraws only on change, exit 1 unless
  succeeded), `logs [-f] [--attempt]` (job state read *before* chunks;
  exits when terminal and a read returns nothing), `events [-f]`,
  `runners`, `cancel`. `QUARRY_SERVER`/`QUARRY_TOKEN`, `--interval`.
  `internal/api.handleSubmitRun` now also accepts `multipart/form-data`
  (`pipeline` part parsed, `source` drained and discarded, body bounded by
  `MaxPipelineBytes+MaxSourceBytes` (256 MiB)); raw-YAML path unchanged.
  Tests: bundle excludes `.git` (root and nested, `.gitignore` kept),
  symlinks not followed (skips on Windows without the privilege),
  deterministic bytes; job/runs/runners/events table snapshots; DAG
  tie-break; api multipart submit (201, 400 without pipeline part).
  Verified manually against a local server + fake runner: run --wait,
  runs, status, watch, logs -f, events, runners, retry exhaustion, 404s.
- Flaky: nothing.
- Next: C10.
known gap: `quarry cancel` posts `/api/runs/{id}/cancel`, which is not served
yet (404) — the server cancel path still doesn't exist; `artifacts` is not
in this entry; the uploaded bundle is discarded until C10 stores/serves it;
the RUNNER column shows the runner id, not its name (one call per tick).

## C08 · logs: attempt-fenced chunk ingest, cursor reads, runner log shipper — done

- Landed: migration `0003_log_chunks` (`PRIMARY KEY (job_id, attempt,
  seq)`, `data BLOB`); `store/logs.go` (`InsertLogChunk` via `INSERT OR
  IGNORE` → inserted bool, `LogBytes`, `ListLogChunks seq > ?`) and
  `HasJobEvent`. `scheduler.AppendLogs` fences on
  `(running, attempt)` in one Tx (stale/finished → `ErrFenced`, unknown →
  `ErrNotFound`, `seq < 1` → `InvalidResultError`, whose message is now
  "invalid request"), applies `Config.LogCapBytes` (default 10 MiB,
  `QUARRY_LOG_CAP`): the crossing chunk is cut, later chunks stored empty
  so redelivery still dedups, `job.logs_truncated {attempt, cap_bytes}`
  recorded exactly once (`HasJobEvent` covers the exact-fit edge).
  `api/logs.go`: `POST /api/runner/jobs/{id}/logs` (1 MiB body, base64
  `data`, 204/409/404/400) and `GET /api/jobs/{id}/logs?after=&attempt=`
  → `{attempt, chunks, next}` with strictly-greater-than cursor; `next` =
  last seq returned, else the `after` asked for. `internal/logship`: an
  `io.Writer` shipper — 250 ms / 64 KiB flush, seq assigned once at seal
  (retries resend the identical batch), capped exponential backoff,
  `Sink` contract (`ErrStale` = 409 → buffer dropped + `OnStale` once;
  `ErrRejected` = other 4xx → dropped, reported by `Close`), 512 KiB per
  POST, 16 MiB buffer cap with a `[quarry] log shipper dropped N bytes`
  marker, `Close(ctx)` flushes synchronously (done ctx = discard). Agent:
  one shipper per attempt is the executor's writer; `OnStale` cancels the
  attempt with `errAbort`; the verdict (`context.Cause`) is snapshotted
  right after `Run` returns, then the shipper is closed under
  `LogFlushTimeout` (30 s) **before** `complete` — a 409 met by the final
  flush never becomes an abort (A4 bug 1); `Config.LogFlush{Interval,
  Bytes,Timeout}`. `docs/protocol.md` documents both endpoints.
  Tests: store dedup/cursor; scheduler ordered+deduped+fenced, cap
  truncates once, exact-fit-then-overflow; api ingest/read/409/404/400;
  logship flush-on-close, size threshold, interval, retry resends
  identical seqs, stale, rejected, Close ctx bound, done-ctx discard,
  buffer cap marker; agent logs-before-complete-never-after, logs 409
  kills a running attempt, final-flush 409 still reports; harness streams
  300 KB of fake output through the real path with gapless seqs.
- Flaky: nothing (agent/logship/harness 4× under `-race` clean).
- Next: C09.
known gap: reads return the whole tail in one response (no `limit`); no
`/api/runner/jobs/{id}/logs` rate limiting; docker executor untouched —
its `io.Writer` is simply the shipper now, but the real-executor path is
only covered by `make test-docker`, not by the unit suite.

## C07 · executor/docker: volume-per-attempt job execution with source injection — done

- Landed: `internal/executor/docker` — `New(Config{RunnerName, Source,
  KeepFailed})`, `Run` per blueprint §11: inspect → pull-if-missing (stream
  drained, only milestone lines to the log as `[quarry] …`), named volume
  `quarry-<job>-<attempt>` at `/workspace`, container of the same name
  (`WorkingDir=/workspace`, env = job env + `CI=true QUARRY_JOB QUARRY_RUN
  QUARRY_ATTEMPT` which win on clash, `--memory`/`--cpus` from
  `resources`, labels `quarry.runner/job/run/attempt` for C16's reaper, no
  socket/privileges), source tar `CopyToContainer` into `/workspace`, then a
  generated `/quarry/run.sh` (`set -e`, `echo '+ <step>'` before each step,
  single-quote escaped) copied as its own tar to `/`; attach (`Tty=false`)
  **before** start, `stdcopy` demux into the writer; `ContainerWait` with
  `WaitConditionNextExit` registered before start on a detached context
  (`NotRunning` fires at once on a created container — found in test);
  ctx cancel → `ContainerKill` + bounded wait → `ctx.Err()` (also on
  pre-start failures once ctx is done); `Result{ExitCode, OOMKilled,
  Duration}`; container then volume removed in defers on a detached 30 s
  context, skipped for failed attempts when `KeepFailed`.
  `executor.Result` gained `OOMKilled` (agent appends "(out of memory)" to
  the exit-code error). `agent.SourceFetcher(cfg)` does `GET
  /api/runs/{id}/source` for the executor; **404 → empty workspace** until
  C10 serves bundles (endpoint not added: `internal/api` untouched).
  `cmd/runner`: `QUARRY_EXECUTOR=docker`, `QUARRY_KEEP_FAILED=1`.
  `github.com/docker/docker v28.5.2` added (approved stack). Tests behind
  `//go:build docker` (`make test-docker`, alpine:3.20): echo job incl.
  env + first-line capture + stderr, non-zero exit + `set -e`, timeout
  kill, source visible in `/workspace`, step with quotes, cleanup on
  success/failure/kill/pull-error + `KeepFailed`; untagged unit tests for
  `runScript`/`env`/`parseMemory`/`hostConfig`. Docker suite ~12 s, 3×
  under `-race` clean.
- Flaky: nothing. stdout/stderr order across the two pipes is not fixed;
  tests only assert order within stdout.
- Next: C08.
known gap: no `/api/runs/{id}/source` endpoint yet (C10); the lease
monitor / `lost_runner` expiry mentioned in C06's gap is still open (it was
never part of this entry); OOM has no dedicated `failure_kind`, it is an
`exit_code` failure with `OOMKilled` in the executor result.

## C06 · agent: runner loop, executor interface, fake executor, in-process harness — done

- Landed: `internal/executor` — one-method `Executor` (`Run(ctx, JobSpec,
  io.Writer) (Result, error)`; nil error + exit code = ran, error = infra,
  kill via ctx), `FakeExecutor` scripted per job name (exit code, infra
  error, delay, hang-until-`Release`, log bytes) with an `Executions()`
  record. `internal/agent` — HTTP-only client with its own wire types
  (runner binary never links store/api), `POST /api/runner/register`
  called once at startup before any claim (name → server-assigned
  `runner_id`, idempotent per name via unique `runners.name` in migration
  0002; transport/5xx retried with backoff, 4xx fatal), poll loop with
  ±25% jitter and a capacity semaphore (a full runner does not poll), one
  goroutine per (job,attempt), heartbeat goroutine
  honouring `abort` (kill, report nothing) and `cancel` (kill, report
  `cancelled`), per-job timeout → `timeout`, executor error → `infra`,
  complete with exponential retry on transport/5xx (409/4xx final), SIGTERM
  drain kills attempts and reports `infra("runner shutting down")`.
  `cmd/runner` env wiring (`QUARRY_SERVER`/`API_TOKEN`/`RUNNER_NAME`/`LABELS`/
  `CAPACITY`/`POLL_INTERVAL`/`HEARTBEAT_INTERVAL`/`EXECUTOR=fake`).
  `internal/harness` — `New(t, Opts)` (server on temp DB + httptest port,
  fake `Clock` injected into the store, N agents each with a FakeExecutor),
  `Client()`, `Submit`, `WaitRun`, `WaitJob`, `Events`, `EventTypes`,
  `Script`/`Release`/`Executions`, `Kill`/`Restart`, `AgentID` (assigned
  id), `WaitFor` deadline polling; teardown in `t.Cleanup`. Tests: diamond
  across 3 agents, each job once under 5-agent contention, failing job
  skips dependents, infra failure, label routing, kill/restart keeps the
  same `runner_id`; harness suite ~2.5 s. `internal/api` gained
  `handleRegister` + test; `internal/store` gained `GetRunnerByName`.
- Flaky: nothing (agent+harness 5× under `-race` clean).
- Next: C07.
known gap: logs go to `io.Discard` until C08; lease TTL is wired but never
expires (fake clock, no monitor) until C07; a killed runner reports `infra`
which is terminal while `max_attempts=1`.

## C05 · scheduler: atomic claim, heartbeat leases, fenced completion, DAG advancement — done

- Landed: `docs/protocol.md` (runner protocol + fencing/advancement rules);
  `internal/scheduler` — `Claim` (runner upsert, queued jobs by `queued_at`,
  labels ⊆ in Go, guarded `state=queued` update, attempt+1, lease, `run.started`),
  `Heartbeat` (lease extend where `running AND attempt AND runner_id`,
  `continue`/`abort`), `Complete` (fence → terminal or infra-requeue below
  `max_attempts` → fixpoint advancement pending→queued/skipped → run
  finalization, all one Tx; duplicate = no-op, else `ErrFenced`). Store gained
  guarded primitives in `jobs.go` and `_txlock=immediate` (BEGIN IMMEDIATE).
  `internal/api`: `POST /api/runner/{claim,heartbeat,jobs/{id}/complete}`
  (204/409/404/400). `cmd/server`: `QUARRY_LEASE_TTL` (default 30s).
- Flaky: nothing.
- Next: C06.
known gap: `submitRun` still sets `max_attempts=1` (blueprint default 3), so
infra retry is inert until that lands; `cancel` directive is reserved, no
cancel path yet; lease expiry/lost_runner is C07.

## C04 · api: run submission, run/job/event reads, server main — done

- Landed: `internal/api` — stdlib mux, bearer middleware (constant-time,
  401 on `/api/*`), `X-Request-ID` middleware, `POST /api/runs` (raw inline
  YAML body, 400 on parse/validation, 413 over 1 MiB), `GET /api/runs?limit=`,
  `GET /api/runs/{id}` (run + jobs), `GET /api/runs/{id}/events?after=`,
  `GET /api/jobs/{id}`, `GET /api/runners`, `GET /healthz`. `submit.go` maps
  pipeline → store rows in one Tx: roots `queued`, rest `pending`, deps,
  `run.created` event. `cmd/server`: `QUARRY_LISTEN`/`QUARRY_DB`/
  `QUARRY_API_TOKEN` (required), SIGINT/SIGTERM graceful shutdown.
- Flaky: nothing.
- Next: C05.
known gap: `cancel`, `source`, `logs`, `artifacts`, `/metrics` deferred to
C05/C08/C10 (need scheduler transitions or tables that don't exist yet).

## C03 · store: SQLite schema, migrations, runs/jobs/runners/events — done

- Landed: `internal/store` over `modernc.org/sqlite` — single-conn pool,
  WAL/busy_timeout/foreign_keys pragmas via DSN, embedded migrations
  (`migrations/0001_init.sql`: runs, jobs, job_deps, runners, events;
  each applied in its own tx with a `schema_migrations` row), `Tx(ctx, fn)`,
  `CreateRun`, `Get/List{Run,Job,JobDeps,Runner,Events}`, `UpsertRunner`,
  `AppendEvent`. Injected `Clock` (Unix ms); `Reader()` for reads outside a tx.
- Flaky: nothing.
- Next: C04.
known gap: runner `state` values not pinned by blueprint — store defaults to `online`; revisit in C05.

## C02 · pipeline: parse and validate .quarry.yml, build DAG — done

- Landed: `internal/pipeline` — `Parse([]byte)` (yaml.v3, unknown fields
  rejected), `Pipeline`/`Job`/`Resources` types, 30m default timeout,
  `ValidationError` listing every problem with job names, Kahn cycle
  detection that names the jobs in the cycle, `TopoOrder()` (declaration-
  order tie-break), `Roots()`, `Dependents()`, `Job()`. `gopkg.in/yaml.v3`
  added to `go.mod`.
- Flaky: nothing.
- Next: C03.
known gap: unknown-field errors report field/line, not job name (yaml.v3 raw message).

## C01 · init: module layout, Makefile, CI workflow, CLAUDE.md — done

- Landed: `go.mod` (module `quarry`), `cmd/{server,runner,quarry}` stubs that
  print `<name> <version>`, `internal/version` (ldflags-stamped `Version`),
  `Makefile` (`build test test-docker lint`), `.github/workflows/ci.yml`
  (gofmt + `go vet`, `go test -race ./...`, `make build`), `CLAUDE.md`,
  README stub.
- Flaky: nothing.
- Next: C02.
