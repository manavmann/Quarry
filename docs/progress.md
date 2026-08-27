# Progress

Read this first each session. Newest entry on top.

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
