# Progress

Read this first each session. Newest entry on top.

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
