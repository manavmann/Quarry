Read `docs/progress.md` before starting any session.

## What this is

Quarry is a distributed CI/CD platform in Go: a fleet of pull-based
runners executes DAG pipelines in isolated Docker containers under
lease-based scheduling that survives runner crashes and control-plane
restarts. Artifacts go through a pluggable `ArtifactStore`, logs are
shipped in batches from the runner, and a cobra CLI drives it all.

## Binaries

- `cmd/server` — control plane: API, scheduler, lease monitor, store
- `cmd/runner` — agent: claims jobs, runs the executor, ships logs
- `cmd/quarry` — CLI (cobra): run/watch/logs/cancel/artifacts/runners/events

## Package map

- `internal/pipeline` — parse/validate `.quarry.yml`, build DAG, cycle detection
- `internal/store` — SQLite schema + migrations; every write goes through here
- `internal/api` — HTTP handlers: user endpoints + `/api/runner/*` protocol
- `internal/scheduler` — atomic claim, heartbeat leases, fenced completion, DAG advancement
- `internal/agent` — runner poll loop, per-(job,attempt) goroutines, heartbeat
- `internal/executor` — `Executor` interface; `docker/` (real) and `fake.go` impls
- `internal/logship` — runner-side batching log shipper
- `internal/artifact` — `ArtifactStore` interface; `local/` and `cairn/` backends
- `internal/cli` — quarry CLI command implementations
- `internal/harness` — in-process test harness (server + N fake agents)
- `internal/metrics` — Prometheus registries
- `internal/version` — build version stamped via ldflags; the one thing all three binaries share

## Invariants — no exceptions

- Every runner-side write checks `(state='running', attempt)`.
- Every state transition happens inside one `store` transaction.
- `Executor` stays a one-method interface; kill via context, not a flag.
- Job containers never get the Docker socket or `Privileged`.
- Docker tests stay behind the `docker` build tag.
- Timestamps: Unix milliseconds, generated in Go. Never `CURRENT_TIMESTAMP`, never strings.
- No `time.Now()` in `scheduler` or the lease monitor — injected/fake clock only.
- No goroutine without a context and a documented owner.
- No `time.Sleep` in tests — fake clock or deadline polling.
- `FakeExecutor` and `DockerExecutor` share tests where possible.
- New tables only via migration.
- Config only via env vars, defaults in one place.
- No new dependency beyond the approved stack without a `design-decisions.md` entry.
- Every README claim must correspond to a passing test.

## Commands

- `make build` — build all three binaries
- `make test` — unit + harness suites, `-race`
- `make test-docker` — real executor tests (requires Docker; build tag `docker`)
- `make lint`
- Harness: `internal/harness` spins up a server + N in-process fake agents on a temp DB/port — use it for anything that isn't executor-specific.

## Dependency policy

Approved: `modernc.org/sqlite`, Docker Engine Go SDK, `gopkg.in/yaml.v3`,
`prometheus/client_golang`, `spf13/cobra`. Everything else is hand-written
because it *is* the project. Adding anything not on this list requires a
`docs/design-decisions.md` entry first.

## Session rules

- Context per session: this file, `docs/progress.md`, the roadmap entry, and the named package(s). Never "read the repo."
- Don't touch `web/`, `deploy/`, `docs/` unless the task is about them.
- Don't touch `internal/executor/docker` unless the task is executor work — it's the slowest thing to reason about.
- Name files explicitly before editing. No `grep -r` beyond the named packages. Require a plan with a file list before any edit.
- If a refactor seems needed: stop and say why — do not do it.
- Fresh session per commit, and after any session touching >10 files.
- Stronger reasoning for C05, C07, C12–C15, C19, and any bug involving attempts or leases. Cheaper model is fine for C01, C03, C09, C11, C16, C18, C20, C21, C24.

## Per-commit checklist

1. Targeted tests for the touched package(s) first (`go test ./internal/<pkg> -run ...`), then the full unit suite with `-race`.
2. Docs touched only if behavior changed.
3. `docs/progress.md` updated: what landed, what's flaky, next roadmap entry.
4. End the session with a ≤10-line summary: files changed, tests added, any deviation from the roadmap entry.
