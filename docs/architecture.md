# Architecture

First draft. `docs/protocol.md` is the precise runner protocol and
scheduling rules; this document is the map.

## Components

```
   quarry CLI ──HTTP──▶ ┌──────────────── server ────────────────┐
                        │ api ─▶ scheduler ─▶ store (SQLite/WAL)  │
                        │                    artifact store       │
                        └────────────────────▲───────────────────┘
                               /api/runner/* │ claim · heartbeat ·
                                             │ logs · artifacts · complete
        ┌──────────────┐   ┌──────────────┐  │  ┌──────────────┐
        │  runner-1    │   │  runner-2    │──┴──│  runner-3    │
        │ agent+docker │   │ agent+docker │     │ agent+docker │
        └──────┬───────┘   └──────┬───────┘     └──────┬───────┘
               └──── host Docker daemon (/var/run/docker.sock) ────┘
                       job containers: unprivileged siblings
```

- **`cmd/server`** — the control plane, one process, no external
  services. Owns the SQLite database (`internal/store`), the scheduler
  (`internal/scheduler`), the HTTP API (`internal/api`) and the artifact
  store (`internal/artifact`, local backend by default).
- **`cmd/runner`** — a stateless agent (`internal/agent`). It registers
  by name, polls `claim` while below capacity, runs each attempt through
  an `Executor` (`internal/executor/docker`, or the fake for tests),
  heartbeats, ships logs (`internal/logship`), uploads artifacts and
  reports completion. It never links `store` or `api`.
- **`cmd/quarry`** — the cobra CLI (`internal/cli`). Bundles a workspace,
  submits `.quarry.yml`, watches, reads logs/events/artifacts.

Runners are *pull-based*: the server never connects to a runner, so
runners need no inbound port and can sit behind NAT. Adding capacity is
starting another runner process with the same token.

## Life of a run

1. `quarry run` tars the workspace (`.git` excluded, symlinks not
   followed) and POSTs it with the pipeline as multipart to
   `POST /api/runs`. The server mints the run id, streams the bundle to
   `sources/<run>.tar` in the artifact store, parses and validates the
   YAML (`internal/pipeline`: unknown fields rejected, cycles named) and
   in **one transaction** inserts the run, its jobs (roots `queued`, the
   rest `pending`), their edges and a `run.created` event.
2. A runner's `claim` picks the oldest `queued` job whose labels are a
   subset of its own and flips it to `running` with `attempt+1` and a
   lease, guarded by `state='queued'` so two runners cannot take it.
3. The Docker executor creates a volume `quarry-<job>-<attempt>` at
   `/workspace`, copies the source bundle and a generated `run.sh` into
   the container, attaches, then starts it. Output streams through the
   log shipper in 250 ms / 64 KiB batches; the heartbeat goroutine
   extends the lease every 5 s and obeys `abort`/`cancel`.
4. On exit the executor copies declared `artifacts:` out of the
   container; the agent uploads them, flushes the last logs, then sends
   `complete`. The server fences the write on `(running, attempt)`,
   applies the terminal state (or requeues an `infra` failure below
   `max_attempts`), advances the DAG (`pending` → `queued` or `skipped`)
   and finalises the run — all in one transaction, so a dependent is
   queued exactly once.
5. `quarry watch` polls `GET /api/runs/{id}` and redraws the job table in
   DAG order until the run is terminal.

## Where state lives

| what                       | where                                                      |
|----------------------------|------------------------------------------------------------|
| runs, jobs, deps, runners  | SQLite tables (`internal/store/migrations`)                |
| events (`quarry events`)   | `events` table, append-only                                |
| logs                       | `log_chunks (job_id, attempt, seq)`, capped per attempt    |
| artifacts + source bundles | `ArtifactStore` (`runs/<run>/jobs/…`, `sources/<run>.tar`) |
| runner working state       | nowhere — a restarted runner re-registers under the same name and gets the same `runner_id` |

Every write to a job by a runner is fenced on `(state='running',
attempt)`; every state transition is one store transaction; timestamps
are Unix milliseconds generated in Go from an injected clock. See
`CLAUDE.md` for the full invariant list and `docs/design-decisions.md`
for why.

## Deployment shape

`deploy/docker-compose.yml` is the reference cluster: the server with a
named `quarry-data` volume (database + artifacts) and three labeled
runners sharing the host Docker daemon. The runner image runs as root to
open the socket; job containers are ordinary unprivileged siblings on
the host daemon (Docker-out-of-Docker). A runner's healthcheck is "has
it registered with the server", so `docker compose up --wait` returns
only when the fleet can take work.

## Not yet

Lease expiry / `lost_runner` monitor, the `quarry cancel` server path,
`/metrics`, artifact retention, `web/`. Tracked in `docs/progress.md`.
