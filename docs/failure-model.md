# Failure model

Draft. What Quarry does when something breaks, who decides, and what a
user sees. `docs/protocol.md` has the exact rules; every row below is
covered by a harness test named in the last column.

## Failure kinds

A failed attempt carries exactly one `failure_kind`. Retry eligibility is
decided by the kind alone — never by who reported it or how many times.

| kind          | who decides           | meaning                                  | retried below `max_attempts` |
|---------------|-----------------------|------------------------------------------|------------------------------|
| `exit_code`   | runner                | the container exited non-zero            | no — the job's fault         |
| `timeout`     | runner                | the job ran past its `timeout`           | no                           |
| `cancelled`   | runner (on directive) | a user cancelled the run                 | no                           |
| `infra`       | runner                | image pull, container setup, artifact upload, runner shutting down | yes |
| `lost_runner` | **server monitor**    | the lease expired without a completion   | yes                          |

`max_attempts` is `QUARRY_MAX_ATTEMPTS` (default 3) and is stamped on
every job when the run is submitted. A retry keeps the job's `attempt`
number until the next claim increments it, so attempt numbers count
claims, not failures. A runner cannot report `lost_runner` itself; the
server rejects it with 400.

## Scenarios

| what breaks                              | what happens                                                                                                          | user sees                                                        | test |
|------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|------|
| a step fails                             | `failed(exit_code)` at once, dependents `skipped`, run `failed`                                                       | `failed (exit 2)`, attempt 1/3                                   | `TestExitCodeFailureIsNeverRetried` |
| image pull / setup error                 | `failed(infra)` → `queued` again, up to `max_attempts`; then terminal                                                 | `job.requeued` events, attempt climbs, then `failed (infra)`     | `TestInfraFailureIsReported` |
| artifact store down                      | upload retried by the runner, then reported as `infra` → same as above                                                | `failed (infra)` after `max_attempts`, no partial artifacts       | `TestArtifactStoreFailureIsInfra` |
| runner shut down cleanly (SIGTERM)       | kills its containers, reports `infra("runner shutting down")` → requeued                                              | job requeued immediately, picked up by any runner                | `TestKillAndRestartAgent` |
| runner dies / partitioned mid-job        | heartbeats stop; after `QUARRY_LEASE_TTL` (30 s) the monitor requeues as `lost_runner`; another runner claims attempt 2 | ~30–35 s pause, `job.requeued {failure_kind: lost_runner}`, then success | `TestLostRunnerJobIsReassigned` |
| every attempt loses its lease            | at the cap the monitor writes `failed(lost_runner)` and cascades                                                      | `failed (lost_runner)`, dependents `skipped`                     | `TestLostRunnerExhaustsAttempts` |
| lost runner comes back                   | its heartbeat gets `abort`; its late `complete`/logs/artifacts get 409; it kills the container and forgets the attempt | nothing — the newer attempt's result stands                      | `TestLostRunnerJobIsReassigned`, scheduler `TestTickRequeuesExpiredLease…` |
| runner silent for 30 s                   | `offline` in `GET /api/runners`; any claim or heartbeat makes it `online` again                                       | `quarry runners` shows `offline`                                 | `TestSilentRunnerGoesOffline` |
| control plane restarts                   | leases and queue are in SQLite; runners retry with backoff; the monitor resumes on the next tick                      | a pause, no lost work                                            | compose: `run survives compose restart server` (C11) |

## The monitor

One goroutine on the server (`scheduler.RunMonitor`, owned by `cmd/server`
`run()`), ticking every 5 s. Each tick is one transaction:

1. every `running` job with `lease_expires_at < now` → `queued`
   (`attempt < max_attempts`) or `failed(lost_runner)` + DAG advancement;
2. every `online` runner with `last_seen_at < now - 30s` → `offline`.

A tick never selects what the previous tick changed, so it is idempotent,
and it reads time only from the store's injected clock — the harness
drives it with a fake clock and a 10 ms ticker.

## Timing

Reassignment latency is lease TTL + up to one monitor tick + the next
poll: 30 s + 5 s + ~1 s with defaults. Lowering `QUARRY_LEASE_TTL` speeds
recovery but a runner that misses that many heartbeats (every 5 s) is
treated as dead even if it is merely slow; its attempt is then wasted
work, never a duplicate result.

## Not covered

Exactly-once side effects (a partitioned runner may finish a superseded
attempt's `deploy` step before its next heartbeat says `abort`), OOM as a
distinct kind (it is `exit_code` with `OOMKilled` in the executor result),
user cancellation (no server path yet).
