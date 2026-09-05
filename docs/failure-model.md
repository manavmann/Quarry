# Failure model

What Quarry does when something breaks, who decides, and what a user
sees. `docs/protocol.md` has the exact rules. Every row below names the
test that pins it, as `package/file.go:TestName`; a row whose test does
not exist says so instead of naming one. All packages are under
`internal/` unless written `cmd/…`; `docker/…` tests need `make
test-docker`.

## Failure kinds

A failed attempt carries exactly one `failure_kind`. Retry eligibility is
decided by the kind alone (`scheduler.retryable`) — never by who
reported it or how many times.

| kind          | who decides           | meaning                                  | retried below `max_attempts` |
|---------------|-----------------------|------------------------------------------|------------------------------|
| `exit_code`   | runner                | the container exited non-zero            | no — the job's fault         |
| `timeout`     | runner (ctx deadline); server backstop after timeout + 30 s grace | the job ran past its `timeout` | no         |
| `cancelled`   | runner (on `cancel` directive) | a user cancelled the run; the job ends `cancelled`, not `failed` | no |
| `infra`       | runner                | image pull, container setup, artifact upload, runner shutting down | yes |
| `lost_runner` | **server monitor**    | the lease expired without a completion   | yes                          |

`max_attempts` is `QUARRY_MAX_ATTEMPTS` (default 3) and is stamped on
every job when the run is submitted. A retry keeps the job's `attempt`
number until the next claim increments it, so attempt numbers count
claims, not failures. A runner cannot report `lost_runner` itself; the
server rejects it with 400. A job with a cancel pending is never retried,
whatever kind it fails with.

Pinned by `scheduler/monitor_test.go:TestRetryableIsDecidedByFailureKindOnly`
and `scheduler/scheduler_test.go:TestCompleteRejectsBadResult`.

## Scenarios

### Jobs

| what breaks | what happens | user sees | test |
|---|---|---|---|
| a step fails | `failed(exit_code)` at once, dependents `skipped`, run `failed` | `failed (exit 2)`, attempt 1/3 | `harness/harness_test.go:TestExitCodeFailureIsNeverRetried`, `harness/harness_test.go:TestFailingJobSkipsDependents`, `scheduler/scheduler_test.go:TestFailureCascadeSkipsTransitively` |
| a step is OOM-killed | the container exits non-zero; `exit_code` with `(out of memory)` appended to the error, never retried | `failed (exit 137)` | **no test** — `docker.Run` reads `State.OOMKilled` after the wait, but no test runs a job under a memory limit and asserts the suffix |
| image pull / setup error | `failed(infra)` → `queued` again, up to `max_attempts`; then terminal | `job.requeued` events, attempt climbs, then `failed (infra)` | `harness/harness_test.go:TestInfraFailureIsReported`, `scheduler/scheduler_test.go:TestInfraFailureRetriesUntilMaxAttempts` |
| a step outruns its `timeout` | the runner's executor context expires: container killed, `failed(timeout)`, dependents `skipped`, never retried | `failed (timeout)`, attempt 1/3 | `harness/cancel_test.go:TestJobTimeoutIsEnforcedByRunner`, `agent/agent_test.go:TestAgentTimeoutReportsTimeout`, `docker/docker_test.go:TestDockerTimeoutKill` |
| a runner fails to enforce the timeout | the monitor sees `started_at + timeout + 30 s < now` and asks for a cancel (`reason: timeout`); the runner kills the job on its next heartbeat and its `cancelled` report is stored as `failed(timeout)` | `job.cancel_requested {reason: timeout}`, then `failed (timeout)` | `harness/cancel_test.go:TestTimeoutBackstopCancelsThroughHeartbeat`, `scheduler/cancel_test.go:TestTickBackstopsTimeoutAsCancelDirective` |
| log output past `QUARRY_LOG_CAP` | the crossing chunk is cut, later chunks stored empty, `job.logs_truncated` once; the job's verdict is unaffected | logs stop at 10 MiB, one event | `scheduler/scheduler_test.go:TestAppendLogsCapTruncatesOnce`, `scheduler/scheduler_test.go:TestAppendLogsCapExactFitThenOverflowRecordsEvent` |

### Cancellation

| what breaks | what happens | user sees | test |
|---|---|---|---|
| user runs `quarry cancel` | jobs not yet started → `cancelled` at once; running attempts get `cancel` on the next heartbeat (≤ 5 s), the executor context is cancelled, the container killed, the attempt reported `failed(cancelled)` → job `cancelled`; run `cancelled` (or `failed` if a job had already failed) | `cancelled` rows, `job.cancel_requested` / `job.cancelled` events, `watch` exits 1 | `harness/cancel_test.go:TestCancelPropagatesWithinOneHeartbeat`, `harness/cancel_test.go:TestCancelUnclaimedRunIsImmediate`, `scheduler/cancel_test.go:TestCancelRunAfterAFailureStaysFailed`, `cli/e2e_test.go:TestCancelEndToEnd`, `docker/docker_test.go:TestDockerCancelKill` |
| cancelled attempt reports `infra` instead | never requeued: the cancel pending wins, job `failed(infra)` and the run finalizes | `failed (infra)`, no `job.requeued` | `scheduler/cancel_test.go:TestCancelRequestedJobIsNotRequeued` |
| runner dies while a cancel is pending | the lease expires but the job is not retried: terminal `failed(lost_runner)` regardless of attempt count | `failed (lost_runner)` | `scheduler/cancel_test.go:TestCancelRequestedLeaseExpiryNeverRequeues` |

### Runners

| what breaks | what happens | user sees | test |
|---|---|---|---|
| artifact store down | upload retried by the runner, then reported as `infra` → requeued up to `max_attempts` | `failed (infra)` after `max_attempts`, no partial artifacts | `harness/harness_test.go:TestArtifactStoreFailureIsInfra`, `agent/artifacts_test.go:TestAgentArtifactUploadRetriesThenInfra`, `artifact/remote/remote_test.go:TestPutStoreDown` |
| runner shut down cleanly (SIGTERM) | kills its containers, reports `infra("runner shutting down")` → requeued; re-registering the same name gets the same `runner_id` | job requeued immediately, picked up by any runner | `harness/harness_test.go:TestKillAndRestartAgent`, `agent/agent_test.go:TestAgentShutdownReportsInfra` |
| runner dies / partitioned mid-job | heartbeats stop; after `QUARRY_LEASE_TTL` (30 s) the monitor requeues as `lost_runner`; another runner claims attempt 2 | ~30–35 s pause, `job.requeued {failure_kind: lost_runner}`, then success | `harness/lease_test.go:TestLostRunnerJobIsReassigned`, `scheduler/monitor_test.go:TestTickRequeuesExpiredLeaseAndFencesTheOldAttempt` |
| every attempt loses its lease | at the cap the monitor writes `failed(lost_runner)` and cascades | `failed (lost_runner)`, dependents `skipped` | `harness/lease_test.go:TestLostRunnerExhaustsAttempts`, `scheduler/monitor_test.go:TestTickFailsLostRunnerAtMaxAttemptsAndCascades` |
| lost runner comes back (zombie) | its heartbeat gets `abort`; it kills the container, discards its buffer, sends nothing more, frees the slot. Its late `complete`/logs/artifacts, if any got out first, get 409 | nothing — the newer attempt's result stands | `harness/lease_test.go:TestZombieRunnerAbortsSupersededAttempt`, `agent/agent_test.go:TestAgentAbortDirectiveKillsAndDiscards`, `scheduler/scheduler_test.go:TestCompleteStaleAttemptIsFenced`, `scheduler/scheduler_test.go:TestHeartbeatExtendsOnlyLiveAttempts` |
| zombie's container finished before its heartbeat | the final log flush and `complete` hit the fence (409); the verdict was already fixed so the flush 409 does not become an abort, `complete` is sent once and not retried | nothing | `agent/agent_test.go:TestAgentLogs409AfterFinishDoesNotSuppressComplete`, `agent/agent_test.go:TestAgentFencedCompleteIsNotRetried` |
| zombie ships a log batch / uploads an artifact | 409 up front, nothing stored; the runner aborts the attempt | nothing | `agent/agent_test.go:TestAgentLogs409AbortsAttempt`, `agent/artifacts_test.go:TestAgentArtifactUpload409Aborts`, `api/artifacts_test.go:TestArtifactUploadIsFencedAndValidated`, `scheduler/scheduler_test.go:TestAppendLogsOrderedDedupedAndFenced` — **unit-level only** for the artifact path: the agent test runs against a stub server; no harness test drives finished container → artifact 409 → abort end to end (the harness zombie test takes the heartbeat-abort path) |
| fence lost *between* the artifact object write and its row | the row transaction fails, the object just written is deleted, 409 | nothing | **no test** — `api/artifacts_test.go:TestArtifactUploadIsFencedAndValidated` covers the up-front fence and "no rows after a rejected upload", not a requeue that lands inside the `Put` |
| two runners claim at the same instant | exactly one `UPDATE … WHERE state='queued'` changes a row; the other walks on | one `job.claimed` per attempt | `scheduler/scheduler_test.go:TestClaimConcurrentExactlyOneWins`, `harness/harness_test.go:TestEachJobExecutedOnceUnderContention` |
| duplicate `complete` delivery (retry after a lost reply) | second report for a terminal job with the same attempt is a 200 no-op; the first result stands | nothing | `scheduler/scheduler_test.go:TestCompleteDuplicateIsNoop` |
| runner silent for 30 s | `offline` in `GET /api/runners`; any claim or heartbeat makes it `online` again | `quarry runners` shows `offline` | `harness/lease_test.go:TestSilentRunnerGoesOffline`, `scheduler/monitor_test.go:TestTickMarksSilentRunnersOfflineOnce` |
| SIGKILLed runner left a container and volume behind | on its next start the runner reaps every container and volume labelled `quarry.runner=<its name>` (containers first), never another runner's; skipped under `QUARRY_KEEP_FAILED` | one log line per orphan | `docker/docker_test.go:TestDockerReapOrphans`, `docker/docker_test.go:TestDockerReapKeepFailed` |
| fleet-scale runner loss under load | 50-job fan-out/fan-in on 3 runners, two hard-killed mid-run: every job succeeds within 3 attempts, no `(job, attempt)` executes twice, every requeue is `lost_runner` on a dead runner | one `run.finished`, dead runners `offline` | `harness/stress_test.go:TestStressFanOutFanInWithRunnerLoss` |

### Control plane and network

| what breaks | what happens | user sees | test |
|---|---|---|---|
| control plane restarts | SQLite preserves jobs, attempts and leases; shutdown drains in-flight requests; startup logs state counts and defers lease expiry for one full lease TTL, including leases that expired during downtime; live runners renew before anything is reassigned. Timeout backstop and offline marking are not deferred | a pause; the original attempts finish once; `watch` resumes and `logs -f` keeps its cursor | `harness/restart_test.go:TestServerRestartMidRun`, `harness/restart_test.go:TestServerRestartAfterLeaseTTL`, `scheduler/monitor_test.go:TestTickStartupGrace`, `scheduler/monitor_test.go:TestTickStartupGraceKeepsTimeoutBackstop`, `cmd/server/main_test.go:TestServerShutdownDrainsRequests`, `cmd/server/main_test.go:TestStartupStateCounts`, `cli/e2e_test.go:TestWatchReconnect`, `cli/e2e_test.go:TestLogsFollowReconnectCursor` |
| runner ↔ server network loss (runner alive) | claim, heartbeat, logs, artifact upload and `complete` retry transport errors with backoff capped at 10 s for as long as the runner is alive; only HTTP 5xx replies consume the completion budget; the lease still expires if the outage outlasts the TTL | a pause, then the attempt's result lands — or a `lost_runner` requeue if the outage was longer than the TTL | `agent/agent_test.go:TestAgentConnectionBackoff`, `agent/agent_test.go:TestAgentDeliverySurvivesConnectionLoss`, `agent/agent_test.go:TestAgentSourceReconnectAfterPartialRead`, `logship/logship_test.go:TestConnectionRetryBackoff` |
| runner shuts down during an outage | delivery is bounded by `LogFlushTimeout` (30 s) after shutdown, then the attempt is dropped; the lease expires and the monitor requeues it | `lost_runner` requeue after the TTL | `agent/agent_test.go:TestAgentShutdownDuringOutage`, `logship/logship_test.go:TestCloseContextBoundsFinalFlush` |
| runner starts before the server | `register` retries transport errors and 5xx with backoff until the server answers; a 4xx is fatal | runner appears once the server is up | `agent/agent_test.go:TestAgentRetriesRegisterUntilServerIsUp`, `agent/agent_test.go:TestAgentRegisterRejectionIsFatal` |
| CLI ↔ server network loss | `run`, `watch`, `logs -f`, downloads retry with backoff capped at 10 s; a download restarts into a temp file, never exposing a partial read; permanent errors stay final | a pause, then output resumes without duplicates | `cli/client_test.go:TestConnectionRetryBackoff`, `cli/client_test.go:TestDownloadReconnectAfterPartialRead`, `cli/client_test.go:TestReconnectKeepsPermanentErrorsFinal` |

## The monitor

One goroutine on the server (`scheduler.RunMonitor`, owned by `cmd/server`
`run()`), ticking every 5 s. Each tick is one transaction
(`scheduler.Tick`):

1. once one lease TTL has passed since this scheduler was built, every
   `running` job with `lease_expires_at < now` → `queued` (`attempt <
   max_attempts` and no cancel pending, attempt kept) or
   `failed(lost_runner)` + DAG advancement;
2. every `running` job with no cancel pending past `started_at + timeout
   + 30 s` gets a cancel request (`reason: timeout`) for its next
   heartbeat;
3. every `online` runner with `last_seen_at < now - 30 s` → `offline`.

A tick never selects what the previous tick changed, so it is idempotent,
and it reads time only from the store's injected clock — the harness
drives it with a fake clock and a 10 ms ticker. Metrics are applied only
after the tick's transaction commits.

The startup deadline is recreated from the injected clock on every boot;
it does not recover or cache job state. Only lease expiry is deferred:
timeout backstops and runner-offline detection still run during grace.
After grace, leases that were not renewed expire normally. No startup
transition requeues a job merely because its old lease expired while the
server was down.

Connection loss does not consume the agent's completion/upload HTTP-error
budget or the CLI's gateway-response budget. Final logs and completion
wait while the runner is alive; runner shutdown bounds delivery by
`LogFlushTimeout` (30 s by default). Source and artifact downloads restart
into temporary files after interrupted reads, before exposing bytes to the
consumer. HTTP fencing, permanent rejections, and job timeouts still apply.

## Timing

Reassignment latency is lease TTL + up to one monitor tick + the next
poll: 30 s + 5 s + ~1 s with defaults. Measured on the compose cluster it
is TTL + ~2 s (`docs/benchmarks.md`, `scripts/bench/runner-loss.sh`).
Lowering `QUARRY_LEASE_TTL` speeds recovery but a runner that misses that
many heartbeats (every 5 s) is treated as dead even if it is merely slow;
its attempt is then wasted work, never a duplicate result.

## Not covered

Exactly-once side effects (a partitioned runner may finish a superseded
attempt's `deploy` step before its next heartbeat says `abort`); OOM as a
distinct kind (it is `exit_code` with `OOMKilled` in the executor
result); control-plane HA; an attempt orphaned while its runner stays up
(cleanup's 30 s remove timeout expiring) waits for that runner's next
restart to be reaped.
