# Progress

Read this first each session. Newest entry on top.

## C21 · readme: recruiter-facing README, demo script v2, GIF — done

- Landed: `README.md` rewritten (pitch, `docs/demo.gif` with a caption of what it shows, what's in the box, five-minute quickstart with the beats, Mermaid component diagram + life of a run, a nine-row failure table where every row links its test, benchmark numbers, CLI incl. the previously undocumented `artifacts`, pipeline format, build/test, layout, the five dependencies). `scripts/demo.sh` v2: beats `kill-runner` (first job's attempt 1 sleeps via `QUARRY_ATTEMPT`, the runner holding it is `compose kill`ed, `watch` must succeed, asserts build = attempt 2 on a different runner + a `job.requeued`/`lost_runner` event, then `compose start` and the orphan container must be reaped), `restart-server` (`compose restart server` mid-job; asserts attempt 1 and no `job.requeued`), `cancel` (rewritten on shared helpers), `remote` (profile + backend switch, artifact downloaded back out, `down` covers all profiles); `QUARRY_CLI` / `QUARRY_SERVER` / `QUARRY_LEASE_TTL` overrides; `compose build -q server runner-1` + `up -d --wait` instead of `up --build` (see Verified: the cold-cache bake race); the `remote` beat asserts the coordinator's bucket lists an object under `jobs/<package>/`. `scripts/record/main.go` (stdlib asciicast v2 recorder: merged stdout+stderr on a pipe, per-line timestamps, `-from` regexp to start the cast late) + `scripts/record-demo.sh` (recreates the cluster at TTL 10s, records the kill beat, renders with the `ghcr.io/asciinema/agg` image; nothing installed on the host) → `docs/demo.cast` (18 KB) and `docs/demo.gif` (41 s, 0.97 MB). `deploy/docker-compose.yml` gained `name: quarry` (containers `quarry-*`; a sibling `deploy/` dir in another checkout no longer shares the project — it did on this machine). CI `compose` job builds the two images first, then runs `demo.sh kill-runner` after the default beat. `.gitattributes` (`* text=auto eol=lf`, `*.gif binary`).
- Verified: all four beats end to end on Docker Desktop (default 42 s, kill-runner 77 s at TTL 30s / ~40 s at 10s, restart-server 65 s, cancel 26 s, remote 43 s with a locally built storage image); from-zero run: `demo.sh down -v`, `docker system prune`, `quarry-*:dev` + `golang:1.27-alpine` images and the build cache removed, fresh `git clone` into an empty dir with this session's patch applied, README followed literally (all four beats, the by-hand block, `make build/lint/test/test-docker`) — two real bugs found and fixed: (1) on a cold cache Compose 2.39's bake builds runner-1/2/3 (one shared tag) in parallel and two fail with `image quarry-runner:dev already exists` — `up --build` too, i.e. v1 had it; hence `build server runner-1` first everywhere (demo, CI, README, compose header); (2) with `core.autocrlf=true` the clone came out CRLF and the example's `lint` job failed on `gofmt -l` — hence `.gitattributes`. Remote beat with the local storage image: coordinator bucket holds `runs/<run>/jobs/<pkg>/1/dist/go-app`, 1,568,928 B, same sha256 as `quarry artifacts`, fetched directly from `:8090`; local store untouched; with the denied image the beat exits 1 at `compose up`; no `quarry.job` containers or `quarry-*` volumes left; every `Test…` cited in the README greps to a `func`; `go test -race -count=1 ./...`, `go vet ./...` (incl. `scripts/record`), gofmt clean. GIF frames inspected at 1/8/20/35 s.
- Flaky: nothing. Deviations: no blueprint §30 in the repo, executed from the roadmap line; vhs was the first choice and was dropped — its `latest` image ships Chromium 152 which vhs 0.12 cannot drive (no frames), and under `v0.10.0` any command printing more than a few KB stalls behind ttyd, so `watch` output never renders; the GIF is asciicast + agg instead. `ghcr.io/manavmann/distributed-object-storage:latest` is not pullable anonymously (`denied`), so the `remote` beat is documented as needing image access or `QUARRY_STORAGE_IMAGE`. Port 8080 on this machine is held by the other project's coordinator, so local verification ran with `QUARRY_PORT=18080`; the cluster is left up on that port. "A friend from zero" substituted by the prune + fresh-clone run above (same machine, `QUARRY_PORT=18080` because 8080 is taken here; README now says so). The GIF shows no port (it starts at the runner table). `scripts/bench/*.sh` still use `up -d --build --wait` (C19, untouched) and would hit the bake race on a cold cache. The cluster is left down. Next: C22.

## C20 · docs: architecture, failure model, design decisions, diagrams — done

- Landed (docs only, no code): `docs/protocol.md` rewritten against the code as of C19 — Mermaid job state machine with every attempt/retry edge (who fires it, attempt kept vs bumped, cancel-pending and max_attempts branches); `claim` corrected to "runner upsert + candidate walk + guarded UPDATE all in one Tx"; `complete` requeue rule now states all three conditions (`retryable ∧ attempt < max_attempts ∧ cancel_requested_at IS NULL`) and the columns `RequeueJob` clears; heartbeat directive table keyed on the `ExtendLease` row count; idle runners don't heartbeat; Retries gains the cancel-pending and startup-grace rules; the three ASCII sequence diagrams replaced by two Mermaid ones — runner protocol with the zombie fenced out on its heartbeat (primary path only; the finished-container-then-409 and artifact-409 variants are a text note citing their agent tests) and monitor-centric runner-loss recovery (grace `opt`, cap `alt`, offline marking). `docs/architecture.md`: Mermaid component diagram replaces the ASCII box; reaper, remote artifact backend, compose profiles, cancel columns, restart grace, benchmarks link. `docs/failure-model.md`: every row cites `package/file.go:TestName` (64 distinct tests, all verified to exist by grep); scenarios split into jobs / cancellation / runners / control plane; 16 new rows (cancel-pending infra + lease expiry, zombie variants, concurrent claim, duplicate complete, log cap, reaper, stress, network loss on each side, register-before-server, shutdown-during-outage); two rows flagged **no test**: OOM-kill suffix, and fence lost *between* artifact `Put` and row Tx (the existing API test covers only the up-front fence); the C13 finished-container → artifact-409 → abort path is cited but flagged **unit-level only** (stub server, no harness end to end). `docs/design-decisions.md` §8–§15 (15 ADRs total, inside the §21 10–15 band): retry-by-kind, tick-not-timers, silent abort, cancel-on-heartbeat + context cause (backstop-stored-as-timeout folded in), one-TTL restart grace, reconnect-while-alive, reaper-by-own-name, harness fault injection; a metrics-after-commit entry was written and dropped to stay in band — the mechanism is in architecture.md Observability.
- Verified: every `Test…` name in all four docs greps to a `func Test…(` in `internal/` or `cmd/`; Mermaid blocks scanned for `;`, stray `<`/`>=`, `%%` (none) — no Mermaid CLI on this machine, so rendering was checked by grammar, not by a renderer; `git diff --stat` touches only `docs/`.
- Flaky: n/a. Deviations: blueprint §21 was not available in the repo, the entry was executed from the roadmap description (four docs current, four diagrams, test per failure row, ADRs for C12–C19); README untouched (C21). Next: C21.

## C19 · test: stress suite with induced runner loss; benchmarks — done

- Landed: harness `HardKill(i)` (SIGKILL-like: the agent's transport drops every request, its attempts are cancelled, nothing is reported, leases expire only through the fake clock; `Restart`/cleanup join the goroutines). `internal/harness/stress_test.go`: `TestStressFanOutFanInWithRunnerLoss` — `src → 48 workers → sink` (50 jobs) on 3 agents × capacity 2, agents 0 and 1 hard-killed as soon as each holds a running attempt, clock stepped 1 s at a time (waiting for the survivor's `last_seen_at` each step) until no dead runner holds a lease; asserts run/jobs succeeded within 3 attempts, no `(job, attempt)` executed twice anywhere, every final attempt executed once on the recorded runner, one `job.claimed` per executed attempt, every `job.requeued` is `lost_runner` on a dead runner and never for a survivor execution, no `job.failed`/`job.skipped`, one `run.finished`, dead runners `offline`. `scripts/bench/` (stdlib `bench.go` driver + `sched-overhead.sh`, `throughput.sh`, `log-ingest.sh`, `runner-loss.sh`, `compose.override.yml` raising `QUARRY_LOG_CAP`); `docs/benchmarks.md` with methodology, hardware and this machine's numbers: ~660–750 claims/s at 1/3/10 fake runners (p50 = the 1 s poll), 218 jobs/min median on compose (~1.4 s Docker per job), 15.3 MB/s / 272 chunks/s at ~98 % of one server core, recovery = TTL + ~2 s (12.6 s @ 10 s, 31.6 s @ 30 s).
- Verified: stress test 10× + harness suite 3× under `-race` clean, `go test -race ./...`, `go vet ./...`, gofmt, `make lint`; all four benchmark scripts run end to end on Windows/Docker Desktop, no leftover `quarry.job` containers or volumes.
- Flaky: nothing. Races found: none in product code — the only failure the stress test surfaced (7/10 timeouts) was a fixture bug: a dead runner's in-flight claim/heartbeat lands after the first clock step, so a fixed `TTL + 1 s` walk missed its lease by the strict `<`; the helper now steps until no dead runner holds an attempt. Observation, not a bug: `Claim` reads `Now()` for `UpsertRunner` and again for `ClaimJob` inside one Tx (visible only with a stepped fake clock). Deviations: README table left for C21 as instructed; compose untouched (log cap via override file under `scripts/bench/`). Next: C20.

## C18 · observability: prometheus metrics, structured log fields, prometheus profile — done

- Landed: `internal/metrics` (new; `prometheus/client_golang v1.24.1`, approved stack): `Server` = `quarry_jobs_total{state}` (transitions by state entered), `quarry_queue_depth{labels}` + `quarry_runners{state}` (custom collector reading `scheduler.Snapshot` at scrape: `ListQueuedJobs` grouped by `LabelKey` = sorted `k=v,k=v`, `ListRunners` by state), `quarry_job_duration_seconds`, `quarry_claim_latency_seconds` (`started_at − queued_at`), `quarry_lease_expirations_total`, `quarry_log_bytes_total` (bytes actually stored: after cap and dedup); `Runner` = `quarry_runner_active_jobs`, `quarry_runner_executor_errors_total`. Each set on its own registry, `Handler()` via promhttp. Scheduler: `Config.Metrics` (nil → private set), a `transitions` accumulator filled inside Claim/Complete/CancelRun/advance/Tick/AppendLogs and applied only after commit so a rolled-back Tx never counts. API: `GET /metrics` unauthenticated next to `/healthz`, `Config.Metrics`, queued roots counted at submit. Agent: `Config.Metrics`, gauge around each attempt, executor-error counter on the `default` (infra-from-executor/upload) branch. `slog` everywhere: `api.Config.Logger`, `agent.Config.Logger`, `scheduler.RunMonitor`, harness → `*slog.Logger`; binaries emit JSON with `component=server|runner`; `request_id` on failed API requests, `runner_id` on every runner line after registration, `run_id/job_id/attempt` on attempt lines. `cmd/runner`: `QUARRY_METRICS_LISTEN` (unset = no listener; `serveMetrics` owned by `run()`), docker `Reap` bridged via `slog.NewLogLogger` (docker package untouched). Compose: `prometheus` profile (`prom/prometheus:v3.5.0`, `deploy/prometheus.yml`, 5 s scrape of server + runner-1..3, host `:9090`), runners set `QUARRY_METRICS_LISTEN=:9100`. `docs/architecture.md` gained "Observability".
- Verified: `metrics` (4 tests: every series renders, snapshot error omits gauges, runner set, `LabelKey`); scheduler `TestMetricsTraceTransitions/CountLeaseExpirations/CountStoredLogBytes`; api `TestMetricsQueueDepthChangesDuringRun` (submit → depth 2, claim → 1 + running/claim-latency/runners online, complete → succeeded/duration + depth back to 2; the entry's verify line over HTTP) and `TestMetricsNeedsNoToken`; agent `TestAgentMetricsAndLogFields` (gauge 1→0 around a hanging job, one executor error, JSON line carries `runner_id/run_id/job_id/attempt`); `cmd/runner` `TestServeMetrics`; `cmd/server` startup line asserted as JSON fields. `go test -race ./...` clean, 3× on agent/harness/api/scheduler; `go vet` (incl. `-tags docker`), gofmt clean; `docker compose --profile prometheus config` valid. Real binaries: fake runner + CLI run — `/metrics` showed `queued 2 / running 2 / succeeded 2`, claim latency count 2, runner `active_jobs 0`, both logs JSON.
- Flaky: nothing. Deviations: `/metrics` is unauthenticated (deliberate; scrape config needs no token); runner listener is opt-in rather than default-on so two runners on one host don't collide on a port; no per-request access log (only failed requests carry `request_id`), the events table stays the trace; `Reap` lines are plain `msg` text without fields; §18's `quarry_jobs_total{state}` is interpreted as "transitions by state entered" (`queued` includes requeues); the prometheus profile was validated with `compose config` only, not brought up (no `up` this session). Next: C19.

## C17 · artifact/remote: replicated-object-store backend and compose profile — done

- Landed: `internal/artifact/remote` (`New(ctx, Config)` ensures the bucket, 409 = ok; `Put` spools to a temp file then streams it with `Content-Length`, retrying transport/5xx — `InsufficientReplicas` included — from the spool with capped backoff, 4xx never, ctx-aware; `Get` per-attempt header timeout, body at caller's pace; `Delete` = HEAD then DELETE because the coordinator's DELETE is idempotent; `List` follows `truncated`/`next_start_after`; every non-2xx decoded into `*Error{Status,Code,Message}`; optional bearer token). `cmd/server`: `QUARRY_ARTIFACT_BACKEND=local|remote`, `QUARRY_ARTIFACT_REMOTE_URL/BUCKET/TOKEN`, `openArtifacts` fails startup if the coordinator is unreachable. Compose `remote` profile: `storage-coordinator` + 3 nodes (RF=3, W=2, image `QUARRY_STORAGE_IMAGE`, coordinator on host `:8090`), server env driven by `QUARRY_ARTIFACT_BACKEND`. `docs/design-decisions.md` §7; CLAUDE.md package map `cairn/` → `remote/`.
- Verified: untagged `remote` suite over an httptest fake coordinator (13 tests: local's cases ported, retries, store-down, 4xx not retried, cancel in backoff, pagination, token, escaping); docker-tagged `TestCluster{RoundTrip,PutFailureLeavesNothing,Replicated}` against the real cluster via `QUARRY_TEST_REMOTE_URL`; `go test -race ./...`, `make lint`, `go vet -tags docker` clean. End to end: full compose stack with `QUARRY_ARTIFACT_BACKEND=remote`, `examples/go-app` run succeeded with `storage-node-2` stopped → artifact on node1+node3 (`/cluster/locate`), CLI download sha256 verified, node back → repaired to 3 replicas in 2 s.
- Flaky: nothing. Deviations: no GHCR image exists yet (the storage project has no publish workflow), so the compose default `ghcr.io/manavmann/distributed-object-storage:latest` is a placeholder — verified with a locally built `quarry-storage:dev` via `QUARRY_STORAGE_IMAGE`; server cannot `depends_on` a profiled service, so with the remote backend it relies on `New`'s retries + `restart: unless-stopped`; the harness store-failure test was not re-parametrised for remote — the equivalent lives in `remote.TestPutStoreDown`. Next: C18.

## C16 · executor/docker: orphan container and volume reaping on startup — done

- Landed: `internal/executor/docker/reap.go` — `(*Executor).Reap(ctx,
  logger)`: `ContainerList(All)` filtered by `quarry.runner=<name>`,
  `ContainerRemove(Force)` each, then `VolumeList` by the same label and
  `VolumeRemove(force)` each — containers before volumes so nothing is
  still mounted; one log line per item as `quarry-<job>-<attempt> (run
  <run>)` from the C07 `quarry.job/run/attempt` labels (containers also
  show their state) plus a `N containers, M volumes found` line. A list
  failure is returned; a remove failure is logged and the rest continues.
  Only this runner's name is matched, so runners sharing one daemon
  (compose) never touch each other's attempts. With `KeepFailed` the
  reaper logs and does nothing, so `QUARRY_KEEP_FAILED=1` still keeps a
  failed attempt's remains across a runner restart. `cmd/runner`: after
  the signal ctx and before `agent.Run`, a docker executor is `Reap`ed;
  an error is fatal (the daemon is unreachable, no job could run anyway).
  `docker.go` only had its label comment updated; `Run`/attach/start/
  cleanup untouched, no new dependency.
- Verified: docker-tagged `TestDockerReapOrphans` (running `sleep 300`
  container + named volume under this runner's label, same pair under a
  foreign runner name → `Reap` → `assertClean` passes, both log lines
  present, foreign pair still 1 container + 1 volume) and
  `TestDockerReapKeepFailed` (labelled volume survives `Reap` with
  `KeepFailed`); full `make test-docker` suite (~25 s) and `go test -race
  ./...`, `go vet`, gofmt, `make lint` clean.
- Flaky: nothing.
- Next: C17.
known gap: the reaper is skipped entirely under `KeepFailed` (the entry
says reap unconditionally) — kept containers from earlier runs
accumulate until removed by hand; reaping runs once at startup only, an
attempt orphaned while the runner stays up (e.g. cleanup's 30 s remove
timeout expiring) waits for the next restart; no Compose-level verify of
a SIGKILLed runner this session (C12's manual clean-up scenario is
covered by the tagged test, not end to end).

## C15 · server: restart recovery and reconnect — done

- Landed: graceful HTTP drain, SQLite startup state counts, one-TTL lease-expiry grace, connection retries capped at 10 s through agent/shipper/CLI (including final delivery and source/artifact transfers), watch/log cursor resume; fencing unchanged.
- Verified: targeted suites, `go test -race ./...`, `go vet ./...`, gofmt/diff checks, fake-clock grace boundaries and same-DB restart harness; Compose restart plus 35 s downtime (>30 s TTL) succeeded on attempt 1 with one claim/no requeue, original runner/container, watch/logs exit 0 and no duplicated output.
- Flaky: none in tests; Compose shared-image build collision avoided by building server and runner-1 once. Next: C16 (not started).

## C14 · runs: cancellation and job timeouts — done

- Landed: cancel API/CLI, heartbeat directives, per-job timeouts and server backstop; context-first verdicts, idempotent events, no retry after cancellation (including lease expiry), and terminal run finalization.
- Verified: targeted tests, `go test -race ./...`, `go vet ./...`, gofmt, Docker `TestDockerCancelKill`/`TestDockerTimeoutKill`, CLI cancel, one-heartbeat propagation, and `scripts/demo.sh cancel` against an executing container (run cancelled, watch exit 1, no running job container).
- Flaky: none observed. Next: C15 (not started). Cancel-pending lease expiry remains terminal `failed(lost_runner)` without retry.

## C13 · protocol: zombie runner abort, superseded-attempt handling, protocol doc — done

- Landed: no code change — the abort reaction was already complete end to
  end (C05 heartbeat `abort` on fence failure; C06 `apply` cancels with
  `errAbort`, nothing reported, slot released by `start`'s defer; C08 logs
  409 → `OnStale` → abort; C10 upload 409 → abort; complete 409 →
  `errFenced`, not retried). This entry pins it: harness
  `TestZombieRunnerAbortsSupersededAttempt` (two agents, capacity 1,
  per-agent artifact content, fake clock: A muted and hung past TTL, B
  finishes attempt 2 with its artifact, A unmuted → heartbeat `abort` →
  A's executor killed with `context.Canceled`, build still attempt 2 on B,
  exactly one artifact object/row under attempt 2 with B's bytes, then
  test+lint observed running concurrently — one on each runner — so A's
  slot was freed; one `job.requeued`, no `job.failed`, A `online`).
  Verified the test fails (10 s timeout) when `apply("abort")` is a
  no-op. `docs/protocol.md` gained "Runner abort path" (the four abort
  signals, the one reaction, the two narrowing rules, cancel vs abort)
  and "Sequence diagrams" (happy path; lost-runner expiry + reassignment;
  zombie resume → abort, and the finished-container → 409 variant).
  Full suite + harness 4× under `-race` clean; `make lint` clean.
- Flaky: nothing.
- Next: C14.
known gap: the harness exercises the heartbeat-abort path only; the
"finished container → 409 upload → abort" variant is covered by the agent
unit test `TestAgentArtifactUpload409Aborts` against a stub server, not
end to end; `make test-docker` not run (daemon down), so "abort cleans
containers" rests on C07's kill/cleanup tests; `git checkout` on this
Windows tree writes CRLF (`core.autocrlf=true`) and `gofmt -l` then flags
the file — normalize to LF before `make lint`.

## C12 · scheduler: lease monitor, infra-failure retries, runner offline detection — done

- Landed: `scheduler.Config` gains `MaxAttempts` (default 3),
  `MonitorInterval` (5 s), `RunnerOfflineAfter` (30 s); `retryable(kind)`
  is the single retry-eligibility decision (`infra|lost_runner`) and
  `Complete` uses it — claim logic untouched. `scheduler/monitor.go`:
  `Tick` = one Tx (expired leases → `RequeueJob` + `job.requeued
  {failure_kind: lost_runner, runner_id}` below `max_attempts`, else
  `FinishJob(failed, lost_runner)` + `job.failed` + `advance`; then
  `MarkRunnersOffline(now-30s)`), returns a `TickReport`; `RunMonitor(ctx,
  logger)` paces ticks with a ticker but reads time only from the store
  clock. Store: `ListExpiredLeases`, `MarkRunnersOffline` (no migration).
  `api.submitRun` stamps `sched.MaxAttempts()` instead of 1;
  `Server.RunMonitor(ctx)`; `cmd/server`: `QUARRY_MAX_ATTEMPTS`, monitor
  goroutine owned by `run()` and stopped before the store closes. Harness:
  per-agent transport with `MuteHeartbeats(i, bool)`, `Opts.MaxAttempts/
  MonitorInterval/RunnerOfflineAfter` (monitor at 10 ms, fake clock),
  `Client.Runners()`. Tests: scheduler `retryable` table + `lost_runner`
  rejected from runners, requeue + idempotent tick + stale complete
  `ErrFenced` + heartbeat `abort` + reclaim as attempt 2, exhaustion
  cascade with a sibling kept alive by heartbeats, offline once + back on
  contact; harness `TestLostRunnerJobIsReassigned` (mute → clock +31 s →
  other runner succeeds, lost runner `offline`, HTTP stale complete 409),
  `TestLostRunnerExhaustsAttempts`, `TestSilentRunnerGoesOffline`,
  `TestExitCodeFailureIsNeverRetried`. Re-verified C10's
  `TestArtifactStoreFailureIsInfra` and C06's `TestInfraFailureIsReported`/
  `TestKillAndRestartAgent`: they asserted terminal `failed(infra)` on
  attempt 1 because `max_attempts` was 1 — now they assert the requeue
  (kill → `queued`, same runner finishes attempt 2) and terminal failure
  at attempt 3 (2 for the artifact test) with `job.requeued` events.
  `docs/failure-model.md` draft, protocol/architecture updated. Compose
  verified: `docker compose kill runner-2` 8 s into a 45 s job → requeued
  as `lost_runner` at +39 s, `runner-3` ran attempt 2, run succeeded,
  `runner-2` shows `offline`. 4× under `-race` clean.
- Flaky: nothing.
- Next: C13.
known gap: the SIGKILLed runner's job container + volume
(`quarry-<job>-1`) are orphaned on the host until C16's reaper (cleaned by
hand this session); no `runner.offline`/`runner.online` events (events
are per run); `MonitorInterval`/`RunnerOfflineAfter` have no env vars;
`quarry status` shows `lost_runner` only via `failed (lost_runner)`, the
requeue is visible in `events` only; reassignment latency is TTL + tick +
poll (~36 s with defaults), not tunable below the 5 s tick.

## C11 · deploy: compose cluster, images, example pipeline, demo script v1 — done

- Landed: `deploy/server.Dockerfile` + `deploy/runner.Dockerfile`
  (multi-stage, `golang:1.27-alpine` → `alpine:3.20`, `CGO_ENABLED=0`,
  version via `ARG VERSION`; server runs as `quarry` with
  `/var/lib/quarry` as the DB + artifact volume, runner runs as root for
  the socket), `deploy/docker-compose.yml` (server on named volume
  `quarry-data`, `/healthz` healthcheck; runner-1..3 with
  `os=linux,pool=build|test|test`, socket mount, healthcheck = "name
  appears in `GET /api/runners`", `depends_on: service_healthy`; token
  `QUARRY_API_TOKEN` default `dev-token`), root `.dockerignore`,
  `examples/go-app` (stdlib word counter with tests; `.quarry.yml`:
  build → [test, lint] → package, `artifacts: [dist]`, `labels: {os:
  linux}`), `scripts/demo.sh` (build CLI, `up --build --wait`, pre-pull
  job image, `run`, `watch`, list package artifacts via curl+sed on the
  run JSON, `down [-v]`), README quickstart, `docs/architecture.md`
  first draft, `docs/design-decisions.md` (DooD, volume-per-attempt,
  SQLite single-writer, attach-before-start, `(state, attempt)` fence,
  run.sh per job). CI gained a `compose` job: `up --wait`, `timeout 120
  quarry run --wait` on a cold cache, 4/4 healthy, `demo.sh` end to end,
  no leftover `quarry.job` containers/`quarry-*` volumes, `down -v`.
  Verified locally on Windows Docker Desktop: demo 45 s wall including
  build+up; cold-cache `run --wait` 24 s (runner pulled the image inside
  `build`); run survives `compose restart server`. Nothing under
  `internal/` changed.
- Flaky: nothing. macOS not verified this session (no machine); CI covers
  Linux only — GitHub macOS runners have no Docker.
- Next: C12.
known gap: no `quarry` CLI image (demo needs Go on the host); runner
healthcheck only proves registration, not that it can reach the daemon;
compose has no `QUARRY_LOG_CAP`/`QUARRY_KEEP_FAILED` knobs exposed; job id
for `artifacts` still has to be scraped from the run JSON (status table
shows names only).

## C10 · artifact: ArtifactStore, local backend, source bundles, upload/download — done

- Landed: `internal/artifact` (`Store` = `Put(ctx,key,r,size)`/`Get`/
  `Delete`/`List(prefix)`, `ErrNotFound`, `ValidatePath`/`ValidateKey`
  rejecting absolute, empty/`.`/`..` segments, backslash and control
  chars; `SourceKey`/`JobPrefix`/`JobKey` build the only two key shapes)
  and `artifact/local` (temp file in the destination dir + rename, so a
  failed reader or crash leaves nothing; `size < 0` = read to EOF for
  multipart parts). Migration `0004_artifacts` (blueprint §9, PK
  `(job_id, attempt, path)`); `store/artifacts.go` (`UpsertArtifact`,
  `GetArtifact`, `ListArtifacts`). `api/artifacts.go`: `POST
  /api/runner/jobs/{id}/attempts/{attempt}/artifacts/{path...}` —
  `Content-Length` required (411, 1 GiB cap 413), fence checked before
  the write, body tee'd into sha256 and streamed to `Put`, then fence +
  row upsert in one Tx (fence loss → object deleted, 409); `GET
  /api/jobs/{id}/artifacts[?attempt=]` and `.../artifacts/{path...}`
  (row's Content-Length/Type, sha256 as ETag); `GET /api/runs/{id}/source`.
  `handleSubmitRun` mints the run id first and streams the multipart
  `source` part into `sources/<run>.tar`, discarding it on any failure;
  `api.Config.Artifacts` is required (panic otherwise). Executor:
  `JobSpec.ArtifactDir` (agent-owned temp dir the executor fills;
  `FakeExecutor` `Outcome.Artifacts`); `docker/artifacts.go` copies each
  declared path after a zero exit, strips the leading tar component
  (`dist/` → not `dist/dist/`), writes regular files only, skips a
  missing path with a `[quarry]` line, fails infra otherwise — attach/
  start/cleanup untouched. Agent: uploads every file with `Content-Length`
  (retry transport/5xx with the completion backoff) *before* the final log
  flush and `complete`; compares the reply's sha256/size with its own
  streamed hash — this detects a mismatch after the upload completes,
  retries the whole upload, and fails the attempt as infra if it
  persists; other upload failure → `failed(infra)`; 409 → abort (nothing
  reported).
  `cmd/server`: `QUARRY_ARTIFACT_DIR` (default `quarry-artifacts`). CLI
  `artifacts <job> [--download dir] [--attempt N]` (PATH/SIZE/SHA256; download
  verifies sha256, temp+rename, refuses paths that leave `dir`). Harness:
  local store behind `FailArtifactPuts`, `Client.Artifacts/Download`.
  `docs/protocol.md` gained the four endpoints. Tests: local atomicity
  (failed reader leaves nothing, previous object survives, size
  mismatch, ctx), traversal rejected at package/backend/API/CLI; store
  upsert/list; api upload/list/download/409/404/411/400, source
  round-trip + orphan cleanup; docker untagged strip/single-file/
  traversal/symlink + tagged directory artifact; agent uploads-before-
  complete, retry→infra, 409→abort; harness artifacts listed with sha256
  and downloadable, store failure → `failed(infra)`; CLI e2e download via
  the harness. 4× under `-race` clean.
- Flaky: nothing. `make test-docker` not run this session (daemon down);
  the tagged test compiles under `go vet -tags docker`.
- Next: C11.
known gap: `max_attempts` is still 1, so "store failure → infra retry"
is terminal `failed(infra)` rather than a requeue; the runner cannot
send a pre-computed sha256 with `Content-Length` (trailers need chunked
encoding), so the server hashes the stream and the runner can only
detect a mismatch after the upload completes (the object and row are
already stored by then); `artifacts:` entries are paths, not globs (`bin/**` is
"not found, skipped"); an artifact file and a directory of the same name
in one attempt is a 500 on the local backend; no per-run artifact
retention/GC (C16).

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
