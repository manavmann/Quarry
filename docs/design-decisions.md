# Design decisions

The choices an interviewer would ask "why" about, each with the
alternative that lost. Add an entry here *before* introducing a new
dependency or changing one of these. Newest at the bottom.

## 1. Docker-out-of-Docker, not Docker-in-Docker

**Decision.** The runner talks to the host's Docker daemon through the
mounted `/var/run/docker.sock` (`client.FromEnv`), so job containers are
*siblings* of the runner on the host daemon. The runner image runs as
root only to open that socket; job containers never receive the socket,
`Privileged`, or extra capabilities (`hostConfig` in
`internal/executor/docker`).

**Alternative.** Docker-in-Docker: a privileged runner container with its
own nested daemon.

**Why it lost.** DinD needs `--privileged`, a second image cache per
runner (every job would re-pull `golang:*`), nested overlayfs, and its
own cleanup story. DooD shares the host's image cache, needs no
privileges beyond the socket, and lets the same executor code run
against a bare-metal daemon or Docker Desktop unchanged. The cost — a
runner *is* trusted with the daemon — is the same trust every self-hosted
CI runner already carries, and is why the runner, not the job, is root.

## 2. A named volume per attempt, not host-path mounts

**Decision.** Each attempt gets its own volume `quarry-<job>-<attempt>`
mounted at `/workspace`; the source bundle is streamed into it with
`CopyToContainer`, and the volume is removed with the container.

**Alternative.** Extract the bundle to a directory on the runner and bind
mount it (`-v /tmp/ws:/workspace`).

**Why it lost.** Under DooD a bind-mount path is interpreted by the *host*
daemon, not by the runner container, so the runner's `/tmp/ws` does not
exist where the daemon looks. Named volumes are daemon-side objects and
work identically for a containerised or a bare runner. The per-attempt
name also gives a retrying attempt a clean workspace by construction,
makes orphaned workspaces enumerable by label for a reaper, and keeps two
attempts of the same job (a lost runner still writing) from sharing a
directory.

## 3. SQLite in WAL mode with one writer, not a client-server database

**Decision.** `internal/store` opens `modernc.org/sqlite` (pure Go) with
WAL, `busy_timeout`, `foreign_keys`, `_txlock=immediate` and a
single-connection pool. Every state transition is one `BEGIN IMMEDIATE`
transaction; DAG advancement happens inside the completion transaction.

**Alternative.** Postgres (or MySQL) behind the control plane.

**Why it lost.** The control plane is a single process by design (no HA
in scope), and the scheduler's correctness argument is "one writer, one
transaction per transition" — which SQLite gives for free and which a
connection pool against a network database makes *harder* to reason
about (isolation level, retry on serialization failure). Zero external
services means `docker compose up` is the whole install and the harness
can run hundreds of scheduler tests on a temp file. The store is behind
its own package, so a second backend is a migration problem, not an
architecture change.

## 4. Attach before start, not `docker logs` after the fact

**Decision.** The executor attaches to the created container's
stdout/stderr (`Tty=false`, demuxed with `stdcopy`) and registers
`ContainerWait(WaitConditionNextExit)` *before* `ContainerStart`, then
streams the output into the log shipper as it happens.

**Alternative.** Start the container, then read `ContainerLogs(follow)`;
or run the job, then dump the log once it exits.

**Why it lost.** Attaching after start races the container's first
bytes: a job that prints and exits in a few milliseconds loses its
output, and a `wait` registered after start can miss the exit entirely
(found in test: `NotRunning` fires immediately on a *created*
container, which is why the wait is registered on the created container
and on a detached context). Dumping at the end would make `quarry logs
-f` useless for a 20-minute build and would lose everything on a runner
crash. Streaming through the shipper is also what makes the log-chunk
fence (decision 5) apply to every byte.

## 5. `(state='running', attempt)` fencing, not a lock or a lease id

**Decision.** Every runner-side write — heartbeat, log chunk, artifact,
completion — is guarded by `WHERE state='running' AND attempt=?` (and
`runner_id` for heartbeats) in the same transaction as the write. A
failed guard is `409`/`abort`; the runner kills its container and
forgets the attempt. The attempt counter is bumped on every successful
claim, never reset.

**Alternative.** A per-job lock held by the runner, or a random lease
token issued at claim and checked on write.

**Why it lost.** A lock is exactly what a crashed runner cannot release;
the lease monitor must be able to take the job away unilaterally, and
then the *old* holder's late writes must be rejected without any
cooperation from it. Attempt alone is not enough either: between lease
expiry (running → queued) and the next claim the old attempt number is
still the latest, and only the state check rejects the lost runner's
write in that window. Checking two columns the job row already has costs
nothing, needs no extra table, and makes the rule the same for all four
write paths — which is why it is an invariant in `CLAUDE.md`.

## 6. One container per job running a generated `run.sh`, not `sh -c` strings

**Decision.** A job's `steps` are written into `/quarry/run.sh` (`set
-e`, each step echoed as `+ <step>` before it runs, single quotes
escaped) which is copied into the container as its own tar and run as
`/bin/sh /quarry/run.sh`. All steps of a job share one container and one
`/workspace`.

**Alternative.** One container per step (`docker run image sh -c
"<step>"`), or joining the steps with `&&` into a single `Cmd`.

**Why it lost.** A container per step throws away the workspace (or
forces a shared volume plus N container starts per job) and turns each
job into N schedulable units. Joining with `&&` mangles quoting, hides
which step failed, and breaks on steps that legitimately contain `&&`
or `||` (the example pipeline's lint step does). A script gives shell
semantics the user already knows, a visible `+ step` marker in the log,
fail-fast via `set -e`, and an exit code that is the failing step's. The
generated file is a pure function of the spec (`runScript`, unit-tested
without Docker).

## 7. Remote artifacts over the coordinator's HTTP API, with a spool, not a client library

**Decision.** `internal/artifact/remote` is a hand-written `net/http`
client for the replicated object store's coordinator (one bucket, one
object per key, `PUT/GET/HEAD/DELETE /v1/{bucket}/{key}` and the paged
listing), selected with `QUARRY_ARTIFACT_BACKEND=remote`. `Put` spools the
reader to a temp file exactly as `local` does and then streams the file
with an explicit `Content-Length`; transport errors and 5xx replies (the
coordinator's `InsufficientReplicas`, `NoHealthyReplica`, `TooManyUploads`
are all 503) are retried from the spool with capped exponential backoff,
4xx never. `Delete` does a `HEAD` first because the coordinator's `DELETE`
is idempotent and `artifact.Store` promises `ErrNotFound`. No auth is sent
unless `QUARRY_ARTIFACT_REMOTE_TOKEN` is set (the cluster's shared secret
only guards its node-to-coordinator heartbeat, not `/v1`). The compose
`remote` profile runs the cluster at RF=3, W=2 next to the server.

**Alternative.** Import the storage project's own Go client, or an S3 SDK
with the coordinator behind an S3-compatible shim; or stream the request
body straight through to the coordinator with no spool.

**Why it lost.** A client import would be the first dependency that is
not on the approved list and would tie Quarry's build to the other
project's module path and rename-in-progress; the API surface Quarry
needs is five routes and one error shape. Straight-through streaming
cannot retry: the API handler hands `Put` a one-shot request body, so an
`InsufficientReplicas` after the bytes are consumed would have to be
turned into a runner-side re-upload, and a long or short reader would
already be on the cluster before the mismatch was known. The spool costs
one extra disk write per artifact — the same cost the local backend pays
for its temp-and-rename — and buys retries, exact `Content-Length` for
readers of unknown size (multipart source bundles), and `local`'s
"nothing stored on failure" semantics unchanged.

## 8. Retry eligibility is a function of `failure_kind` alone

**Decision.** `scheduler.retryable(kind)` is the single place that says
whether a failed attempt may be claimed again: `infra` and `lost_runner`
yes, `exit_code`, `timeout` and `cancelled` no — and only while
`attempt < max_attempts` and no cancel is pending. `max_attempts` counts
claims (the attempt number is bumped on claim, kept on requeue), is
stamped on every job at submission from `QUARRY_MAX_ATTEMPTS`, and the
runner cannot report `lost_runner` at all (`400`).

**Alternative.** Let the runner decide (`retryable: true` in the
completion body), or pattern-match the error message ("pull failed",
"connection reset") server-side, or a per-job `retries:` field.

**Why it lost.** A runner that is confused enough to fail an attempt is
not the right party to judge whether the platform should hide that; and
a message heuristic is a list that never stops growing. Splitting the
kinds by *whose fault it was* gives one predicate the monitor, `Complete`
and the tests all share, and makes "a failing test is never retried" a
one-line property rather than a policy. A per-job override can be added
later as an input to the same predicate.

## 9. The lease monitor is a periodic single-transaction tick, not per-lease timers

**Decision.** `scheduler.Tick` runs every `MonitorInterval` (5 s), reads
`now` from the store's injected clock once, and in one `BEGIN IMMEDIATE`
transaction expires every lease strictly before `now`, applies the
timeout backstop, and marks silent runners offline. It selects nothing
the previous tick changed, so it is idempotent; a failed tick is retried
by the next one. `RunMonitor` is a goroutine owned by `cmd/server`'s
`run()` and the ticker only paces it.

**Alternative.** A `time.AfterFunc` per claim that fires at
`lease_expires_at`; or SQLite triggers; or checking expiry lazily inside
`Claim`.

**Why it lost.** Per-lease timers are in-memory state that a server
restart loses and a fake clock cannot drive; they also race the
heartbeat that extends the lease. A tick reads the one source of truth
(the row's `lease_expires_at`) and needs no recovery step after a
restart. Lazy expiry in `Claim` would never fire on an idle cluster, so
the last job of a run whose runner died would sit `running` for ever.
The cost is up to one interval of extra latency (5 s on a 30 s TTL),
which the benchmarks show as TTL + ~2 s in practice.

## 10. A fenced-out runner aborts silently; the server never hears about it

**Decision.** When any of the four write paths says `abort`/`409`, the
runner cancels the attempt's context with the abort cause, kills and
removes the container, discards the shipper's buffer, sends nothing
further — not even `complete` — and frees the slot. There is no
"attempt aborted" endpoint or event.

**Alternative.** `POST …/aborted` so the server can record that a zombie
existed, or letting the zombie's `complete` through as an informational
`failed(superseded)`.

**Why it lost.** From the store's point of view the attempt ended when
its lease expired and the monitor requeued it; the runner is catching up
with a decision already made, and nothing it could say would change any
row. Anything it *did* write would either be rejected by the same fence
(so the endpoint is pointless) or would have to bypass it (so the fence
would have a hole). The 409s the server returns are the complete record
of the zombie; the monitor's `job.requeued {runner_id}` event already
names the runner that was lost.

## 11. Cancel rides on the heartbeat, and every kill is a context cause

**Decision.** `POST /api/runs/{id}/cancel` only writes
`cancel_requested_at / cancel_reason` on running jobs; the runner learns
of it as the `cancel` directive on its next heartbeat. Runner-side,
cancel, abort, job timeout and runner shutdown all end the attempt the
same way — `context.CancelCause` on the attempt's context — and
`context.Cause` decides what, if anything, is reported: `failed(timeout)`,
`failed(cancelled)`, nothing, or `failed(infra)`.

**Alternative.** A server→runner push channel (long-poll or WebSocket)
so cancel is immediate; or a `Kill()` method on the executor.

**Why it lost.** A push channel is the one thing the pull-based design
was chosen to avoid: it needs connection state on the server, reconnect
logic on both sides, and a story for what a cancel means when the
channel is down. The heartbeat already exists, is already fenced, and
bounds cancel latency at one interval (5 s) plus kill time — the same
budget as lease renewal. A `Kill()` method would make `Executor` a
two-method interface and give the docker package a second code path for
the same operation; killing via the context keeps it at one and makes
the fake executor's behaviour identical.

The same directive carries the monitor's timeout backstop: a running
job past `started_at + timeout + 30 s` gets `cancel_reason=timeout`,
the runner kills and reports `failed(cancelled)` exactly as for a user
cancel, and `Complete` consults `cancel_reason` to store `failed(timeout)`
instead of `cancelled`. A third `timeout` directive would only change
the label the runner echoes back; who asked for the kill is knowledge
the server already holds in the row, and storing a backstop as
`cancelled` would make a timed-out run look like a user action and let a
timeout dodge the never-retried rule by accident.

## 12. One lease TTL of grace after a control-plane restart, no state recovery

**Decision.** `scheduler.New` sets `leaseGraceUntil = now + LeaseTTL`
from the injected clock; until then `Tick` skips lease expiry entirely
(including leases that expired during the outage) while the timeout
backstop and offline marking still run. Nothing else is recovered on
boot: the row is the truth, the runner heartbeats renew it, and after
grace unrenewed leases expire normally.

**Alternative.** Requeue every `running` job on boot ("the server was
down, assume the worst"); or persist the monitor's last tick time and
extend leases by the measured downtime.

**Why it lost.** Requeue-on-boot turns every server restart into a
duplicate execution of every in-flight job, which is exactly the
at-least-once cost the lease protocol is meant to minimise; a runner
that kept running through a 35 s outage still has a valid container and
will report a result that would then be fenced out for no reason.
Extending by measured downtime needs a persisted clock and is wrong the
moment the server's wall clock jumps. One TTL of grace is the longest a
live runner can need to renew, needs no state, and is proven by
`TestServerRestartAfterLeaseTTL`: the original attempts finish once,
with one claim and no requeue.

## 13. Reconnect for ever while alive, bounded only by shutdown

**Decision.** Agent, log shipper and CLI wrap every call in a reconnect
loop: a transport error waits with exponential backoff capped at 10 s
and retries until the context ends; HTTP replies are handled by the
caller. Only 5xx replies consume the completion retry budget (8 tries);
409 and other 4xx are final. After `SIGTERM` the runner gets
`LogFlushTimeout` (30 s) to deliver final logs and verdicts, then stops.
Source and artifact downloads restart into a temp file after a partial
read so a consumer never sees a truncated stream.

**Alternative.** A fixed retry budget for everything (give up after N
transport failures), or no retry at all with the lease monitor as the
only recovery path.

**Why it lost.** A finished attempt's verdict is the most valuable byte
the runner holds; dropping it after N failed connects converts a
30-second network blip into a wasted attempt plus a full lease TTL of
delay. While the runner is alive the lease is still being renewed (the
heartbeat retries the same way), so waiting costs nothing; the moment
the runner is shutting down the wait must end, and `LogFlushTimeout` is
that bound. Distinguishing transport errors from HTTP errors keeps the
budget meaningful: a server that answers 500 eight times is broken in a
way retrying will not fix.

## 14. The orphan reaper matches on the runner's own name only

**Decision.** On startup `docker.Executor.Reap` lists containers and
volumes labelled `quarry.runner=<this runner's name>` and force-removes
them, containers before volumes. Other runners' labels are never
touched, and under `QUARRY_KEEP_FAILED` the reaper logs and does nothing.

**Alternative.** Reap everything labelled `quarry.job`, or reap by age,
or ask the server which attempts are still running.

**Why it lost.** Runners on a compose host share one daemon; reaping by
`quarry.job` would let `runner-1` restarting kill `runner-2`'s live
attempt. Age is a guess. Asking the server needs the reaper to run after
registration and to trust a "still running" answer that a lease
expiry can invalidate a second later. The runner's own name is the one
label whose owner is unambiguous: anything under it belongs to a
previous incarnation of *this* process, which by construction is dead.
`KeepFailed` is a debugging switch, and a debugging switch that erased
the thing you were debugging on restart would be worse than none.

## 15. Fault injection lives in the harness transport and the fake clock

**Decision.** `internal/harness` gives every in-process agent its own
HTTP transport with switches — `MuteHeartbeats(i)` drops heartbeats
only, `HardKill(i)` drops every request, cancels the agent's attempts
and reports nothing, `Kill(i)` is a clean shutdown — and the server's
clock is a fake stepped by the test. The stress suite kills runners this
way and walks the clock 1 s at a time until no dead runner holds a
lease.

**Alternative.** Spawn real runner processes and `SIGKILL` them; use
real time with short TTLs.

**Why it lost.** Real processes make "the runner is dead but its last
claim is still in flight" untestable rather than merely hard — the C19
stress test found exactly that interleaving, and only because the
harness could step time and observe leases between steps. Short real
TTLs make every test a race against the scheduler on a loaded CI box,
which is how `time.Sleep` gets into a test suite. The compose cluster
and `scripts/bench/runner-loss.sh` cover the real-process case as a
benchmark, not as a correctness test.
