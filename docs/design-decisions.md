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
