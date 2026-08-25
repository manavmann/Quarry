# Runner protocol

How a runner talks to the control plane, and the exact rules the
scheduler applies. Blueprint §8/§13/§14 are the source; this document is
what `internal/scheduler` implements and what its tests check.

## Job states

```
pending ──(all needs succeeded)──▶ queued ─(claim)──▶ running ──▶ succeeded
   │                                 ▲                   │           failed
   └──(a need failed/cancelled/──▶ skipped               └──(infra, attempt < max_attempts;
        skipped)                                              or lease expired)──▶ queued
```

`queued` means the job is claimable. `attempt`
starts at 0 and is incremented by every successful claim, so attempt `n`
is the n-th runner to hold the job. A job is terminal in `succeeded`,
`failed`, `cancelled` or `skipped`.

## Endpoints

All `/api/runner/*` endpoints take the same bearer token as the user API.
Bodies are JSON. Timestamps are Unix milliseconds.

### `POST /api/runner/register`

```json
{"name": "box-a", "labels": {"os": "linux"}, "capacity": 2}
```

Resolves a runner name to its `runner_id`, minting one on first sight.
Names are unique (`runners.name`, migration 0002); re-registering the
same name is idempotent — the reply carries the same `runner_id` and the
row's labels and capacity are refreshed (version comes from claim). The lookup and upsert
share one transaction. A runner calls this once at startup, before its
first claim, and retries transport/5xx failures with backoff (a control
plane that is still starting just delays it); a 4xx is fatal.

Responses: `200 {"runner_id": "…"}`, `400` when `name` is empty or
`capacity` is negative.

### `POST /api/runner/claim`

```json
{"runner_id": "r1", "name": "r1", "labels": {"os": "linux"}, "capacity": 2, "version": "dev"}
```

Refreshes the runner row (labels, capacity, version, `last_seen_at`),
then, in one `BEGIN IMMEDIATE` transaction:

1. select `queued` jobs ordered by `queued_at, rowid`;
2. pick the first whose spec labels ⊆ runner labels (matched in Go);
3. `UPDATE jobs SET state='running', runner_id=?, attempt=attempt+1,
   lease_expires_at=now+ttl, started_at=now WHERE id=? AND state='queued'`;
4. if the run was `pending`, mark it `running`; append `job.claimed`
   (and `run.started`).

Responses: `200` with the job and its new attempt, or `204` when nothing
matched. A runner polls this every ~1 s while below capacity.

```json
{"job": {"id": "…", "run_id": "…", "name": "test", "attempt": 1,
         "lease_expires_at": 1700000030000, "lease_ttl_ms": 30000, "spec": {…}}}
```

### `POST /api/runner/heartbeat`

```json
{"runner_id": "r1", "jobs": [{"job_id": "…", "attempt": 1}]}
```

Refreshes the runner's `last_seen_at` and, for each listed job, extends
the lease with

```sql
UPDATE jobs SET lease_expires_at = now+ttl
 WHERE id=? AND state='running' AND attempt=? AND runner_id=?
```

Reply carries one directive per job:

| directive  | meaning                                                          |
|------------|------------------------------------------------------------------|
| `continue` | lease extended, keep going                                       |
| `cancel`   | reserved: a user cancelled the job (lands with `quarry cancel`)  |
| `abort`    | the fence failed — the job is unknown, not running, or the       |
|            | attempt is not yours any more. Kill the container, report nothing |

An unknown runner is upserted as a side effect (name = id, no labels).

### `POST /api/runner/jobs/{id}/complete`

```json
{"runner_id": "r1", "attempt": 1, "status": "failed",
 "failure_kind": "exit_code", "exit_code": 2, "error": "…"}
```

`status` is `succeeded` or `failed`; `failure_kind` is one of
`exit_code | timeout | infra | cancelled` and is required when failed.
Everything below happens in **one transaction**, in this order:

1. **Fence.** Load the job. If `state='running' AND attempt=?` does not
   hold:
   - job already terminal with the same attempt → `200`, no-op
     (duplicate delivery; the first result stands);
   - otherwise → `409` (stale attempt, requeued job, unknown attempt).
   The runner treats 409 as *abort*.
2. **Terminal transition.** `succeeded` → `succeeded`. `failed` with
   kind `infra` and `attempt < max_attempts` → back to `queued` at the
   queue tail (`runner_id=NULL`, `queued_at=now`, attempt unchanged;
   event `job.requeued`). Any other failure → `failed` with the kind and
   exit code. Events `job.succeeded` / `job.failed`.
3. **Advancement.** For every `pending` job of the run, until nothing
   changes: all needs `succeeded` → `queued` (`job.queued`); any need in
   `failed | cancelled | skipped` → `skipped` (`job.skipped`). Because the
   pending→queued update is guarded by `state='pending'`, a dependent is
   queued exactly once.
4. **Run finalization.** If no job is `pending | queued | running` the run
   is terminal: `failed` if any job failed, else `cancelled` if any job
   was cancelled, else `succeeded`. Event `run.finished`.

Responses: `200` with the job as stored, `404` unknown job, `409` fence
failure, `400` bad body.

## Fencing rule

Every runner-side write — heartbeat, log chunk, artifact, completion —
checks `(state='running', attempt)`. Not attempt alone: between a lease
expiry (running → queued) and the next claim, the old attempt number is
still the latest, and the state check is what rejects the lost runner's
late write. A runner that receives `409` or `abort` kills its container
and forgets the attempt.

## Retries

- `max_attempts` bounds the number of claims. Only `infra` and
  `lost_runner` failures retry; `exit_code`, `timeout` and `cancelled` do
  not — a failing test is not flakiness the platform should hide.
- Lease TTL is `QUARRY_LEASE_TTL` (default `30s`); runners heartbeat every
  5 s, so ~6 missed heartbeats precede reassignment. Expiry itself is the
  lease monitor's job (C07): running jobs past `lease_expires_at` go back
  to `queued` (or `failed(lost_runner)` at the attempt cap) and the next
  claim increments `attempt`.

## Execution semantics

- Job execution is at-least-once across attempts; each attempt's results
  are accepted at most once. A partitioned runner may keep running a
  superseded attempt until its next heartbeat; its logs, artifacts and
  completion are rejected with 409 and it aborts the container.
- State transitions are linearizable: a single writer, one transaction
  per transition. DAG advancement happens inside the completion
  transaction, so a dependent is queued exactly once.
- API reads are read-your-writes after any 2xx.
- Logs are totally ordered per attempt by `seq`, visible within roughly
  flush interval + poll interval (~0.75 s).
- Cancellation is best-effort and bounded by one heartbeat interval plus
  kill time.
- Run result is a pure function of terminal job states.
- Not provided: exactly-once side effects (a deploy step can run twice if
  a runner is partitioned — jobs must be idempotent, same as every real
  CI), cross-job log ordering, control-plane HA.
