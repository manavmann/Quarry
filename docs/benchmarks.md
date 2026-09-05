# Benchmarks

Four numbers, each produced by a script under `scripts/bench/` that
anyone can rerun. Every script prints the environment it ran in, one
line per repetition and a `median:` line; the tables below are those
lines from the run recorded here. Nothing was tuned for them.

## Methodology

- Three repetitions of each measurement; the median is reported, with
  the p95 where a distribution is being measured. Repetition 1 of a
  container benchmark is a warm-up and is shown, not dropped.
- Timestamps are the server's (Unix ms, stamped in one transaction with
  the state change), so a number never depends on a client's clock —
  except runner-loss recovery, where the kill and the replacement are
  both observed from the driver process so the two clocks cancel out.
- Images are pulled before a run (`docker pull` on the host daemon; the
  runners share it), so no repetition downloads anything.
- The driver is `scripts/bench/bench.go` (standard library only); the
  `.sh` files around it bring the cluster up and pass the flags.

### Hardware and software for the numbers below

| | |
|---|---|
| Date / commit | 2026-09-20, `211dbb0` plus this commit's scripts |
| CPU | AMD Ryzen 5 3600X, 6 cores / 12 threads |
| Memory | 16 GiB (Docker Desktop VM: 12 vCPU, 7.7 GiB) |
| Disk | Toshiba DT01ACA100, 7200 rpm HDD (SQLite WAL + artifacts live here) |
| OS | Windows 11 Home 10.0.26200; benchmarks 2–4 run in Docker Desktop (WSL2, kernel 6.6.87) |
| Docker | Engine 28.4.0, Compose v2.39.2, overlayfs |
| Go | 1.27.0 windows/amd64 |
| Cluster | `deploy/docker-compose.yml`: 1 server, 3 runners at capacity 2, Docker executor, local artifact backend |
| Defaults in play | runner poll 1 s, heartbeat 5 s, lease TTL 30 s (10 s where stated), monitor tick 5 s, log cap raised to 256 MiB for benchmark 3 only |

## 1. Scheduling overhead

`scripts/bench/sched-overhead.sh` — the server and *N* runners with
`QUARRY_EXECUTOR=fake` on this machine (no Docker), 100 independent
jobs per submission. A fake job takes no time, so queued→running is
the scheduler's claim transaction plus one HTTP round trip plus the
runner's wait for its next poll. *Pickup* is the idle runners' wait
before the first claim; *claims/s* is the rate between the first and
last claim, where every claim follows the previous one at once.

| Runners | p50 | p95 | pickup | claims/s |
|---|---|---|---|---|
| 1 | 1084 ms | 1151 ms | 1008 ms | 669 |
| 3 | 588 ms | 640 ms | 496 ms | 660 |
| 10 | 592 ms | 643 ms | 519 ms | 750 |

Reading it: p50 is the 1 s poll interval, not the scheduler. Once a
runner is claiming, the control plane hands out 650–750 claims/s no
matter how many runners contend, each claim and its completion being
one SQLite write transaction on one connection (`_txlock=immediate`).
More runners neither help nor hurt: the single writer is the ceiling
and the guarded claim never retries. With `QUARRY_BENCH_POLL=100ms`
the same script gives p50 114–125 ms, p95 169–188 ms at every runner
count — the poll interval is the whole difference.

## 2. Throughput

`scripts/bench/throughput.sh` — compose cluster, 30 jobs of `true` on
`alpine:3.20` per submission, jobs/min from the first `queued_at` to the
last `finished_at`. This measures the cost of creating a volume and a
container, copying the source in, starting, waiting and removing, per
job, on Docker Desktop's WSL2 daemon — not the scheduler (see 1).

| Rep | Jobs | Wall | jobs/min | per-job running p50 |
|---|---|---|---|---|
| 1 (cold) | 30 | 22.1 s | 81.5 | 3663 ms |
| 2 | 30 | 7.4 s | 241.9 | 1343 ms |
| 3 | 30 | 8.2 s | 218.4 | 1439 ms |
| **median** | | | **218** | |

About 1.4 s of Docker per trivial job, six at a time.

## 3. Log ingestion

`scripts/bench/log-ingest.sh` — compose cluster with
`scripts/bench/compose.override.yml` raising `QUARRY_LOG_CAP` to
256 MiB; one job emitting 50 MB of 100-byte lines (`yes … | head -c
52428800`). MB/s and chunks/s are over the attempt's `started_at` →
`finished_at`, which includes container start and the final synchronous
flush (the runner completes only after every chunk is stored). Chunks
are the shipper's 64 KiB / 250 ms batches. Server CPU is the peak 1 s
`docker stats` sample of the server container during the run.

| Rep | Bytes stored | Chunks | Time | MB/s | chunks/s |
|---|---|---|---|---|---|
| 1 | 52 428 926 | 904 | 4.85 s | 10.8 | 186 |
| 2 | 52 428 926 | 931 | 3.43 s | 15.3 | 272 |
| 3 | 52 428 926 | 918 | 3.03 s | 17.3 | 303 |
| **median** | | | | **15.3** | **272** |

Server CPU peak: 98 % of one core. Each chunk arrives in a POST with a
base64 body, decoded, fenced and inserted in its own transaction; at
~300 chunks/s the server is CPU-bound on one core, which is the number
to watch before the disk is.

## 4. Runner-loss recovery

`scripts/bench/runner-loss.sh` — compose cluster recreated with
`QUARRY_LEASE_TTL` per row; one `sleep 600` job; `docker compose kill`
of the runner that claimed it; time until attempt 2 is observed
running on another runner (driver polls every 50 ms). The killed
runner is started again between repetitions and reaps its orphaned
container on the way up. Expected: TTL − (age of the last heartbeat,
≤ 5 s) + monitor tick (≤ 5 s) + replacement's poll (≤ 1 s).

| Lease TTL | rep 1 | rep 2 | rep 3 | median |
|---|---|---|---|---|
| 10 s | 12.6 s | 12.7 s | 12.5 s | **12.6 s** |
| 30 s | 31.6 s | 32.5 s | 30.4 s | **31.6 s** |

Recovery is the TTL plus about two seconds; nothing but the TTL is
worth turning. In every repetition the job ran exactly once more, on a
different runner, and the killed runner's container was gone after its
restart (`docker ps -a --filter label=quarry.job` empty).

## Rerunning

```sh
scripts/bench/sched-overhead.sh     # local binaries, no Docker; ~1 min
scripts/bench/throughput.sh         # compose up + 3×30 jobs; ~2 min after the image build
scripts/bench/log-ingest.sh         # compose up with the log-cap override; ~1 min
scripts/bench/runner-loss.sh        # two TTLs × 3 kills; ~5 min
```

Knobs: `QUARRY_BENCH_REPS`, `QUARRY_BENCH_AGENTS`, `QUARRY_BENCH_POLL`,
`QUARRY_BENCH_JOBS`, `QUARRY_BENCH_BYTES`, `QUARRY_BENCH_TTLS`,
`QUARRY_BENCH_IMAGE`, and the compose ones (`QUARRY_PORT`,
`QUARRY_API_TOKEN`). The compose scripts leave the cluster running;
`scripts/demo.sh down` stops it.
