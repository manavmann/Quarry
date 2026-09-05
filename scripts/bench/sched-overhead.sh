#!/usr/bin/env sh
# Benchmark 1: scheduling overhead. Builds the binaries, then for 1, 3 and
# 10 fake runners started on this machine (QUARRY_EXECUTOR=fake, so a job
# costs nothing to run) submits 100-job fan-outs and reports the
# queued→running p50/p95 stamped by the server. No Docker involved.
#
#   scripts/bench/sched-overhead.sh              # agents 1 3 10, poll 1s (the runner default)
#   QUARRY_BENCH_AGENTS="3" QUARRY_BENCH_POLL=100ms scripts/bench/sched-overhead.sh
set -eu
. "$(dirname -- "$0")/common.sh"
disclose

say "Building server and runner"
(cd "$root" && go build -o bin/server ./cmd/server && go build -o bin/runner ./cmd/runner)

for n in ${QUARRY_BENCH_AGENTS:-1 3 10}; do
  say "Scheduling overhead with $n fake runner(s)"
  (cd "$root" && go run ./scripts/bench sched -bin bin -agents "$n" -jobs "${QUARRY_BENCH_JOBS:-100}" \
     -reps "$reps" -poll "${QUARRY_BENCH_POLL:-1s}")
done
