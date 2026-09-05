#!/usr/bin/env sh
# Benchmark 4: runner-loss recovery. For each lease TTL, recreates the
# compose server with QUARRY_LEASE_TTL set, submits a long job, kills the
# runner container that claimed it and measures the time until attempt 2
# is running on another runner. The killed runner is started again
# between repetitions.
#
#   scripts/bench/runner-loss.sh                 # TTLs 10s and 30s
#   QUARRY_BENCH_TTLS="5s 30s 60s" scripts/bench/runner-loss.sh
set -eu
. "$(dirname -- "$0")/common.sh"
disclose

for ttl in ${QUARRY_BENCH_TTLS:-10s 30s}; do
  say "Starting the compose cluster with QUARRY_LEASE_TTL=$ttl"
  QUARRY_LEASE_TTL="$ttl" $compose up -d --build --wait
  docker pull -q "$image"
  say "Runner-loss recovery at TTL $ttl"
  (cd "$root" && go run ./scripts/bench recovery -ttl "$ttl" -reps "$reps" -image "$image" \
     -kill "$compose kill {runner}" -restore "$compose start {runner}")
done
