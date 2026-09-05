#!/usr/bin/env sh
# Benchmark 3: log ingestion. Brings the compose cluster up with the log
# cap raised (scripts/bench/compose.override.yml), runs one job that emits
# 50 MB of 100-byte lines, and reports MB/s and chunks/s over the attempt's
# running time plus the server container's peak CPU while it ran.
set -eu
. "$(dirname -- "$0")/common.sh"
disclose

say "Starting the compose cluster with QUARRY_LOG_CAP raised"
compose="$compose -f $root/scripts/bench/compose.override.yml"
$compose up -d --build --wait
docker pull -q "$image"
server_ctr=$($compose ps -q server)

# Sample the server's CPU every second while the driver runs.
samples=$(mktemp)
(
  while :; do
    docker stats --no-stream --format '{{.CPUPerc}}' "$server_ctr" 2>/dev/null || true
    sleep 1
  done
) > "$samples" &
sampler=$!
trap 'kill $sampler 2>/dev/null; rm -f "$samples"' 0

say "Log ingestion, one job emitting ${QUARRY_BENCH_BYTES:-52428800} bytes"
(cd "$root" && go run ./scripts/bench logs -bytes "${QUARRY_BENCH_BYTES:-52428800}" -reps "$reps" -image "$image")

kill $sampler 2>/dev/null
echo "server CPU (docker stats, 1 s samples, one core = 100%): peak $(tr -d '%' < "$samples" | sort -n | tail -n 1)%"
