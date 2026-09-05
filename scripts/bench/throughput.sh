#!/usr/bin/env sh
# Benchmark 2: throughput. Brings the compose cluster up (3 runners at
# capacity 2, Docker executor), pre-pulls the job image, and submits
# fan-outs of trivial `true` jobs; reports jobs/min. This measures the
# cost of starting and stopping a container per job, not the scheduler.
# The cluster is left running; `scripts/demo.sh down` stops it.
set -eu
. "$(dirname -- "$0")/common.sh"
disclose

say "Starting the compose cluster"
$compose up -d --build --wait
docker pull -q "$image"

say "Throughput on 3 runners x capacity 2"
(cd "$root" && go run ./scripts/bench throughput -jobs "${QUARRY_BENCH_JOBS:-30}" -reps "$reps" -image "$image")
