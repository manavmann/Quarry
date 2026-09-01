#!/usr/bin/env sh
# Quarry demo v1: bring up the compose cluster, pre-pull the job image,
# submit examples/go-app and watch it run. Needs Docker (with compose v2)
# and Go; nothing else. Run from anywhere:
#
#   scripts/demo.sh            # up + run + watch
#   scripts/demo.sh cancel     # up + run + cancel it mid-flight + watch it end cancelled
#   scripts/demo.sh down       # tear the cluster down (keeps quarry-data)
#   scripts/demo.sh down -v    # ... and delete the data volume too
#
# Env: QUARRY_API_TOKEN (default dev-token), QUARRY_PORT (default 8080),
# QUARRY_KEEP_UP=1 leaves the cluster running after a failed run.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="docker compose -f $root/deploy/docker-compose.yml"
export QUARRY_API_TOKEN="${QUARRY_API_TOKEN:-dev-token}"
export QUARRY_TOKEN="$QUARRY_API_TOKEN"
export QUARRY_SERVER="http://127.0.0.1:${QUARRY_PORT:-8080}"
job_image=$(sed -n 's/^ *image: *//p' "$root/examples/go-app/.quarry.yml" | head -n 1)

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

if [ "${1:-}" = "down" ]; then
  shift
  say "docker compose down $*"
  $compose down "$@"
  exit 0
fi

say "Building quarry CLI"
(cd "$root" && go build -o bin/quarry ./cmd/quarry)
quarry="$root/bin/quarry"

say "Starting server + 3 runners (docker compose up --build --wait)"
$compose up -d --build --wait

say "Pre-pulling the job image ($job_image) so the first run is not a download"
docker pull -q "$job_image"

say "Runners registered:"
"$quarry" runners

say "Submitting examples/go-app"
if [ "${1:-}" = "cancel" ]; then
  # Hold the first job inside its container long enough to observe and
  # cancel it, without changing the example pipeline on disk.
  cancel_pipeline=$(mktemp)
  trap 'rm -f "$cancel_pipeline"' 0
  awk '{ print } !added && /^    steps:/ {
    print "      - echo quarry-cancel-ready"
    print "      - sleep 60"
    added = 1
  }' "$root/examples/go-app/.quarry.yml" > "$cancel_pipeline"
  run=$("$quarry" run -C "$root/examples/go-app" -f "$cancel_pipeline")
  rm -f "$cancel_pipeline"
  trap - 0
else
  run=$("$quarry" run -C "$root/examples/go-app")
fi
echo "run $run"

if [ "${1:-}" = "cancel" ]; then
  # The cancel beat: observe output from an executing container, cancel
  # the run, then watch it end cancelled. watch exits 1
  # for any non-succeeded run, which here is the expected outcome.
  say "Waiting for a job container of $run to execute its ready marker"
  i=0
  while :; do
    containers=$(docker ps --filter label=quarry.run="$run" --filter status=running --format "{{.ID}}")
    container=$(printf '%s\n' "$containers" | head -n 1)
    if [ -n "$container" ] && docker logs "$container" 2>&1 | grep -qx quarry-cancel-ready; then
      [ "$(docker inspect --format '{{.State.Running}}' "$container")" = true ] && break
    fi
    i=$((i + 1))
    if [ "$i" -gt 120 ]; then echo "no executing job observed within 60 s"; "$quarry" status "$run"; exit 1; fi
    sleep 0.5
  done
  echo "Container $container emitted quarry-cancel-ready and is still running."
  "$quarry" status "$run"
  say "Cancelling run $run"
  "$quarry" cancel "$run"
  say "Watching run $run wind down"
  if "$quarry" watch "$run"; then
    say "Run $run succeeded despite the cancel — the cancel did not land"
    exit 1
  else
    status=$?
    [ "$status" -eq 1 ] || exit "$status"
  fi
  if ! "$quarry" status "$run" | head -n 1 | grep -q cancelled; then
    say "Run $run ended in a state other than cancelled"
    "$quarry" status "$run"; exit 1
  fi
  say "Run $run is cancelled; events:"
  "$quarry" events "$run"
  containers=$(docker ps --filter label=quarry.run="$run" --format "{{.Names}}")
  if [ -n "$containers" ]; then
    say "job containers of $run are still running:"; docker ps --filter label=quarry.run="$run"; exit 1
  fi
  echo "No job container of $run left running."
  echo "Tear down with: $0 down"
  exit 0
fi

say "Watching run $run"
if "$quarry" watch "$run"; then
  say "Run $run succeeded"
  # The package job's id comes from GET /api/runs/{id}; jobJSON emits
  # id, run_id, name in that order, so one sed is enough without jq.
  if command -v curl >/dev/null 2>&1; then
    pkg=$(curl -fsS -H "Authorization: Bearer $QUARRY_TOKEN" "$QUARRY_SERVER/api/runs/$run" |
      sed -n 's/.*{"id":"\([^"]*\)","run_id":"[^"]*","name":"package".*/\1/p')
    if [ -n "$pkg" ]; then
      say "Artifacts of job package ($pkg)"
      "$quarry" artifacts "$pkg"
      echo "Download with:  QUARRY_TOKEN=$QUARRY_TOKEN $quarry artifacts $pkg --download out"
    fi
  fi
  echo "Events:         QUARRY_TOKEN=$QUARRY_TOKEN $quarry events $run"
  echo "Tear down with: $0 down"
else
  status=$?
  say "Run $run did not succeed (exit $status)"
  "$quarry" events "$run" || true
  echo "Server logs:"; $compose logs --tail 40 server || true
  [ "${QUARRY_KEEP_UP:-}" = "1" ] || $compose down
  exit "$status"
fi
