#!/usr/bin/env sh
# Quarry demo v1: bring up the compose cluster, pre-pull the job image,
# submit examples/go-app and watch it run. Needs Docker (with compose v2)
# and Go; nothing else. Run from anywhere:
#
#   scripts/demo.sh            # up + run + watch
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
run=$("$quarry" run -C "$root/examples/go-app")
echo "run $run"

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
