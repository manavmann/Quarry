#!/usr/bin/env sh
# Quarry demo v2: bring up the compose cluster, pre-pull the job image,
# submit examples/go-app and watch it run — optionally while something
# goes wrong. Needs Docker (with compose v2) and Go; nothing else. Run
# from anywhere:
#
#   scripts/demo.sh                 # up + run + watch
#   scripts/demo.sh kill-runner     # ... SIGKILL the runner holding a job mid-step;
#                                   #     the job lands on another runner as attempt 2
#   scripts/demo.sh restart-server  # ... restart the control plane mid-run;
#                                   #     the run finishes on attempt 1
#   scripts/demo.sh cancel          # ... cancel the run mid-flight; watch it end cancelled
#   scripts/demo.sh remote          # ... with artifacts stored in the replicated object
#                                   #     store (compose profile `remote`, needs its image)
#   scripts/demo.sh down            # tear the cluster down (keeps quarry-data)
#   scripts/demo.sh down -v         # ... and delete the data volume too
#
# Env: QUARRY_API_TOKEN (default dev-token), QUARRY_PORT (default 8080),
# QUARRY_SERVER (default http://127.0.0.1:$QUARRY_PORT), QUARRY_LEASE_TTL
# (compose default 30s; the kill-runner beat takes that long to recover),
# QUARRY_CLI (a prebuilt quarry binary; skips `go build`),
# QUARRY_KEEP_UP=1 leaves the cluster running after a failed run.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="docker compose -f $root/deploy/docker-compose.yml"
export QUARRY_API_TOKEN="${QUARRY_API_TOKEN:-dev-token}"
export QUARRY_TOKEN="$QUARRY_API_TOKEN"
export QUARRY_SERVER="${QUARRY_SERVER:-http://127.0.0.1:${QUARRY_PORT:-8080}}"
job_image=$(sed -n 's/^ *image: *//p' "$root/examples/go-app/.quarry.yml" | head -n 1)
beat=${1:-run}

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { say "$*"; exit 1; }

case "$beat" in
  down)
    shift
    say "docker compose down $*"
    # Every profile, so the remote and prometheus services come down too.
    COMPOSE_PROFILES=remote,prometheus $compose down "$@"
    exit 0 ;;
  run|kill-runner|restart-server|cancel|remote) ;;
  *) fail "unknown beat '$beat' (run | kill-runner | restart-server | cancel | remote | down)" ;;
esac

if [ -n "${QUARRY_CLI:-}" ]; then
  quarry=$QUARRY_CLI
else
  say "Building quarry CLI"
  (cd "$root" && go build -o bin/quarry ./cmd/quarry)
  quarry="$root/bin/quarry"
fi

if [ "$beat" = remote ]; then
  # Both the profile (the storage cluster) and the backend switch (the
  # server's artifact store) are needed; see deploy/docker-compose.yml.
  export COMPOSE_PROFILES=remote QUARRY_ARTIFACT_BACKEND=remote
  say "Starting server + 3 runners + object store (docker compose --profile remote build + up --wait)"
else
  say "Starting server + 3 runners (docker compose build + up --wait)"
fi
# Build each image once (server, and the runner image via runner-1) before
# up: `up --build` builds runner-1/2/3 in parallel through bake and, on a
# cold cache, two of them fail with `image quarry-runner:dev already exists`.
$compose build -q server runner-1
$compose up -d --wait

say "Pre-pulling the job image ($job_image) so the first run is not a download"
docker pull -q "$job_image"

say "Runners registered:"
"$quarry" runners

# hold_pipeline MARKER STEP writes a copy of the example pipeline whose
# first job echoes MARKER and then runs STEP, so a beat can observe an
# executing container and act on it without changing the example on disk.
hold_pipeline() {
  awk -v marker="$1" -v step="$2" '{ print } !added && /^    steps:/ {
    print "      - echo " marker
    print "      - " step
    added = 1
  }' "$root/examples/go-app/.quarry.yml"
}

# submit_held MARKER STEP submits the held pipeline and sets $run.
submit_held() {
  held=$(mktemp)
  hold_pipeline "$1" "$2" > "$held"
  run=$("$quarry" run -C "$root/examples/go-app" -f "$held")
  rm -f "$held"
}

# wait_marker RUN MARKER polls until a running job container of RUN has
# printed MARKER and is still running; sets $container.
wait_marker() {
  i=0
  while :; do
    container=$(docker ps --filter label=quarry.run="$1" --filter status=running --format "{{.ID}}" | head -n 1)
    if [ -n "$container" ] && docker logs "$container" 2>&1 | grep -qx "$2"; then
      [ "$(docker inspect --format '{{.State.Running}}' "$container")" = true ] && break
    fi
    i=$((i + 1))
    if [ "$i" -gt 120 ]; then "$quarry" status "$1"; fail "no executing job observed within 60 s"; fi
    sleep 0.5
  done
}

# job_field RUN JOB N prints column N of JOB's row in the status table
# (2 state, 3 attempt without the /max suffix, 4 runner).
job_field() {
  "$quarry" status "$1" | awk -v job="$2" -v n="$3" '$1 == job { sub("/.*", "", $3); print $n }'
}

# no_run_containers RUN polls up to 30 s until no job container of RUN
# exists, running or not.
no_run_containers() {
  i=0
  while [ -n "$(docker ps -aq --filter label=quarry.run="$1")" ]; do
    i=$((i + 1))
    if [ "$i" -gt 60 ]; then docker ps -a --filter label=quarry.run="$1"; fail "job containers of $1 left behind"; fi
    sleep 0.5
  done
}

say "Submitting examples/go-app"
case "$beat" in
  kill-runner)
    # Attempt 1 of build sleeps so there is a lease to lose; attempt 2,
    # on the surviving runner, runs the real steps at once.
    submit_held quarry-hold 'test "${QUARRY_ATTEMPT:-1}" -gt 1 || sleep 120' ;;
  restart-server) submit_held quarry-hold 'sleep 20' ;;
  cancel)         submit_held quarry-hold 'sleep 60' ;;
  *)              run=$("$quarry" run -C "$root/examples/go-app") ;;
esac
echo "run $run"

if [ "$beat" = kill-runner ]; then
  say "Waiting for a job of $run to execute"
  wait_marker "$run" quarry-hold
  runner=$(docker inspect --format '{{index .Config.Labels "quarry.runner"}}' "$container")
  "$quarry" status "$run"
  say "Killing $runner (docker compose kill: SIGKILL, no goodbye, its job container keeps running)"
  $compose kill "$runner"
  ttl=$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$($compose ps -q server)" | sed -n 's/^QUARRY_LEASE_TTL=//p')
  say "Watching $run: the lease expires after QUARRY_LEASE_TTL=${ttl:-30s}, the monitor requeues build, another runner claims attempt 2"
  "$quarry" watch "$run" || fail "run $run did not succeed after the runner loss"
  say "Events of $run (job.requeued with reason lost_runner, then a claim by a different runner):"
  "$quarry" events "$run"
  attempt=$(job_field "$run" build 3); replacement=$(job_field "$run" build 4)
  [ "$attempt" = 2 ] || fail "build finished as attempt $attempt, expected 2"
  [ "$replacement" != "$runner" ] || fail "build's attempt 2 ran on the killed runner $runner"
  "$quarry" events "$run" | grep -q 'job.requeued.*lost_runner' || fail "no job.requeued/lost_runner event"
  say "build: attempt 2 on $replacement; attempt 1's container is still sleeping on the daemon:"
  docker ps --filter label=quarry.run="$run" --format 'table {{.Names}}\t{{.Status}}\t{{.Label "quarry.runner"}}'
  say "Starting $runner again (docker compose start): it reaps its orphaned container on the way up"
  $compose start "$runner"
  no_run_containers "$run"
  echo "No job container of $run left."
  "$quarry" runners
  echo "Tear down with: $0 down"
  exit 0
fi

if [ "$beat" = restart-server ]; then
  say "Waiting for a job of $run to execute"
  wait_marker "$run" quarry-hold
  "$quarry" status "$run"
  say "Restarting the control plane mid-run (docker compose restart server)"
  $compose restart server
  say "Watching $run: runners reconnect, the running attempt keeps its lease, nothing is re-run"
  "$quarry" watch "$run" || fail "run $run did not succeed across the server restart"
  attempt=$(job_field "$run" build 3)
  [ "$attempt" = 1 ] || fail "build finished as attempt $attempt, expected 1"
  if "$quarry" events "$run" | grep -q job.requeued; then fail "a job was requeued across the restart"; fi
  say "Events of $run (no job.requeued):"
  "$quarry" events "$run"
  echo "Tear down with: $0 down"
  exit 0
fi

if [ "$beat" = cancel ]; then
  # The cancel beat: observe output from an executing container, cancel
  # the run, then watch it end cancelled. watch exits 1
  # for any non-succeeded run, which here is the expected outcome.
  say "Waiting for a job container of $run to execute its ready marker"
  wait_marker "$run" quarry-hold
  echo "Container $container emitted quarry-hold and is still running."
  "$quarry" status "$run"
  say "Cancelling run $run"
  "$quarry" cancel "$run"
  say "Watching run $run wind down"
  if "$quarry" watch "$run"; then
    fail "Run $run succeeded despite the cancel — the cancel did not land"
  else
    status=$?
    [ "$status" -eq 1 ] || exit "$status"
  fi
  if ! "$quarry" status "$run" | head -n 1 | grep -q cancelled; then
    "$quarry" status "$run"; fail "Run $run ended in a state other than cancelled"
  fi
  say "Run $run is cancelled; events:"
  "$quarry" events "$run"
  containers=$(docker ps --filter label=quarry.run="$run" --format "{{.Names}}")
  if [ -n "$containers" ]; then
    docker ps --filter label=quarry.run="$run"; fail "job containers of $run are still running"
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
      if [ "$beat" = remote ]; then
        # Round trip through the coordinator: the server served the
        # listing above from the object store; now fetch the bytes back.
        out=$(mktemp -d)
        say "Downloading them back out of the object store"
        "$quarry" artifacts "$pkg" --download "$out"
        [ -s "$out/dist/go-app" ] || fail "dist/go-app did not come back from the remote store"
        rm -rf "$out"
        # And the coordinator itself must hold the object under this job.
        bucket="http://127.0.0.1:${QUARRY_STORAGE_PORT:-8090}/v1/${QUARRY_ARTIFACT_REMOTE_BUCKET:-quarry-artifacts}/"
        say "Coordinator's view of the bucket ($bucket)"
        curl -fsS "$bucket" | grep -o "\"key\":\"[^\"]*\"" | grep "jobs/$pkg/" ||
          fail "no object for job $pkg in the coordinator's bucket"
      else
        echo "Download with:  QUARRY_TOKEN=$QUARRY_TOKEN $quarry artifacts $pkg --download out"
      fi
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
