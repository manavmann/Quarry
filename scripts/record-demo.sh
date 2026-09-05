#!/usr/bin/env sh
# Records docs/demo.gif: the kill-runner beat of scripts/demo.sh, captured
# as an asciicast by scripts/record (standard library, timestamps each
# line as it arrives) and rendered by asciinema's agg from its container
# image. Nothing is installed on the host; needs Docker and Go.
#
#   scripts/record-demo.sh          # writes docs/demo.cast and docs/demo.gif
#
# The cluster is (re)created with QUARRY_LEASE_TTL=10s so the recording
# shows a ~12 s recovery instead of ~32 s; run `scripts/demo.sh` afterwards
# to return to the compose default of 30s. Env: QUARRY_API_TOKEN (default
# dev-token), QUARRY_PORT (default 8080), QUARRY_AGG (agg image, default
# ghcr.io/asciinema/agg).
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
agg=${QUARRY_AGG:-ghcr.io/asciinema/agg}
cols=100
rows=32

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

say "Recreating the cluster with QUARRY_LEASE_TTL=10s"
QUARRY_LEASE_TTL=10s docker compose -f "$root/deploy/docker-compose.yml" build -q server runner-1
QUARRY_LEASE_TTL=10s docker compose -f "$root/deploy/docker-compose.yml" up -d --wait

say "Recording scripts/demo.sh kill-runner -> docs/demo.cast"
cd "$root"
# The recorder runs the demo on a pipe and starts the cast at the runner
# table, after the CLI build and compose up.
QUARRY_LEASE_TTL=10s \
  go run ./scripts/record -o docs/demo.cast -cols "$cols" -rows "$rows" -from 'Runners registered' -- sh scripts/demo.sh kill-runner

say "Rendering docs/demo.gif with $agg"
# The bind-mount source must be a path the daemon understands: Git Bash on
# Windows needs the Windows spelling and must not rewrite the container paths.
hostdocs="$root/docs"
case "$(uname -s)" in MINGW*|MSYS*) hostdocs=$(cd "$hostdocs" && pwd -W) ;; esac
MSYS_NO_PATHCONV=1 docker run --rm -v "$hostdocs:/data" "$agg" \
  --cols "$cols" --rows "$rows" --font-size 15 --idle-time-limit 3 --last-frame-duration 5 --theme monokai \
  /data/demo.cast /data/demo.gif
ls -l "$root/docs/demo.cast" "$root/docs/demo.gif"
