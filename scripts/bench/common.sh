# Shared by the scripts/bench/*.sh entry points; not executable on its own.
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
compose="docker compose -f $root/deploy/docker-compose.yml"
export QUARRY_API_TOKEN="${QUARRY_API_TOKEN:-dev-token}"
export QUARRY_TOKEN="$QUARRY_API_TOKEN"
export QUARRY_SERVER="http://127.0.0.1:${QUARRY_PORT:-8080}"
image="${QUARRY_BENCH_IMAGE:-alpine:3.20}"
reps="${QUARRY_BENCH_REPS:-3}"

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# disclose prints the environment a result should be read with.
disclose() {
  say "Environment"
  echo "date:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "commit:  $(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "go:      $(go version)"
  echo "os:      $(uname -srm)"
  if command -v docker >/dev/null 2>&1; then
    echo "docker:  $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo unavailable), $(docker info --format '{{.NCPU}} cpus, {{.MemTotal}} bytes' 2>/dev/null)"
  fi
}
