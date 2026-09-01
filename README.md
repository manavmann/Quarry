# Quarry

Quarry is a distributed CI/CD platform written in Go. A control plane
(`cmd/server`) parses `.quarry.yml` pipelines into a DAG and hands jobs to a
fleet of pull-based runners (`cmd/runner`) that execute each step in an
isolated Docker container under lease-based scheduling, so work survives
runner crashes and control-plane restarts. The `quarry` CLI (`cmd/quarry`)
submits pipelines and streams logs, artifacts, and events. Build with
`make build`, test with `make test`.

## Quickstart

Needs Docker (with compose v2) and Go; no other services. From a fresh
clone:

```sh
scripts/demo.sh
```

The script builds the `quarry` CLI, starts the compose cluster in
[deploy/](deploy/) (one server on a named data volume plus three labeled
runners that execute jobs on your Docker daemon), pre-pulls the job
image, submits [examples/go-app](examples/go-app) and watches it run:
`build` → `test` + `lint` in parallel → `package`, which publishes
`dist/go-app` as an artifact. `scripts/demo.sh down` tears it down
(`down -v` also deletes the data volume). Step by step, the same thing is:

```sh
docker compose -f deploy/docker-compose.yml up -d --build --wait
export QUARRY_TOKEN=dev-token            # QUARRY_API_TOKEN in the compose env
go build -o bin/quarry ./cmd/quarry
bin/quarry run --wait -C examples/go-app  # prints the run id, then the job table
bin/quarry runners                        # runner-1..3, online, labels
```

CI runs exactly this on Linux and asserts the run finishes in under two
minutes on a cold image cache. Job containers never receive the Docker
socket or privileges; only the runner does (see
[docs/design-decisions.md](docs/design-decisions.md) and
[docs/architecture.md](docs/architecture.md)).

## CLI usage

`quarry` talks to a control plane over HTTP. Point it at the server with
`QUARRY_SERVER` (default `http://127.0.0.1:8080`) and authenticate with
`QUARRY_TOKEN`; `--server` / `--token` override either. Every command retries
transient connection errors before giving up.

```
quarry run [--wait] [-C dir] [-f file]   bundle the workspace (excluding .git, symlinks not
                                         followed), submit <dir>/.quarry.yml, print the run id
quarry runs [-n 20]                      list recent runs
quarry status <run>                      show a run and its jobs once
quarry watch <run>                       redraw the job table (state, attempt, runner, duration)
                                         in DAG order until the run finishes; exit 1 unless succeeded
quarry logs <job> [-f] [--attempt N]     print a job's log; -f follows until the job is
                                         finished and the log is drained
quarry events <run> [-f]                 print a run's events
quarry runners                           list registered runners
quarry cancel <run>                      cancel a run: unstarted jobs are cancelled at once,
                                         running ones killed on their next heartbeat
```

`--interval` sets the poll period for `watch`, `--wait` and `-f` (default 1s).
