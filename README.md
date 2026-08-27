# Quarry

Quarry is a distributed CI/CD platform written in Go. A control plane
(`cmd/server`) parses `.quarry.yml` pipelines into a DAG and hands jobs to a
fleet of pull-based runners (`cmd/runner`) that execute each step in an
isolated Docker container under lease-based scheduling, so work survives
runner crashes and control-plane restarts. The `quarry` CLI (`cmd/quarry`)
submits pipelines and streams logs, artifacts, and events. Build with
`make build`, test with `make test`.

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
quarry cancel <run>                      request cancellation (server endpoint lands with the
                                         cancel path; 404 until then)
```

`--interval` sets the poll period for `watch`, `--wait` and `-f` (default 1s).
