# Quarry

Quarry is a distributed CI/CD platform written in Go. A control plane
(`cmd/server`) parses `.quarry.yml` pipelines into a DAG and hands jobs to a
fleet of pull-based runners (`cmd/runner`) that execute each step in an
isolated Docker container under lease-based scheduling, so work survives
runner crashes and control-plane restarts. The `quarry` CLI (`cmd/quarry`)
submits pipelines and streams logs, artifacts, and events. Build with
`make build`, test with `make test`.
