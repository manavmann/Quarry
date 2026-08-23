# Progress

Read this first each session. Newest entry on top.

## C02 · pipeline: parse and validate .quarry.yml, build DAG — done

- Landed: `internal/pipeline` — `Parse([]byte)` (yaml.v3, unknown fields
  rejected), `Pipeline`/`Job`/`Resources` types, 30m default timeout,
  `ValidationError` listing every problem with job names, Kahn cycle
  detection that names the jobs in the cycle, `TopoOrder()` (declaration-
  order tie-break), `Roots()`, `Dependents()`, `Job()`. `gopkg.in/yaml.v3`
  added to `go.mod`.
- Flaky: nothing.
- Next: C03.
known gap: unknown-field errors report field/line, not job name (yaml.v3 raw message).

## C01 · init: module layout, Makefile, CI workflow, CLAUDE.md — done

- Landed: `go.mod` (module `quarry`), `cmd/{server,runner,quarry}` stubs that
  print `<name> <version>`, `internal/version` (ldflags-stamped `Version`),
  `Makefile` (`build test test-docker lint`), `.github/workflows/ci.yml`
  (gofmt + `go vet`, `go test -race ./...`, `make build`), `CLAUDE.md`,
  README stub.
- Flaky: nothing.
- Next: C02.
