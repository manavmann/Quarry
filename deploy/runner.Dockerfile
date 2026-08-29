# Quarry runner. Build from the repo root:
#   docker build -f deploy/runner.Dockerfile -t quarry-runner .
#
# The runner runs as root on purpose: it drives the *host* Docker daemon
# through the mounted /var/run/docker.sock (Docker-out-of-Docker), and
# the socket's group id differs per host (0 on Docker Desktop, `docker`
# on Linux). Job containers it starts never get the socket or
# privileges — see docs/design-decisions.md.
FROM golang:1.27-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X quarry/internal/version.Version=${VERSION}" \
      -o /out/runner ./cmd/runner

FROM alpine:3.20
COPY --from=build /out/runner /usr/local/bin/runner
ENV QUARRY_EXECUTOR=docker
ENTRYPOINT ["/usr/local/bin/runner"]
