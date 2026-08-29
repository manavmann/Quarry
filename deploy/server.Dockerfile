# Quarry control plane. Build from the repo root:
#   docker build -f deploy/server.Dockerfile -t quarry-server .
#
# The state directory /var/lib/quarry (SQLite DB + local artifact store)
# is a volume; compose names it so it survives `docker compose down`.
FROM golang:1.27-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X quarry/internal/version.Version=${VERSION}" \
      -o /out/server ./cmd/server

FROM alpine:3.20
RUN addgroup -S quarry && adduser -S -G quarry -h /var/lib/quarry quarry
COPY --from=build /out/server /usr/local/bin/server
USER quarry
WORKDIR /var/lib/quarry
VOLUME /var/lib/quarry
ENV QUARRY_LISTEN=:8080 \
    QUARRY_DB=/var/lib/quarry/quarry.db \
    QUARRY_ARTIFACT_DIR=/var/lib/quarry/artifacts
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/server"]
