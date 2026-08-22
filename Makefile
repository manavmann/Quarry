GO      ?= go
BIN     ?= bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X quarry/internal/version.Version=$(VERSION)

ifeq ($(OS),Windows_NT)
EXE := .exe
endif

.PHONY: build test test-docker lint clean

build:
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/server$(EXE) ./cmd/server
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/runner$(EXE) ./cmd/runner
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/quarry$(EXE) ./cmd/quarry

test:
	$(GO) test -race ./...

test-docker:
	$(GO) test -race -tags docker ./...

lint:
	@unformatted="$$(gofmt -l ./cmd ./internal)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	$(GO) vet ./...

clean:
	rm -rf $(BIN)
