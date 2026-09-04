// Command server is the Quarry control plane: it opens the store, serves
// the HTTP API and shuts down cleanly on SIGINT/SIGTERM.
//
// Configuration is env-only; defaults live in loadConfig:
//
//	QUARRY_LISTEN        address to bind             (default :8080)
//	QUARRY_DB            SQLite database path        (default quarry.db)
//	QUARRY_API_TOKEN     bearer token for /api/*     (required)
//	QUARRY_LEASE_TTL     job lease duration          (default 30s)
//	QUARRY_MAX_ATTEMPTS  claims per job before an infra/lost_runner
//	                     failure is terminal         (default 3)
//	QUARRY_LOG_CAP       max log bytes per attempt   (default 10485760)
//	QUARRY_ARTIFACT_BACKEND        local or remote     (default local)
//	QUARRY_ARTIFACT_DIR            local store root    (default quarry-artifacts)
//	QUARRY_ARTIFACT_REMOTE_URL     remote coordinator  (required when remote)
//	QUARRY_ARTIFACT_REMOTE_BUCKET  remote bucket       (default quarry-artifacts)
//	QUARRY_ARTIFACT_REMOTE_TOKEN   remote bearer token (default none)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"quarry/internal/api"
	"quarry/internal/artifact"
	"quarry/internal/artifact/local"
	"quarry/internal/artifact/remote"
	"quarry/internal/scheduler"
	"quarry/internal/store"
	"quarry/internal/version"
)

const shutdownTimeout = 10 * time.Second

type config struct {
	listen   string
	dbPath   string
	apiToken string
	leaseTTL time.Duration
	maxAtt   int
	logCap   int64
	backend  string // "local" or "remote"
	blobDir  string
	remote   remote.Config
}

func loadConfig() (config, error) {
	c := config{
		listen:   envOr("QUARRY_LISTEN", ":8080"),
		dbPath:   envOr("QUARRY_DB", "quarry.db"),
		apiToken: os.Getenv("QUARRY_API_TOKEN"),
		leaseTTL: scheduler.DefaultLeaseTTL,
		maxAtt:   scheduler.DefaultMaxAttempts,
		logCap:   scheduler.DefaultLogCapBytes,
		backend:  envOr("QUARRY_ARTIFACT_BACKEND", "local"),
		blobDir:  envOr("QUARRY_ARTIFACT_DIR", "quarry-artifacts"),
		remote: remote.Config{
			BaseURL: os.Getenv("QUARRY_ARTIFACT_REMOTE_URL"),
			Bucket:  envOr("QUARRY_ARTIFACT_REMOTE_BUCKET", "quarry-artifacts"),
			Token:   os.Getenv("QUARRY_ARTIFACT_REMOTE_TOKEN"),
		},
	}
	if c.apiToken == "" {
		return c, errors.New("QUARRY_API_TOKEN must be set")
	}
	switch c.backend {
	case "local":
	case "remote":
		if c.remote.BaseURL == "" {
			return c, errors.New("QUARRY_ARTIFACT_REMOTE_URL must be set for QUARRY_ARTIFACT_BACKEND=remote")
		}
	default:
		return c, fmt.Errorf("QUARRY_ARTIFACT_BACKEND: %q is not local or remote", c.backend)
	}
	if v := os.Getenv("QUARRY_LEASE_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return c, fmt.Errorf("QUARRY_LEASE_TTL: %q is not a positive duration", v)
		}
		c.leaseTTL = d
	}
	if v := os.Getenv("QUARRY_MAX_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return c, fmt.Errorf("QUARRY_MAX_ATTEMPTS: %q is not a positive integer", v)
		}
		c.maxAtt = n
	}
	if v := os.Getenv("QUARRY_LOG_CAP"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return c, fmt.Errorf("QUARRY_LOG_CAP: %q is not a positive byte count", v)
		}
		c.logCap = n
	}
	return c, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version.String("server"))
		return
	}
	// One JSON line per event; every line names the binary. Handlers add
	// request_id, run_id, job_id, attempt and runner_id where they apply.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("component", "server")
	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// run owns every goroutine the server starts: the listener and the lease
// monitor below and the signal watcher inside signal.NotifyContext. All
// end before run returns.
func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := logStateCounts(ctx, st, logger); err != nil {
		return err
	}
	blobs, blobsDesc, err := openArtifacts(ctx, cfg)
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	h := api.New(st, api.Config{
		APIToken: cfg.apiToken, Logger: logger,
		Scheduler: scheduler.Config{LeaseTTL: cfg.leaseTTL, LogCapBytes: cfg.logCap, MaxAttempts: cfg.maxAtt},
		Artifacts: blobs,
	})
	srv := &http.Server{Addr: cfg.listen, Handler: h, ReadHeaderTimeout: 10 * time.Second}
	// The lease monitor stops with ctx; wait for it before the store closes.
	monDone := make(chan struct{})
	go func() {
		defer close(monDone)
		h.RunMonitor(ctx)
	}()
	defer func() { stop(); <-monDone }()
	logger.Info("listening", "version", version.Version, "addr", cfg.listen, "db", cfg.dbPath, "artifacts", blobsDesc, "lease_ttl", cfg.leaseTTL.String(), "max_attempts", cfg.maxAtt)
	return serve(ctx, srv, ln, logger)
}

// openArtifacts opens the configured artifact backend and names it for
// the startup log line. The remote backend ensures its bucket here, so a
// coordinator that is unreachable at startup is a startup failure.
func openArtifacts(ctx context.Context, cfg config) (artifact.Store, string, error) {
	if cfg.backend == "remote" {
		s, err := remote.New(ctx, cfg.remote)
		if err != nil {
			return nil, "", err
		}
		return s, s.Describe(), nil
	}
	s, err := local.New(cfg.blobDir)
	if err != nil {
		return nil, "", err
	}
	return s, s.Root(), nil
}

func logStateCounts(ctx context.Context, st *store.Store, logger *slog.Logger) error {
	counts, err := st.StateCounts(ctx)
	if err != nil {
		return fmt.Errorf("startup state counts: %w", err)
	}
	logger.Info("startup states", "runs", counts["runs"], "jobs", counts["jobs"], "runners", counts["runners"])
	return nil
}

// serve owns the serving goroutine and drains requests before the caller
// closes SQLite. Request contexts are independent of the signal context.
func serve(ctx context.Context, srv *http.Server, ln net.Listener, logger *slog.Logger) error {
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		_ = srv.Close()
		<-errc
		return fmt.Errorf("shutdown: %w", err)
	}
	<-errc // ListenAndServe returns ErrServerClosed once Shutdown begins.
	return nil
}
