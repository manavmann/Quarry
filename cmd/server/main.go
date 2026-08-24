// Command server is the Quarry control plane: it opens the store, serves
// the HTTP API and shuts down cleanly on SIGINT/SIGTERM.
//
// Configuration is env-only; defaults live in loadConfig:
//
//	QUARRY_LISTEN     address to bind            (default :8080)
//	QUARRY_DB         SQLite database path       (default quarry.db)
//	QUARRY_API_TOKEN  bearer token for /api/*    (required)
//	QUARRY_LEASE_TTL  job lease duration          (default 30s)
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"quarry/internal/api"
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
}

func loadConfig() (config, error) {
	c := config{
		listen:   envOr("QUARRY_LISTEN", ":8080"),
		dbPath:   envOr("QUARRY_DB", "quarry.db"),
		apiToken: os.Getenv("QUARRY_API_TOKEN"),
		leaseTTL: scheduler.DefaultLeaseTTL,
	}
	if c.apiToken == "" {
		return c, errors.New("QUARRY_API_TOKEN must be set")
	}
	if v := os.Getenv("QUARRY_LEASE_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return c, fmt.Errorf("QUARRY_LEASE_TTL: %q is not a positive duration", v)
		}
		c.leaseTTL = d
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
	logger := log.New(os.Stderr, "server: ", log.LstdFlags|log.Lmsgprefix)
	if err := run(logger); err != nil {
		logger.Fatal(err)
	}
}

// run owns every goroutine the server starts: the listener below and the
// signal watcher inside signal.NotifyContext. Both end before run returns.
func run(logger *log.Logger) error {
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

	srv := &http.Server{
		Addr: cfg.listen,
		Handler: api.New(st, api.Config{
			APIToken: cfg.apiToken, Logger: logger, Scheduler: scheduler.Config{LeaseTTL: cfg.leaseTTL},
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Printf("%s listening on %s (db %s, lease ttl %s)", version.String("server"), cfg.listen, cfg.dbPath, cfg.leaseTTL)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	logger.Print("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	<-errc // ListenAndServe returns ErrServerClosed once Shutdown begins.
	return nil
}
