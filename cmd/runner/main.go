// Command runner is the Quarry agent: it claims jobs from the control
// plane, runs them through an executor, heartbeats and reports results,
// and drains cleanly on SIGINT/SIGTERM.
//
// Configuration is env-only; unset intervals and capacity take the
// defaults in package agent:
//
//	QUARRY_SERVER              control plane base URL      (required)
//	QUARRY_API_TOKEN           bearer token                (required)
//	QUARRY_RUNNER_NAME         runner name; registration   (default hostname)
//	                           resolves it to a runner_id
//	QUARRY_LABELS              k=v,k=v                     (default none)
//	QUARRY_CAPACITY            concurrent jobs             (default 2)
//	QUARRY_POLL_INTERVAL       claim interval when idle    (default 1s)
//	QUARRY_HEARTBEAT_INTERVAL  lease refresh interval      (default 5s)
//	QUARRY_EXECUTOR            fake | docker              (default fake)
//	QUARRY_KEEP_FAILED         1 keeps failed containers   (default unset; docker only)
//	QUARRY_METRICS_LISTEN      address serving /metrics    (default unset: no listener)
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
	"strings"
	"syscall"
	"time"

	"quarry/internal/agent"
	"quarry/internal/executor"
	"quarry/internal/executor/docker"
	"quarry/internal/metrics"
	"quarry/internal/version"
)

type config struct {
	agent      agent.Config
	executor   string
	keepFailed bool
	metrics    string // listen address for /metrics; "" disables it
}

func loadConfig() (config, error) {
	var c config
	c.agent = agent.Config{
		ServerURL: strings.TrimRight(os.Getenv("QUARRY_SERVER"), "/"),
		Token:     os.Getenv("QUARRY_API_TOKEN"),
		Name:      os.Getenv("QUARRY_RUNNER_NAME"),
		Capacity:  agent.DefaultCapacity,
		Version:   version.Version,
	}
	c.executor = envOr("QUARRY_EXECUTOR", "fake")
	c.keepFailed = os.Getenv("QUARRY_KEEP_FAILED") == "1"
	c.metrics = os.Getenv("QUARRY_METRICS_LISTEN")
	if c.agent.ServerURL == "" || c.agent.Token == "" {
		return c, errors.New("QUARRY_SERVER and QUARRY_API_TOKEN must be set")
	}
	if c.agent.Name == "" {
		host, err := os.Hostname()
		if err != nil {
			return c, fmt.Errorf("QUARRY_RUNNER_NAME unset and hostname unavailable: %w", err)
		}
		c.agent.Name = host
	}
	if v := os.Getenv("QUARRY_LABELS"); v != "" {
		labels, err := parseLabels(v)
		if err != nil {
			return c, err
		}
		c.agent.Labels = labels
	}
	if v := os.Getenv("QUARRY_CAPACITY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return c, fmt.Errorf("QUARRY_CAPACITY: %q is not a positive integer", v)
		}
		c.agent.Capacity = n
	}
	var err error
	if c.agent.PollInterval, err = envDuration("QUARRY_POLL_INTERVAL"); err != nil {
		return c, err
	}
	if c.agent.HeartbeatInterval, err = envDuration("QUARRY_HEARTBEAT_INTERVAL"); err != nil {
		return c, err
	}
	return c, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envDuration parses key as a positive duration, or returns 0 when unset
// so the agent applies its default.
func envDuration(key string) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s: %q is not a positive duration", key, v)
	}
	return d, nil
}

// parseLabels reads "k=v,k2=v2".
func parseLabels(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		if !ok || strings.TrimSpace(k) == "" {
			return nil, fmt.Errorf("QUARRY_LABELS: %q is not k=v", kv)
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out, nil
}

func newExecutor(cfg config) (executor.Executor, error) {
	switch cfg.executor {
	case "fake":
		return executor.NewFake(), nil
	case "docker":
		return docker.New(docker.Config{
			RunnerName: cfg.agent.Name,
			Source:     agent.SourceFetcher(cfg.agent),
			KeepFailed: cfg.keepFailed,
		})
	default:
		return nil, fmt.Errorf("QUARRY_EXECUTOR: unknown executor %q", cfg.executor)
	}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version.String("runner"))
		return
	}
	// One JSON line per event; every line names the binary. The agent adds
	// runner_id once registered and run_id/job_id/attempt inside an attempt.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("component", "runner")
	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// run owns the agent's goroutines through agent.Run, which returns only
// after they have all ended, the optional metrics listener below, and the
// signal watcher inside signal.NotifyContext.
func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	exec, err := newExecutor(cfg)
	if err != nil {
		return err
	}
	cfg.agent.Logger = logger
	cfg.agent.Metrics = metrics.NewRunner()
	a, err := agent.New(cfg.agent, exec)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// A previous process of this runner may have died mid-attempt; its
	// containers and volumes carry our name and go before we claim.
	if d, ok := exec.(*docker.Executor); ok {
		// Reap writes plain lines; route them through the same JSON handler.
		if err := d.Reap(ctx, slog.NewLogLogger(logger.Handler(), slog.LevelInfo)); err != nil {
			return err
		}
	}
	if cfg.metrics != "" {
		_, stopMetrics, err := serveMetrics(cfg.metrics, cfg.agent.Metrics, logger)
		if err != nil {
			return err
		}
		defer stopMetrics()
	}
	logger.Info("polling", "version", version.Version, "name", cfg.agent.Name, "server", cfg.agent.ServerURL, "executor", cfg.executor, "capacity", cfg.agent.Capacity)
	if err := a.Run(ctx); err != nil {
		return err
	}
	logger.Info("stopped")
	return nil
}

// serveMetrics exposes m at addr/metrics and returns the bound address.
// The listener is bound before returning so a bad address is a startup
// error; the serving goroutine is owned by the caller through the returned
// stop, which closes it.
func serveMetrics(addr string, m *metrics.Runner, logger *slog.Logger) (bound string, stop func(), err error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, fmt.Errorf("QUARRY_METRICS_LISTEN: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics listener failed", "err", err)
		}
	}()
	logger.Info("metrics listening", "addr", ln.Addr().String())
	return ln.Addr().String(), func() { _ = srv.Close(); <-done }, nil
}
