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
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"quarry/internal/agent"
	"quarry/internal/executor"
	"quarry/internal/executor/docker"
	"quarry/internal/version"
)

type config struct {
	agent      agent.Config
	executor   string
	keepFailed bool
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
	logger := log.New(os.Stderr, "runner: ", log.LstdFlags|log.Lmsgprefix)
	if err := run(logger); err != nil {
		logger.Fatal(err)
	}
}

// run owns the agent's goroutines through agent.Run, which returns only
// after they have all ended; the signal watcher lives inside
// signal.NotifyContext.
func run(logger *log.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	exec, err := newExecutor(cfg)
	if err != nil {
		return err
	}
	cfg.agent.Logger = logger
	a, err := agent.New(cfg.agent, exec)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Printf("%s %s polling %s (executor %s, capacity %d)",
		version.String("runner"), cfg.agent.Name, cfg.agent.ServerURL, cfg.executor, cfg.agent.Capacity)
	if err := a.Run(ctx); err != nil {
		return err
	}
	logger.Print("stopped")
	return nil
}
