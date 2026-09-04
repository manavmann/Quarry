package main

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"quarry/internal/metrics"
)

// QUARRY_METRICS_LISTEN serves the runner set at /metrics and the stop
// closes the listener; an unusable address is a startup error.
func TestServeMetrics(t *testing.T) {
	m := metrics.NewRunner()
	m.ActiveJobs.Set(1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	addr, stop, err := serveMetrics("127.0.0.1:0", m, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "quarry_runner_active_jobs 1") {
		t.Fatalf("metrics body:\n%s", raw)
	}
	if _, _, err := serveMetrics("256.0.0.1:1", m, logger); err == nil || !strings.Contains(err.Error(), "QUARRY_METRICS_LISTEN") {
		t.Fatalf("bad address err = %v", err)
	}
}
