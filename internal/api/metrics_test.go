package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// scrapeMetrics reads GET /metrics without a token.
func scrapeMetrics(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	return string(raw)
}

func wantSeries(t *testing.T, out string, series ...string) {
	t.Helper()
	for _, s := range series {
		if !strings.Contains(out, s) {
			t.Errorf("missing %q in /metrics:\n%s", s, out)
		}
	}
}

// /metrics shows the queue depth change as a run is submitted, claimed
// and completed, with the counters that go with it.
func TestMetricsQueueDepthChangesDuringRun(t *testing.T) {
	srv, _ := newTestServer(t)
	out := scrapeMetrics(t, srv)
	if strings.Contains(out, "quarry_queue_depth{") {
		t.Fatalf("queue depth before any run:\n%s", out)
	}

	// validYAML: build and lint are roots (queued), test and deploy pending.
	var created runDetailJSON
	do(t, srv, "POST", "/api/runs", validYAML, &created)
	wantSeries(t, scrapeMetrics(t, srv), `quarry_queue_depth{labels=""} 2`, `quarry_jobs_total{state="queued"} 2`)

	const claimBody = `{"runner_id":"r1","name":"one","labels":{"os":"linux"},"capacity":2}`
	var claimed claimedJSON
	if resp := do(t, srv, "POST", "/api/runner/claim", claimBody, &claimed); resp.StatusCode != http.StatusOK {
		t.Fatalf("claim: %d", resp.StatusCode)
	}
	if claimed.Job.Name != "build" {
		t.Fatalf("claimed %q, want build", claimed.Job.Name)
	}
	wantSeries(t, scrapeMetrics(t, srv),
		`quarry_queue_depth{labels=""} 1`,
		`quarry_jobs_total{state="running"} 1`,
		`quarry_claim_latency_seconds_count 1`,
		`quarry_runners{state="online"} 1`)

	if resp := do(t, srv, "POST", "/api/runner/jobs/"+claimed.Job.ID+"/complete",
		`{"runner_id":"r1","attempt":1,"status":"succeeded","exit_code":0}`, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("complete: %d", resp.StatusCode)
	}
	// build (queued first, so claimed first) succeeded → test joins lint in
	// the queue: depth is back to 2.
	wantSeries(t, scrapeMetrics(t, srv),
		`quarry_jobs_total{state="succeeded"} 1`,
		`quarry_job_duration_seconds_count 1`,
		`quarry_jobs_total{state="queued"} 3`,
		`quarry_queue_depth{labels=""} 2`)
}

// /metrics is served without the API token and never under /api/.
func TestMetricsNeedsNoToken(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, err := srv.Client().Get(srv.URL + "/api/metrics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /api/metrics = %d, want 401", resp.StatusCode)
	}
	wantSeries(t, scrapeMetrics(t, srv), "quarry_lease_expirations_total 0", "quarry_log_bytes_total 0")
}
