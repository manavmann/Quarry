package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func spec(needs ...string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"Name": "x", "Image": "alpine", "Needs": needs})
	return b
}

// now is a fixed clock for the snapshots: 2026-01-02T03:04:05Z.
const now int64 = 1767323045000

func TestRenderJobsSnapshot(t *testing.T) {
	two := 2
	// Declared out of DAG order on purpose: deploy first, then its needs.
	jobs := []Job{
		{Name: "deploy", State: "pending", Spec: spec("test", "lint")},
		{Name: "test", State: "running", Attempt: 2, MaxAttempts: 3, RunnerID: "r-2", StartedAt: now - 83_500, Spec: spec("build")},
		{Name: "lint", State: "failed", Attempt: 1, MaxAttempts: 1, RunnerID: "r-1", ExitCode: &two, StartedAt: now - 10_000, FinishedAt: now - 8_750, Spec: spec()},
		{Name: "build", State: "succeeded", Attempt: 1, MaxAttempts: 1, RunnerID: "r-1", StartedAt: now - 100_000, FinishedAt: now - 90_000, Spec: spec()},
		{Name: "docs", State: "failed", Attempt: 1, MaxAttempts: 1, RunnerID: "r-3", FailureKind: "timeout", StartedAt: now - 60_000, FinishedAt: now, Spec: spec()},
	}
	var buf bytes.Buffer
	renderJobs(&buf, jobs, now)
	want := strings.Join([]string{
		"JOB     STATE             ATTEMPT  RUNNER  DURATION",
		"lint    failed (exit 2)   1        r-1     1.3s",
		"build   succeeded         1        r-1     10s",
		"test    running           2/3      r-2     1m24s",
		"deploy  pending           -        -       -",
		"docs    failed (timeout)  1        r-3     1m0s",
		"",
	}, "\n")
	if buf.String() != want {
		t.Errorf("table mismatch\n got:\n%s\nwant:\n%s", buf.String(), want)
	}
}

func TestDagOrderTieBreaksByDeclaration(t *testing.T) {
	jobs := []Job{
		{Name: "c", Spec: spec("a")},
		{Name: "b", Spec: spec()},
		{Name: "a", Spec: spec()},
		{Name: "d", Spec: spec("b", "c")},
	}
	var names []string
	for _, j := range dagOrder(jobs) {
		names = append(names, j.Name)
	}
	if got := strings.Join(names, " "); got != "b a c d" {
		t.Errorf("order = %q, want %q", got, "b a c d")
	}
}

func TestRenderRunsAndRunnersSnapshot(t *testing.T) {
	runs := []Run{
		{ID: "run1", State: "succeeded", Trigger: "api", CreatedAt: now - 200_000, StartedAt: now - 190_000, FinishedAt: now - 100_000},
		{ID: "run2", State: "pending", Trigger: "api", CreatedAt: now},
	}
	var buf bytes.Buffer
	renderRuns(&buf, runs, now)
	want := strings.Join([]string{
		"RUN   STATE      TRIGGER  CREATED               DURATION",
		"run1  succeeded  api      2026-01-02T03:00:45Z  1m30s",
		"run2  pending    api      2026-01-02T03:04:05Z  -",
		"",
	}, "\n")
	if buf.String() != want {
		t.Errorf("runs mismatch\n got:\n%s\nwant:\n%s", buf.String(), want)
	}

	runners := []Runner{
		{ID: "r-1", Name: "alpha", State: "online", Capacity: 2, Labels: map[string]string{"os": "linux", "arch": "amd64"}, Version: "v0.9", LastSeenAt: now - 4_000},
		{ID: "r-2", Name: "beta", State: "online", Capacity: 1},
	}
	buf.Reset()
	renderRunners(&buf, runners, now)
	want = strings.Join([]string{
		"RUNNER  ID   STATE   CAPACITY  LABELS               LAST SEEN  VERSION",
		"alpha   r-1  online  2         arch=amd64,os=linux  4s ago     v0.9",
		"beta    r-2  online  1         -                    -          -",
		"",
	}, "\n")
	if buf.String() != want {
		t.Errorf("runners mismatch\n got:\n%s\nwant:\n%s", buf.String(), want)
	}
}

func TestRenderEventsSnapshot(t *testing.T) {
	events := []Event{
		{ID: 1, Type: "run.created", Detail: json.RawMessage(`{"jobs":2}`), CreatedAt: now - 1000},
		{ID: 2, Type: "job.started", JobID: "j1", Detail: json.RawMessage(`{}`), CreatedAt: now},
	}
	var buf bytes.Buffer
	renderEvents(&buf, events)
	want := strings.Join([]string{
		"2026-01-02T03:04:04Z  run.created  -   {\"jobs\":2}",
		"2026-01-02T03:04:05Z  job.started  j1  ",
		"",
	}, "\n")
	if buf.String() != want {
		t.Errorf("events mismatch\n got:\n%q\nwant:\n%q", buf.String(), want)
	}
}
