package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"quarry/internal/executor"
	"quarry/internal/harness"
	"quarry/internal/store"
)

type reconnectOutput struct {
	mu sync.Mutex
	bytes.Buffer
}

func (w *reconnectOutput) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Buffer.Write(b)
}
func (w *reconnectOutput) text() string { w.mu.Lock(); defer w.mu.Unlock(); return w.Buffer.String() }

func TestWatchReconnect(t *testing.T)            { testCLIReconnect(t, false) }
func TestLogsFollowReconnectCursor(t *testing.T) { testCLIReconnect(t, true) }

func testCLIReconnect(t *testing.T, logs bool) {
	h := harness.New(t, harness.Opts{})
	h.Script("build", executor.Outcome{Hang: true, LogBytes: 100})
	run := h.Submit(`name: reconnect
jobs:
  - name: build
    image: alpine
    steps: ["echo hello"]
`)
	build := h.WaitJob(run.Run.ID, "build", store.JobRunning)
	var out reconnectOutput
	var failures atomic.Int64
	var resumedAfter atomic.Int64
	resumedAfter.Store(-1)
	client := NewClient(h.URL(), harness.Token)
	client.Backoff = time.Millisecond
	client.Retries = 1
	client.HTTP = &http.Client{Transport: reconnectTransport(func(r *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			failures.Add(1)
		}
		if err == nil && failures.Load() > 0 && strings.HasSuffix(r.URL.Path, "/logs") {
			after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
			resumedAfter.CompareAndSwap(-1, after)
		}
		return resp, err
	})}
	a := &App{out: &out, err: io.Discard, client: client, interval: time.Millisecond, now: func() int64 { return 0 }}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		if logs {
			cmd := a.logsCmd()
			cmd.SetArgs([]string{build.ID, "-f"})
			done <- cmd.ExecuteContext(ctx)
		} else {
			done <- a.watch(ctx, run.Run.ID)
		}
	}()
	harness.WaitFor(t, func() bool {
		if logs {
			return len(out.text()) == 100
		}
		return strings.Contains(out.text(), "running")
	}, "CLI initial output")
	initial := out.text()
	h.StopServer()
	harness.WaitFor(t, func() bool { return failures.Load() >= 5 }, "connection retries beyond finite budget")
	select {
	case err := <-done:
		t.Fatalf("CLI exited during outage: %v", err)
	default:
	}
	h.StartServer()
	if logs {
		// Existing first chunk was printed before downtime. Append a fenced
		// tail, then finish; reconnect must print only this new chunk.
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.URL()+"/api/runner/jobs/"+build.ID+"/logs",
			strings.NewReader(fmt.Sprintf(`{"runner_id":%q,"attempt":1,"chunks":[{"seq":2,"data":"dGFpbAo="}]}`, build.RunnerID)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+harness.Token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 204 {
			t.Fatalf("append tail: %d", resp.StatusCode)
		}
	}
	h.Release("build")
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if logs {
		if got := out.text(); got != initial+"tail\n" {
			t.Fatalf("resumed output=%q", got)
		}
		if got := resumedAfter.Load(); got != 1 {
			t.Fatalf("reconnect cursor=%d want 1", got)
		}
	} else if !strings.Contains(out.text(), "succeeded") {
		t.Fatalf("watch never resumed: %s", out.text())
	}
	if got := h.WaitRun(run.Run.ID).Job("build"); got.Attempt != 1 {
		t.Fatalf("job re-executed: %+v", got)
	}
}

// End to end: a job's artifacts travel runner → server → store and
// `quarry artifacts <job> --download` brings them back byte-for-byte,
// verified against the listed sha256. The harness is the real server and
// a real agent; only the executor is fake.
func TestArtifactsDownloadEndToEnd(t *testing.T) {
	h := harness.New(t, harness.Opts{})
	files := map[string]string{"dist/app.bin": strings.Repeat("binary", 10_000), "dist/sub/notes.md": "# notes\n", "report.txt": "ok\n"}
	h.Script("build", executor.Outcome{Artifacts: files})
	run := h.Submit(`name: e2e
jobs:
  - name: build
    image: alpine
    steps: ["make"]
    artifacts: ["dist", "report.txt"]
`)
	build := h.WaitJob(run.Run.ID, "build", store.JobSucceeded)

	var out, errOut bytes.Buffer
	root := New(&out, &errOut)
	root.SetArgs([]string{"--server", h.URL(), "--token", harness.Token, "artifacts", build.ID})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("artifacts: %v", err)
	}
	for p := range files {
		if !strings.Contains(out.String(), p) {
			t.Errorf("listing lacks %s:\n%s", p, out.String())
		}
	}

	dir := filepath.Join(t.TempDir(), "dl")
	out.Reset()
	root = New(&out, &errOut)
	root.SetArgs([]string{"--server", h.URL(), "--token", harness.Token, "artifacts", build.ID, "--download", dir})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("artifacts --download: %v", err)
	}
	for p, want := range files {
		got, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(p)))
		if err != nil || string(got) != want {
			t.Errorf("%s: err=%v len=%d, want len=%d", p, err, len(got), len(want))
		}
	}
}

// End to end: `quarry cancel <run>` against a real server and agent stops
// a hanging job through the heartbeat directive, the run ends cancelled,
// `quarry watch` reports it with exit 1, and a second cancel is a 409.
func TestCancelEndToEnd(t *testing.T) {
	h := harness.New(t, harness.Opts{})
	h.Script("build", executor.Outcome{Hang: true})
	run := h.Submit(`name: e2e-cancel
jobs:
  - name: build
    image: alpine
    steps: ["make"]
  - name: test
    image: alpine
    steps: ["make test"]
    needs: [build]
`)
	h.WaitJob(run.Run.ID, "build", store.JobRunning)

	var out, errOut bytes.Buffer
	root := New(&out, &errOut)
	root.SetArgs([]string{"--server", h.URL(), "--token", harness.Token, "cancel", run.Run.ID})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if !strings.Contains(out.String(), "cancel requested") {
		t.Fatalf("cancel output = %q", out.String())
	}

	out.Reset()
	root = New(&out, &errOut)
	root.SetArgs([]string{"--server", h.URL(), "--token", harness.Token, "--interval", "10ms", "watch", run.Run.ID})
	err := root.ExecuteContext(context.Background())
	var exit *ExitError
	if !errors.As(err, &exit) || exit.Code != 1 {
		t.Fatalf("watch err = %v, want ExitError{1}", err)
	}
	if !strings.Contains(out.String(), "run "+run.Run.ID+": cancelled") || !strings.Contains(out.String(), "cancelled") {
		t.Fatalf("watch output:\n%s", out.String())
	}
	final := h.WaitRun(run.Run.ID)
	if final.Job("build").State != store.JobCancelled || final.Job("test").State != store.JobCancelled {
		t.Fatalf("jobs = %+v", final.Jobs)
	}

	root = New(&out, &errOut)
	root.SetArgs([]string{"--server", h.URL(), "--token", harness.Token, "cancel", run.Run.ID})
	var apiErr *APIError
	if err := root.ExecuteContext(context.Background()); !errors.As(err, &apiErr) || apiErr.Status != 409 {
		t.Fatalf("second cancel err = %v, want 409", err)
	}
}
