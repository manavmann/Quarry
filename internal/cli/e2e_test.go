package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"quarry/internal/executor"
	"quarry/internal/harness"
	"quarry/internal/store"
)

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
