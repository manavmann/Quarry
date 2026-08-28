package cli

import (
	"bytes"
	"context"
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
