//go:build docker

package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/volume"

	"quarry/internal/executor"
	"quarry/internal/pipeline"
)

const testImage = "alpine:3.20"

// newExec builds an executor with a unique runner name so the cleanup
// check can find exactly what this test created. The daemon must be
// reachable: with the docker tag set, an absent daemon is a failure.
func newExec(t *testing.T, cfg Config) *Executor {
	t.Helper()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	cfg.RunnerName = "quarry-test-" + hex.EncodeToString(b)
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.cli.Ping(context.Background()); err != nil {
		t.Fatalf("docker daemon unreachable: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// syncBuffer is the log writer: the demux goroutine writes while the
// test reads after Run returns, and -race wants that to be explicit.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func spec(jobID string, steps ...string) executor.JobSpec {
	return executor.JobSpec{
		JobID: jobID, RunID: "run1", Attempt: 1,
		Job: pipeline.Job{Name: jobID, Image: testImage, Steps: steps},
	}
}

func run(t *testing.T, ctx context.Context, e *Executor, s executor.JobSpec) (executor.Result, string, error) {
	t.Helper()
	var logs syncBuffer
	res, err := e.Run(ctx, s, &logs)
	t.Logf("logs:\n%s", logs.String())
	return res, logs.String(), err
}

// assertClean fails when any container or volume labelled with the
// executor's runner name still exists.
func assertClean(t *testing.T, e *Executor) {
	t.Helper()
	ctx := context.Background()
	f := filters.NewArgs(filters.Arg("label", LabelRunner+"="+e.cfg.RunnerName))
	cs, err := e.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		t.Fatal(err)
	}
	vs, err := e.cli.VolumeList(ctx, volume.ListOptions{Filters: f})
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 0 || len(vs.Volumes) != 0 {
		t.Fatalf("leftovers: %d containers, %d volumes", len(cs), len(vs.Volumes))
	}
}

func sourceTar(files map[string]string) SourceFunc {
	return func(context.Context, string) (io.ReadCloser, error) {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		for name, body := range files {
			_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), ModTime: time.Now()})
			_, _ = tw.Write([]byte(body))
		}
		_ = tw.Close()
		return io.NopCloser(&buf), nil
	}
}

func mustContain(t *testing.T, logs string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(logs, w) {
			t.Errorf("logs lack %q", w)
		}
	}
}

func TestDockerEchoJob(t *testing.T) {
	e := newExec(t, Config{})
	defer assertClean(t, e)
	s := spec("job1", "echo hello", `echo "$QUARRY_JOB/$QUARRY_RUN/$QUARRY_ATTEMPT/$CI/$GREETING"`, "pwd")
	s.Job.Env = map[string]string{"GREETING": "hi", "CI": "false"} // CI is reserved

	res, logs, err := run(t, context.Background(), e, s)
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("got %+v, %v; want exit 0", res, err)
	}
	if res.Duration <= 0 {
		t.Errorf("duration = %v", res.Duration)
	}
	// The first line proves attach-before-start caught the first output.
	if !strings.HasPrefix(logs, "+ echo hello\nhello\n") {
		t.Errorf("logs do not start with the first step echo")
	}
	mustContain(t, logs, "job1/run1/1/true/hi\n", "+ pwd\n/workspace\n")

	// stderr reaches the same writer. Its order relative to stdout is not
	// fixed (two pipes), so only presence is checked.
	res, logs, err = run(t, context.Background(), e, spec("job1b", "echo to-stderr >&2", "exit 0"))
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("got %+v, %v; want exit 0", res, err)
	}
	mustContain(t, logs, "to-stderr\n", "+ exit 0\n")
}

func TestDockerNonZeroExit(t *testing.T) {
	e := newExec(t, Config{})
	defer assertClean(t, e)

	res, _, err := run(t, context.Background(), e, spec("job2", "exit 3"))
	if err != nil || res.ExitCode != 3 {
		t.Fatalf("got %+v, %v; want exit 3, nil", res, err)
	}

	// set -e: a failing step stops the script.
	res, logs, err := run(t, context.Background(), e, spec("job3", "false", "echo unreachable"))
	if err != nil || res.ExitCode != 1 {
		t.Fatalf("got %+v, %v; want exit 1, nil", res, err)
	}
	if strings.Contains(logs, "unreachable") {
		t.Error("script continued past a failing step")
	}
}

func TestDockerTimeoutKill(t *testing.T) {
	e := newExec(t, Config{})
	defer assertClean(t, e)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	res, logs, err := run(t, ctx, e, spec("job4", "echo started", "sleep 60", "echo unreachable"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v (res %+v), want DeadlineExceeded", err, res)
	}
	if took := time.Since(start); took > 20*time.Second {
		t.Fatalf("Run took %v after cancel; kill did not happen", took)
	}
	mustContain(t, logs, "started\n")
	if strings.Contains(logs, "unreachable") {
		t.Error("container ran on after kill")
	}
}

// cancelOn cancels once the container's output contains marker: the way
// a heartbeat cancel directive or a user cancel reaches a running job.
type cancelOn struct {
	syncBuffer
	marker string
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelOn) Write(p []byte) (int, error) {
	n, err := c.syncBuffer.Write(p)
	if strings.Contains(c.String(), c.marker) {
		c.once.Do(c.cancel)
	}
	return n, err
}

// Cancellation mid-run kills the container exactly like a timeout does:
// Run returns context.Canceled promptly, the script does not continue,
// and nothing is left behind.
func TestDockerCancelKill(t *testing.T) {
	e := newExec(t, Config{})
	defer assertClean(t, e)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := &cancelOn{marker: "started\n", cancel: cancel}
	start := time.Now()
	res, err := e.Run(ctx, spec("job5", "echo started", "sleep 60", "echo unreachable"), logs)
	t.Logf("logs:\n%s", logs.String())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v (res %+v), want Canceled", err, res)
	}
	if took := time.Since(start); took > 20*time.Second {
		t.Fatalf("Run took %v after cancel; kill did not happen", took)
	}
	mustContain(t, logs.String(), "started\n")
	if strings.Contains(logs.String(), "unreachable") {
		t.Error("container ran on after kill")
	}
}

func TestDockerSourceVisibleInWorkspace(t *testing.T) {
	e := newExec(t, Config{Source: sourceTar(map[string]string{
		"hello.txt":      "hello from the bundle\n",
		"sub/nested.txt": "nested file\n",
	})})
	defer assertClean(t, e)

	res, logs, err := run(t, context.Background(), e, spec("job5", "cat hello.txt", "cat /workspace/sub/nested.txt", "test ! -e /workspace/quarry"))
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("got %+v, %v; want exit 0", res, err)
	}
	mustContain(t, logs, "hello from the bundle\n", "nested file\n")
}

func TestDockerStepContainingQuotes(t *testing.T) {
	e := newExec(t, Config{})
	defer assertClean(t, e)

	step := `echo "it's a \"quoted\" step" && echo 'single '"'"'nested'`
	res, logs, err := run(t, context.Background(), e, spec("job6", step))
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("got %+v, %v; want exit 0", res, err)
	}
	mustContain(t, logs, "+ "+step+"\n", "it's a \"quoted\" step\n", "single 'nested\n")
}

func TestDockerCleanupLeavesNoContainerOrVolume(t *testing.T) {
	e := newExec(t, Config{})

	// Success, failure and kill all tear down.
	if _, _, err := run(t, context.Background(), e, spec("job7", "true")); err != nil {
		t.Fatal(err)
	}
	assertClean(t, e)
	if _, _, err := run(t, context.Background(), e, spec("job8", "exit 2")); err != nil {
		t.Fatal(err)
	}
	assertClean(t, e)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := run(t, ctx, e, spec("job9", "sleep 60")); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want Canceled", err)
	}
	assertClean(t, e)

	// An infra failure before start (unknown image) leaves nothing either.
	s := spec("job10", "true")
	s.Job.Image = "quarry-no-such-image-" + e.cfg.RunnerName + ":latest"
	if _, _, err := run(t, context.Background(), e, s); err == nil {
		t.Fatal("expected a pull error for a nonexistent image")
	}
	assertClean(t, e)

	// KeepFailed keeps a failed attempt's remains, and only a failed one.
	k := newExec(t, Config{KeepFailed: true})
	if _, _, err := run(t, context.Background(), k, spec("job11", "true")); err != nil {
		t.Fatal(err)
	}
	assertClean(t, k)
	if _, _, err := run(t, context.Background(), k, spec("job12", "exit 1")); err != nil {
		t.Fatal(err)
	}
	bg := context.Background()
	if err := k.cli.ContainerRemove(bg, "quarry-job12-1", container.RemoveOptions{Force: true}); err != nil {
		t.Errorf("failed container was not kept: %v", err)
	}
	if err := k.cli.VolumeRemove(bg, "quarry-job12-1", true); err != nil {
		t.Errorf("failed volume was not kept: %v", err)
	}
	assertClean(t, k)
}

// A directory artifact and a file artifact both land under ArtifactDir
// without a doubled path segment (CopyFromContainer prefixes entries with
// the requested basename); a missing declared path is skipped, not fatal.
func TestDockerArtifactsExtractWithoutDoubledSegment(t *testing.T) {
	e := newExec(t, Config{})
	defer assertClean(t, e)

	s := spec("job-art", "mkdir -p dist/sub", "echo -n binary > dist/app.bin", "echo -n x > dist/sub/x.txt", "echo -n top > out.txt")
	s.Job.Artifacts = []string{"dist", "out.txt", "missing"}
	s.ArtifactDir = t.TempDir()
	res, logs, err := run(t, context.Background(), e, s)
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("got %+v, %v; want exit 0", res, err)
	}
	mustContain(t, logs, "[quarry] artifact missing: not found in container, skipped\n")
	for p, want := range map[string]string{"dist/app.bin": "binary", "dist/sub/x.txt": "x", "out.txt": "top"} {
		b, err := os.ReadFile(filepath.Join(s.ArtifactDir, filepath.FromSlash(p)))
		if err != nil || string(b) != want {
			t.Errorf("%s = %q, %v; want %q", p, b, err, want)
		}
	}
	if _, err := os.Stat(filepath.Join(s.ArtifactDir, "dist", "dist")); err == nil {
		t.Fatal("doubled path segment dist/dist exists")
	}
}

// TestDockerReapOrphans leaves a running container and its volume under
// this runner's label, plus the same under another runner's name, and
// expects Reap to remove only the first pair.
func TestDockerReapOrphans(t *testing.T) {
	e := newExec(t, Config{})
	ctx := context.Background()
	other := e.cfg.RunnerName + "-other"

	orphan := func(runner, job string) {
		t.Helper()
		name := "quarry-" + job + "-1"
		labels := map[string]string{LabelRunner: runner, LabelJob: job, LabelRun: "run1", LabelAttempt: "1"}
		if _, err := e.cli.VolumeCreate(ctx, volume.CreateOptions{Name: name, Labels: labels}); err != nil {
			t.Fatal(err)
		}
		hc, _ := hostConfig(name, pipeline.Resources{})
		c, err := e.cli.ContainerCreate(ctx, &container.Config{
			Image: testImage, Cmd: []string{"sleep", "300"}, Labels: labels,
		}, hc, nil, nil, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.cli.ContainerStart(ctx, c.ID, container.StartOptions{}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = e.cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
			_ = e.cli.VolumeRemove(ctx, name, true)
		})
	}
	if err := e.ensureImage(ctx, testImage, io.Discard); err != nil {
		t.Fatal(err)
	}
	orphan(e.cfg.RunnerName, "mine")
	orphan(other, "theirs")

	var out syncBuffer
	if err := e.Reap(ctx, log.New(&out, "", 0)); err != nil {
		t.Fatal(err)
	}
	t.Logf("reap log:\n%s", out.String())
	assertClean(t, e)
	mustContain(t, out.String(), "removed orphan container quarry-mine-1 (run run1)", "removed orphan volume quarry-mine-1 (run run1)")

	f := filters.NewArgs(filters.Arg("label", LabelRunner+"="+other))
	cs, err := e.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		t.Fatal(err)
	}
	vs, err := e.cli.VolumeList(ctx, volume.ListOptions{Filters: f})
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 1 || len(vs.Volumes) != 1 {
		t.Fatalf("other runner's remains touched: %d containers, %d volumes", len(cs), len(vs.Volumes))
	}
}

// TestDockerReapKeepFailed: with KeepFailed the reaper leaves everything.
func TestDockerReapKeepFailed(t *testing.T) {
	e := newExec(t, Config{KeepFailed: true})
	ctx := context.Background()
	name := "quarry-" + e.cfg.RunnerName + "-keep"
	labels := map[string]string{LabelRunner: e.cfg.RunnerName, LabelJob: name, LabelRun: "run1", LabelAttempt: "1"}
	if _, err := e.cli.VolumeCreate(ctx, volume.CreateOptions{Name: name, Labels: labels}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.cli.VolumeRemove(ctx, name, true) })
	if err := e.Reap(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.cli.VolumeInspect(ctx, name); err != nil {
		t.Fatalf("volume reaped despite KeepFailed: %v", err)
	}
}
