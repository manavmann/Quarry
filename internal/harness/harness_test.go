package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"testing"
	"time"

	"quarry/internal/artifact"
	"quarry/internal/executor"
	"quarry/internal/store"
)

// diamond is build → {test, lint} → deploy.
const diamond = `name: diamond
jobs:
  - name: build
    image: alpine
    steps: ["echo build"]
  - name: test
    image: alpine
    steps: ["echo test"]
    needs: [build]
  - name: lint
    image: alpine
    steps: ["echo lint"]
    needs: [build]
  - name: deploy
    image: alpine
    steps: ["echo deploy"]
    needs: [test, lint]
`

// countByName tallies executions per job name across all agents.
func countByName(h *Harness) map[string]int {
	out := map[string]int{}
	for _, x := range h.Executions() {
		out[x.Spec.Job.Name]++
	}
	return out
}

func TestDiamondAcrossThreeAgentsSucceeds(t *testing.T) {
	h := New(t, Opts{Agents: 3, Capacity: 1})
	// test and lint each block until released, so they must be running
	// on two different agents at once (capacity 1 each).
	h.Script("test", executor.Outcome{Hang: true, LogBytes: 512})
	h.Script("lint", executor.Outcome{Hang: true})

	run := h.Submit(diamond)
	if run.Run.State != store.RunPending {
		t.Fatalf("new run state = %s", run.Run.State)
	}
	h.WaitJob(run.Run.ID, "test", store.JobRunning)
	h.WaitJob(run.Run.ID, "lint", store.JobRunning)
	d, err := h.Client().GetRun(run.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if a, b := d.Job("test").RunnerID, d.Job("lint").RunnerID; a == b || a == "" {
		t.Fatalf("test and lint should run on different agents, got %q and %q", a, b)
	}
	if got := d.Job("deploy").State; got != store.JobPending {
		t.Fatalf("deploy state while needs run = %s", got)
	}
	h.Release("test")
	h.Release("lint")

	done := h.WaitRun(run.Run.ID)
	if done.Run.State != store.RunSucceeded {
		t.Fatalf("run state = %s; jobs %+v", done.Run.State, done.Jobs)
	}
	for _, j := range done.Jobs {
		if j.State != store.JobSucceeded || j.Attempt != 1 {
			t.Errorf("job %s: state %s attempt %d", j.Name, j.State, j.Attempt)
		}
	}
	if got := countByName(h); len(got) != 4 || got["build"] != 1 || got["test"] != 1 || got["lint"] != 1 || got["deploy"] != 1 {
		t.Fatalf("executions per job = %v, want exactly one each", got)
	}
	// Every agent registered with the server.
	types := h.EventTypes(run.Run.ID)
	if types[0] != "run.created" || types[len(types)-1] != "run.finished" {
		t.Fatalf("event types = %v", types)
	}
	if n := count(types, "job.succeeded"); n != 4 {
		t.Fatalf("job.succeeded events = %d in %v", n, types)
	}
}

func TestEachJobExecutedOnceUnderContention(t *testing.T) {
	// Many agents, each polling fast, racing for a small DAG: the guarded
	// claim must hand every job to exactly one runner.
	h := New(t, Opts{Agents: 5, Capacity: 2, PollInterval: time.Millisecond})
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, h.Submit(diamond).Run.ID)
	}
	for _, id := range ids {
		if d := h.WaitRun(id); d.Run.State != store.RunSucceeded {
			t.Fatalf("run %s: %s", id, d.Run.State)
		}
	}
	perJob := map[string]int{}
	for _, x := range h.Executions() {
		perJob[x.Spec.JobID]++
	}
	if len(perJob) != 20 {
		t.Fatalf("%d distinct jobs executed, want 20", len(perJob))
	}
	for id, n := range perJob {
		if n != 1 {
			t.Errorf("job %s executed %d times", id, n)
		}
	}
}

func TestFailingJobSkipsDependents(t *testing.T) {
	h := New(t, Opts{Agents: 3})
	h.Script("test", executor.Outcome{ExitCode: 7})

	run := h.Submit(diamond)
	done := h.WaitRun(run.Run.ID)
	if done.Run.State != store.RunFailed {
		t.Fatalf("run state = %s", done.Run.State)
	}
	want := map[string]string{
		"build": store.JobSucceeded, "test": store.JobFailed, "lint": store.JobSucceeded, "deploy": store.JobSkipped,
	}
	for name, state := range want {
		if j := done.Job(name); j == nil || j.State != state {
			t.Errorf("job %s = %+v, want %s", name, j, state)
		}
	}
	if j := done.Job("test"); j.FailureKind != "exit_code" || j.ExitCode == nil || *j.ExitCode != 7 {
		t.Errorf("test failure = %+v", j)
	}
	if got := countByName(h); got["deploy"] != 0 || got["test"] != 1 {
		t.Fatalf("executions = %v; deploy must never run", got)
	}
	types := h.EventTypes(run.Run.ID)
	if !slices.Contains(types, "job.skipped") || !slices.Contains(types, "job.failed") {
		t.Fatalf("event types = %v", types)
	}
}

func TestInfraFailureIsReported(t *testing.T) {
	h := New(t, Opts{})
	h.Script("build", executor.Outcome{Err: executor.ErrInfra})

	run := h.Submit(diamond)
	done := h.WaitRun(run.Run.ID)
	if done.Run.State != store.RunFailed {
		t.Fatalf("run state = %s", done.Run.State)
	}
	// An infra failure is retried up to max_attempts (default 3), then
	// terminal; every attempt ran the executor.
	if j := done.Job("build"); j.State != store.JobFailed || j.FailureKind != "infra" || j.Attempt != 3 {
		t.Fatalf("build = %+v", j)
	}
	if got := countByName(h); got["build"] != 3 {
		t.Fatalf("build executions = %d, want 3", got["build"])
	}
	if types := h.EventTypes(run.Run.ID); count(types, "job.requeued") != 2 || count(types, "job.failed") != 1 {
		t.Fatalf("event types = %v", types)
	}
	for _, name := range []string{"test", "lint", "deploy"} {
		if j := done.Job(name); j.State != store.JobSkipped {
			t.Errorf("%s = %s, want skipped", name, j.State)
		}
	}
}

// exit_code failures are the job's fault and are never retried, whatever
// max_attempts says.
func TestExitCodeFailureIsNeverRetried(t *testing.T) {
	h := New(t, Opts{MaxAttempts: 3})
	h.Script("build", executor.Outcome{ExitCode: 2})

	run := h.Submit(diamond)
	done := h.WaitRun(run.Run.ID)
	if done.Run.State != store.RunFailed {
		t.Fatalf("run state = %s", done.Run.State)
	}
	j := done.Job("build")
	if j.State != store.JobFailed || j.FailureKind != "exit_code" || j.Attempt != 1 || j.ExitCode == nil || *j.ExitCode != 2 {
		t.Fatalf("build = %+v", j)
	}
	if got := countByName(h); got["build"] != 1 {
		t.Fatalf("build executions = %d, want 1", got["build"])
	}
	if types := h.EventTypes(run.Run.ID); count(types, "job.requeued") != 0 {
		t.Fatalf("event types = %v", types)
	}
}

func TestLabelsRouteJobs(t *testing.T) {
	h := New(t, Opts{Agents: 2, Labels: func(i int) map[string]string {
		if i == 1 {
			return map[string]string{"gpu": "true"}
		}
		return nil
	}})
	run := h.Submit(`name: labelled
jobs:
  - name: train
    image: alpine
    steps: ["echo train"]
    labels: {gpu: "true"}
`)
	done := h.WaitRun(run.Run.ID)
	if done.Run.State != store.RunSucceeded {
		t.Fatalf("run state = %s", done.Run.State)
	}
	if j := done.Job("train"); j.RunnerID != h.AgentID(1) {
		t.Fatalf("train ran on %q, want %q", j.RunnerID, h.AgentID(1))
	}
	if x := h.Executions(); len(x) != 1 || x[0].Agent != 1 {
		t.Fatalf("executions = %+v", x)
	}
}

func TestKillAndRestartAgent(t *testing.T) {
	h := New(t, Opts{Agents: 1, Capacity: 1})
	h.Script("build", executor.Outcome{Hang: true})

	run := h.Submit(diamond)
	h.WaitJob(run.Run.ID, "build", store.JobRunning)
	h.Kill(0)
	// Shutdown kills the attempt and reports it as an infra failure, which
	// requeues the job (attempt 1 kept; the next claim makes it 2).
	if j := h.WaitJob(run.Run.ID, "build", store.JobQueued); j.Attempt != 1 || j.RunnerID != "" {
		t.Fatalf("build after kill = %+v", j)
	}
	id := h.AgentID(0)
	h.Restart(0)
	h.Script("build", executor.Outcome{})
	d := h.WaitRun(run.Run.ID)
	if d.Run.State != store.RunSucceeded {
		t.Fatalf("run after restart = %s", d.Run.State)
	}
	// Re-registering under the same name is idempotent: same runner_id.
	if got := h.AgentID(0); got != id || d.Job("build").RunnerID != id || d.Job("build").Attempt != 2 {
		t.Fatalf("runner id after restart = %q (build = %+v), want %q", got, d.Job("build"), id)
	}
}

func count(ss []string, s string) int {
	n := 0
	for _, x := range ss {
		if x == s {
			n++
		}
	}
	return n
}

func TestLogsStreamFromFakeExecutor(t *testing.T) {
	h := New(t, Opts{Agents: 2})
	// 300 KB of output crosses several 64 KB chunks; the empty job ships
	// nothing. Both go through the real shipper → API → store path.
	h.Script("build", executor.Outcome{LogBytes: 300_000})
	run := h.Submit(diamond)
	d := h.WaitRun(run.Run.ID)
	if d.Run.State != store.RunSucceeded {
		t.Fatalf("run = %+v", d.Run)
	}

	text := h.LogText(d.Job("build").ID)
	if len(text) != 300_000 {
		t.Fatalf("build log is %d bytes, want 300000", len(text))
	}
	const line = "fake executor output line\n"
	for i := 0; i+len(line) <= len(text); i += len(line) {
		if text[i:i+len(line)] != line {
			t.Fatalf("log corrupted at byte %d: %q", i, text[i:i+len(line)])
		}
	}
	if got := h.LogText(d.Job("deploy").ID); got != "" {
		t.Fatalf("deploy log = %q, want empty", got)
	}
	// The cursor round-trips: reading after the last seq is empty and
	// keeps the cursor.
	l, err := h.Client().Logs(d.Job("build").ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := h.Client().Logs(d.Job("build").ID, l.Next)
	if err != nil || len(tail.Chunks) != 0 || tail.Next != l.Next {
		t.Fatalf("tail = %+v, %v", tail, err)
	}
	if slices.Contains(h.EventTypes(run.Run.ID), "job.logs_truncated") {
		t.Fatal("unexpected truncation event")
	}
}

// artifactsPipeline is one job that declares artifacts; the fake executor
// is scripted to leave files behind for them.
const artifactsPipeline = `name: artifacts
jobs:
  - name: build
    image: alpine
    steps: ["make"]
    artifacts: ["dist", "report.txt"]
`

func sha(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// A succeeding job's artifacts travel runner → server → store and come
// back listed with their sha256 and downloadable byte-for-byte.
func TestArtifactsAreUploadedListedAndDownloadable(t *testing.T) {
	h := New(t, Opts{})
	files := map[string]string{"dist/app.bin": "binary bytes", "dist/sub/notes.md": "# notes", "report.txt": "ok\n"}
	h.Script("build", executor.Outcome{Artifacts: files})

	run := h.Submit(artifactsPipeline)
	done := h.WaitRun(run.Run.ID)
	build := done.Job("build")
	if done.Run.State != store.RunSucceeded || build.State != store.JobSucceeded {
		t.Fatalf("run = %s build = %+v", done.Run.State, build)
	}
	arts, err := h.Client().Artifacts(build.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(arts) != len(files) {
		t.Fatalf("artifacts = %+v", arts)
	}
	for _, a := range arts {
		want, ok := files[a.Path]
		if !ok || a.SizeBytes != int64(len(want)) || a.SHA256 != sha(want) {
			t.Errorf("artifact %+v does not match %q", a, want)
		}
		got, err := h.Client().Download(build.ID, a.Path)
		if err != nil || string(got) != want {
			t.Errorf("download %s = %q, %v; want %q", a.Path, got, err, want)
		}
	}
	// The store holds exactly these keys, under the attempt's prefix.
	keys, err := h.ArtifactStore().List(t.Context(), artifact.JobPrefix(run.Run.ID, build.ID, 1))
	if err != nil || len(keys) != len(files) {
		t.Fatalf("store keys = %v, %v", keys, err)
	}
	if text := h.LogText(build.ID); !strings.Contains(text, "[quarry] uploaded artifact dist/app.bin (12 bytes)\n") {
		t.Errorf("log lacks upload line:\n%s", text)
	}
}

// When the server cannot write to the artifact store the upload is a 500,
// the runner retries and then reports the attempt as an infra failure:
// the job never succeeds with missing artifacts. Being infra, it is
// retried until max_attempts and only then terminal.
func TestArtifactStoreFailureIsInfra(t *testing.T) {
	h := New(t, Opts{MaxAttempts: 2})
	h.Script("build", executor.Outcome{Artifacts: map[string]string{"report.txt": "x"}})
	h.FailArtifactPuts(true)

	run := h.Submit(artifactsPipeline)
	done := h.WaitRun(run.Run.ID)
	build := done.Job("build")
	if build.State != store.JobFailed || build.FailureKind != "infra" || build.Attempt != 2 || !strings.Contains(build.Error, "upload artifacts") {
		t.Fatalf("build = %+v, want failed(infra) from the upload after 2 attempts", build)
	}
	arts, err := h.Client().Artifacts(build.ID)
	if err != nil || len(arts) != 0 {
		t.Fatalf("artifacts = %+v, %v; want none", arts, err)
	}
	if keys, _ := h.ArtifactStore().List(t.Context(), "runs/"); len(keys) != 0 {
		t.Fatalf("store keys = %v, want none", keys)
	}
}
