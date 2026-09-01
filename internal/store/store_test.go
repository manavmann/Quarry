package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

// fakeClock is a manually advanced Clock so timestamps are deterministic.
type fakeClock struct{ ms int64 }

func (c *fakeClock) now() int64 { c.ms++; return c.ms }

func openTemp(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	clk := &fakeClock{ms: 1_700_000_000_000}
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "quarry.db"), WithClock(clk.now))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, clk
}

// newRun is a minimal valid run for tests that don't care about run fields.
func newRun(id string) *Run {
	return &Run{ID: id, SourceKey: "local", PipelineYAML: "name: p\njobs: []\n", Trigger: "cli"}
}

func TestMigrateFreshDB(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "quarry.db")

	s, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	v, err := s.SchemaVersion(ctx)
	if err != nil || v != 5 {
		t.Fatalf("SchemaVersion = %d, %v; want 5", v, err)
	}
	for _, tbl := range []string{"runs", "jobs", "job_deps", "runners", "events", "log_chunks", "artifacts"} {
		var n int
		if err := s.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, tbl).Scan(&n); err != nil || n != 1 {
			t.Errorf("table %s missing (n=%d, err=%v)", tbl, n, err)
		}
	}
	var mode string
	if err := s.Reader().QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Errorf("journal_mode = %q, %v; want wal", mode, err)
	}
	var fk int
	if err := s.Reader().QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&fk); err != nil || fk != 1 {
		t.Errorf("foreign_keys = %d, %v; want 1", fk, err)
	}
	s.Close()

	// Reopening is idempotent: no migration re-applies, version unchanged.
	s, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	var applied int
	if err := s.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&applied); err != nil || applied != 5 {
		t.Fatalf("schema_migrations rows = %d, %v; want 5", applied, err)
	}
}

func TestCreateRunWithDepsAndReadBack(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)

	const yamlSrc = "name: build-test\njobs:\n  - name: build\n    image: golang:1.27\n    steps: [go build ./...]\n"
	run := &Run{
		ID: "r1", SourceKey: "github.com/acme/app", PipelineYAML: yamlSrc,
		CommitSHA: "deadbeef", Ref: "refs/heads/main", Trigger: "push",
	}
	jobs := []Job{
		{ID: "j-build", Name: "build", SpecJSON: []byte(`{"image":"golang:1.27"}`)},
		{ID: "j-test", Name: "test", SpecJSON: []byte(`{"image":"golang:1.27"}`), MaxAttempts: 3},
		{ID: "j-lint", Name: "lint", SpecJSON: []byte(`{"image":"golangci"}`), State: JobFailed, FailureKind: FailureInfra},
	}
	deps := []JobDep{{JobID: "j-test", NeedsJobID: "j-build"}, {JobID: "j-lint", NeedsJobID: "j-build"}}
	err := s.Tx(ctx, func(tx *sql.Tx) error { return s.CreateRun(ctx, tx, run, jobs, deps) })
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if run.State != RunPending || run.CreatedAt == 0 {
		t.Errorf("run defaults not applied: %+v", run)
	}

	got, err := s.GetRun(ctx, s.Reader(), "r1")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if *got != *run {
		t.Errorf("GetRun = %+v, want %+v", *got, *run)
	}
	if got.PipelineYAML != yamlSrc {
		t.Errorf("pipeline_yaml not persisted verbatim:\n%q", got.PipelineYAML)
	}

	// Optional run columns round-trip as NULL -> "".
	if err := s.Tx(ctx, func(tx *sql.Tx) error { return s.CreateRun(ctx, tx, newRun("r0"), nil, nil) }); err != nil {
		t.Fatalf("CreateRun r0: %v", err)
	}
	if r0, err := s.GetRun(ctx, s.Reader(), "r0"); err != nil || r0.CommitSHA != "" || r0.Ref != "" {
		t.Errorf("GetRun r0 = %+v, %v; want empty CommitSHA/Ref", r0, err)
	}
	if err := s.Tx(ctx, func(tx *sql.Tx) error {
		return s.CreateRun(ctx, tx, &Run{ID: "bad", SourceKey: "x", Trigger: "cli"}, nil, nil)
	}); err == nil {
		t.Error("CreateRun without PipelineYAML succeeded")
	}

	gotJobs, err := s.ListJobs(ctx, s.Reader(), "r1")
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(gotJobs) != 3 {
		t.Fatalf("ListJobs len = %d, want 3", len(gotJobs))
	}
	for i, j := range gotJobs {
		want := jobs[i] // CreateRun filled in RunID, State, QueuedAt, MaxAttempts
		if j.ID != want.ID || j.Name != want.Name || j.RunID != "r1" || j.State != want.State ||
			j.Attempt != 0 || j.MaxAttempts != want.MaxAttempts || j.FailureKind != want.FailureKind ||
			string(j.SpecJSON) != string(want.SpecJSON) || j.QueuedAt != want.QueuedAt ||
			j.RunnerID != "" || j.ExitCode != nil || j.LeaseExpiresAt != 0 {
			t.Errorf("job[%d] = %+v, want %+v", i, j, want)
		}
	}
	if gotJobs[0].MaxAttempts != 1 || gotJobs[1].MaxAttempts != 3 || gotJobs[0].State != JobPending {
		t.Errorf("job defaults: %+v / %+v", gotJobs[0], gotJobs[1])
	}
	if gotJobs[2].FailureKind != FailureInfra || gotJobs[2].State != JobFailed {
		t.Errorf("failure_kind round-trip: %+v", gotJobs[2])
	}

	gotDeps, err := s.ListJobDeps(ctx, s.Reader(), "r1")
	if err != nil {
		t.Fatalf("ListJobDeps: %v", err)
	}
	wantDeps := []JobDep{{"j-lint", "j-build"}, {"j-test", "j-build"}}
	if len(gotDeps) != len(wantDeps) {
		t.Fatalf("ListJobDeps = %v, want %v", gotDeps, wantDeps)
	}
	for i := range wantDeps {
		if gotDeps[i] != wantDeps[i] {
			t.Errorf("dep[%d] = %v, want %v", i, gotDeps[i], wantDeps[i])
		}
	}

	if _, err := s.GetJob(ctx, s.Reader(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetJob missing: err = %v, want ErrNotFound", err)
	}
	// r0 was created after r1, so it lists first (newest first).
	runs, err := s.ListRuns(ctx, s.Reader(), 10)
	if err != nil || len(runs) != 2 || runs[0].ID != "r0" || runs[1].ID != "r1" {
		t.Errorf("ListRuns = %v, %v", runs, err)
	}
}

func TestCreateRunRollsBackOnBadDep(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)

	err := s.Tx(ctx, func(tx *sql.Tx) error {
		return s.CreateRun(ctx, tx, newRun("r1"),
			[]Job{{ID: "a", Name: "a"}}, []JobDep{{JobID: "a", NeedsJobID: "ghost"}})
	})
	if err == nil {
		t.Fatal("CreateRun with dangling dep succeeded")
	}
	if _, err := s.GetRun(ctx, s.Reader(), "r1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("run persisted after failed tx: err = %v", err)
	}
}

func TestEventsOrdering(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)

	err := s.Tx(ctx, func(tx *sql.Tx) error {
		return s.CreateRun(ctx, tx, newRun("r1"), []Job{{ID: "a", Name: "a"}}, nil)
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	kinds := []string{"run.created", "job.ready", "job.claimed", "job.finished", "run.finished"}
	var seqs []int64
	for _, k := range kinds {
		ev := &Event{RunID: "r1", Type: k}
		if k != "run.created" && k != "run.finished" {
			ev.JobID = "a"
		}
		if err := s.Tx(ctx, func(tx *sql.Tx) error { return s.AppendEvent(ctx, tx, ev) }); err != nil {
			t.Fatalf("AppendEvent %s: %v", k, err)
		}
		seqs = append(seqs, ev.ID)
	}
	// Unrelated run's event must not leak into r1's stream.
	if err := s.Tx(ctx, func(tx *sql.Tx) error {
		if err := s.CreateRun(ctx, tx, newRun("r2"), nil, nil); err != nil {
			return err
		}
		return s.AppendEvent(ctx, tx, &Event{RunID: "r2", Type: "run.created"})
	}); err != nil {
		t.Fatalf("second run: %v", err)
	}

	evs, err := s.ListEvents(ctx, s.Reader(), "r1", 0, 0)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(evs) != len(kinds) {
		t.Fatalf("ListEvents len = %d, want %d", len(evs), len(kinds))
	}
	for i, ev := range evs {
		if ev.Type != kinds[i] || ev.ID != seqs[i] {
			t.Errorf("event[%d] = %+v, want kind %s seq %d", i, ev, kinds[i], seqs[i])
		}
		if i > 0 && (ev.ID <= evs[i-1].ID || ev.CreatedAt < evs[i-1].CreatedAt) {
			t.Errorf("event[%d] not monotonic after event[%d]: %+v / %+v", i, i-1, ev, evs[i-1])
		}
		if string(ev.DetailJSON) != "{}" {
			t.Errorf("event[%d] payload = %q, want {}", i, ev.DetailJSON)
		}
	}
	if evs[0].JobID != "" || evs[1].JobID != "a" {
		t.Errorf("job ids: %+v", evs)
	}

	// Paging: afterID skips what the client has seen; limit caps the page.
	page, err := s.ListEvents(ctx, s.Reader(), "r1", seqs[1], 2)
	if err != nil {
		t.Fatalf("ListEvents page: %v", err)
	}
	if len(page) != 2 || page[0].Type != "job.claimed" || page[1].Type != "job.finished" {
		t.Errorf("page = %+v", page)
	}
}

func TestUpsertRunner(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)

	r := &Runner{ID: "rn1", Name: "box-a", Labels: map[string]string{"os": "linux"}, Capacity: 2, Version: "v0.1"}
	if err := s.Tx(ctx, func(tx *sql.Tx) error { return s.UpsertRunner(ctx, tx, r) }); err != nil {
		t.Fatalf("UpsertRunner: %v", err)
	}
	first := *r
	if first.RegisteredAt == 0 || first.LastSeenAt != first.RegisteredAt {
		t.Fatalf("first upsert timestamps: %+v", first)
	}
	if first.State != RunnerOnline {
		t.Errorf("first upsert state = %q, want %q", first.State, RunnerOnline)
	}

	r.Name = "box-a2"
	r.Labels["arch"] = "arm64"
	r.Version = "v0.2"
	r.State = RunnerOffline
	if err := s.Tx(ctx, func(tx *sql.Tx) error { return s.UpsertRunner(ctx, tx, r) }); err != nil {
		t.Fatalf("UpsertRunner again: %v", err)
	}
	got, err := s.GetRunner(ctx, s.Reader(), "rn1")
	if err != nil {
		t.Fatalf("GetRunner: %v", err)
	}
	if got.Name != "box-a2" || got.Version != "v0.2" || got.Capacity != 2 || got.State != RunnerOffline ||
		got.Labels["os"] != "linux" || got.Labels["arch"] != "arm64" {
		t.Errorf("GetRunner = %+v", got)
	}
	if got.RegisteredAt != first.RegisteredAt {
		t.Errorf("registered_at changed on re-upsert: %d -> %d", first.RegisteredAt, got.RegisteredAt)
	}
	if got.LastSeenAt <= first.LastSeenAt {
		t.Errorf("last_seen_at not refreshed: %d -> %d", first.LastSeenAt, got.LastSeenAt)
	}
	all, err := s.ListRunners(ctx, s.Reader())
	if err != nil || len(all) != 1 {
		t.Errorf("ListRunners = %v, %v", all, err)
	}
}

func TestTxRollsBackOnError(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)

	boom := errors.New("boom")
	err := s.Tx(ctx, func(tx *sql.Tx) error {
		if err := s.CreateRun(ctx, tx, newRun("r1"), nil, nil); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Tx err = %v, want boom", err)
	}
	if _, err := s.GetRun(ctx, s.Reader(), "r1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("run persisted after rollback: err = %v", err)
	}
}

func TestRunnerNamesAreUnique(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)

	a := &Runner{ID: "rn1", Name: "box", Capacity: 1}
	if err := s.Tx(ctx, func(tx *sql.Tx) error { return s.UpsertRunner(ctx, tx, a) }); err != nil {
		t.Fatalf("UpsertRunner: %v", err)
	}
	got, err := s.GetRunnerByName(ctx, s.Reader(), "box")
	if err != nil || got.ID != "rn1" {
		t.Fatalf("GetRunnerByName = %+v, %v", got, err)
	}
	if _, err := s.GetRunnerByName(ctx, s.Reader(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing name: err = %v, want ErrNotFound", err)
	}
	// A second id under the same name is rejected by the unique index.
	b := &Runner{ID: "rn2", Name: "box", Capacity: 1}
	if err := s.Tx(ctx, func(tx *sql.Tx) error { return s.UpsertRunner(ctx, tx, b) }); err == nil {
		t.Fatal("duplicate runner name was accepted")
	}
}

func TestLogChunksDedupAndCursor(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)
	jobs := []Job{{ID: "j1", Name: "a", SpecJSON: []byte(`{}`)}}
	if err := s.Tx(ctx, func(tx *sql.Tx) error { return s.CreateRun(ctx, tx, newRun("r1"), jobs, nil) }); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Out of order and duplicated delivery: 2, 1, 2 again, 3.
	deliveries := []struct {
		seq  int64
		data string
		want bool
	}{{2, "two", true}, {1, "one", true}, {2, "TWO", false}, {3, "three", true}}
	for _, d := range deliveries {
		err := s.Tx(ctx, func(tx *sql.Tx) error {
			ins, err := s.InsertLogChunk(ctx, tx, "j1", 1, d.seq, []byte(d.data))
			if err != nil {
				return err
			}
			if ins != d.want {
				t.Errorf("seq %d: inserted=%v, want %v", d.seq, ins, d.want)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("insert seq %d: %v", d.seq, err)
		}
	}
	got, err := s.ListLogChunks(ctx, s.Reader(), "j1", 1, 0)
	if err != nil {
		t.Fatalf("ListLogChunks: %v", err)
	}
	if len(got) != 3 || got[0].Seq != 1 || got[1].Seq != 2 || got[2].Seq != 3 || string(got[1].Data) != "two" {
		t.Fatalf("chunks = %+v", got)
	}
	// Cursor is strictly greater-than: after=2 does not return seq 2.
	if tail, _ := s.ListLogChunks(ctx, s.Reader(), "j1", 1, 2); len(tail) != 1 || tail[0].Seq != 3 {
		t.Errorf("after=2: %+v", tail)
	}
	if n, err := s.LogBytes(ctx, s.Reader(), "j1", 1); err != nil || n != int64(len("onetwothree")) {
		t.Errorf("LogBytes = %d, %v", n, err)
	}
	// Attempts are separate streams.
	if other, _ := s.ListLogChunks(ctx, s.Reader(), "j1", 2, 0); len(other) != 0 {
		t.Errorf("attempt 2 has chunks: %+v", other)
	}
}

func TestHasJobEvent(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)
	if err := s.Tx(ctx, func(tx *sql.Tx) error {
		if err := s.CreateRun(ctx, tx, newRun("r1"), []Job{{ID: "j1", Name: "a", SpecJSON: []byte(`{}`)}}, nil); err != nil {
			return err
		}
		return s.AppendEvent(ctx, tx, &Event{RunID: "r1", JobID: "j1", Type: "job.logs_truncated", DetailJSON: []byte(`{"attempt":2}`)})
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		typ     string
		attempt int
		want    bool
	}{{"job.logs_truncated", 2, true}, {"job.logs_truncated", 1, false}, {"job.failed", 2, false}} {
		got, err := s.HasJobEvent(ctx, s.Reader(), "r1", "j1", tc.typ, tc.attempt)
		if err != nil || got != tc.want {
			t.Errorf("HasJobEvent(%s, %d) = %v, %v; want %v", tc.typ, tc.attempt, got, err, tc.want)
		}
	}
}

func TestArtifactsUpsertListGet(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)
	jobs := []Job{{ID: "j1", Name: "a", SpecJSON: []byte(`{}`)}}
	if err := s.Tx(ctx, func(tx *sql.Tx) error { return s.CreateRun(ctx, tx, newRun("r1"), jobs, nil) }); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	put := func(a Artifact) {
		t.Helper()
		if err := s.Tx(ctx, func(tx *sql.Tx) error { return s.UpsertArtifact(ctx, tx, &a) }); err != nil {
			t.Fatalf("UpsertArtifact: %v", err)
		}
	}
	put(Artifact{JobID: "j1", Attempt: 1, Path: "dist/b.bin", SizeBytes: 2, SHA256: "bb", ContentType: "application/octet-stream"})
	put(Artifact{JobID: "j1", Attempt: 1, Path: "dist/a.bin", SizeBytes: 1, SHA256: "aa", ContentType: "application/octet-stream"})
	put(Artifact{JobID: "j1", Attempt: 2, Path: "other", SizeBytes: 9, SHA256: "cc", ContentType: "text/plain"})
	// Redelivery replaces the row rather than failing on the primary key.
	put(Artifact{JobID: "j1", Attempt: 1, Path: "dist/a.bin", SizeBytes: 3, SHA256: "aa2", ContentType: "application/octet-stream"})

	got, err := s.ListArtifacts(ctx, s.Reader(), "j1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Path != "dist/a.bin" || got[1].Path != "dist/b.bin" {
		t.Fatalf("ListArtifacts = %+v", got)
	}
	if got[0].SizeBytes != 3 || got[0].SHA256 != "aa2" || got[0].CreatedAt == 0 {
		t.Fatalf("redelivered row = %+v", got[0])
	}
	a, err := s.GetArtifact(ctx, s.Reader(), "j1", 2, "other")
	if err != nil || a.ContentType != "text/plain" {
		t.Fatalf("GetArtifact = %+v, %v", a, err)
	}
	if _, err := s.GetArtifact(ctx, s.Reader(), "j1", 3, "other"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetArtifact missing = %v", err)
	}
}
