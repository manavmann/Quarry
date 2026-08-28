package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"sync"
	"testing"

	"quarry/internal/executor"
	"quarry/internal/pipeline"
)

// artifactStub extends stubServer with the upload endpoint. It records
// each file's bytes and the Content-Length it arrived with, and can be
// scripted to answer with a status sequence before accepting.
type artifactStub struct {
	*stubServer
	mu       sync.Mutex
	files    map[string][]byte // "<job>/<attempt>/<path>" -> body
	lengths  map[string]int64
	statuses []int // popped per upload; empty = 201
}

func newArtifactStub(t *testing.T) *artifactStub {
	t.Helper()
	s := &artifactStub{stubServer: newStub(t), files: map[string][]byte{}, lengths: map[string]int64{}}
	// Re-register the mux with the extra route: httptest.Server's handler
	// is the mux built by newStub, so add to it.
	s.srv.Config.Handler.(*http.ServeMux).HandleFunc("POST /api/runner/jobs/{id}/attempts/{attempt}/artifacts/{path...}", s.handleUpload)
	return s
}

func (s *artifactStub) handleUpload(w http.ResponseWriter, r *http.Request) {
	id, attempt, path := r.PathValue("id"), r.PathValue("attempt"), r.PathValue("path")
	s.stubServer.mu.Lock()
	s.stubServer.ops[id] = append(s.stubServer.ops[id], "artifact")
	s.stubServer.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.statuses) > 0 {
		code := s.statuses[0]
		s.statuses = s.statuses[1:]
		if code != http.StatusCreated {
			http.Error(w, `{"error":"scripted"}`, code)
			return
		}
	}
	key := id + "/" + attempt + "/" + path
	s.files[key] = body
	s.lengths[key] = r.ContentLength
	sum := sha256.Sum256(body)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(artifactReply{Path: path, SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:])})
}

func (s *artifactStub) enqueueWithArtifacts(name string, artifacts ...string) {
	spec, _ := json.Marshal(pipeline.Job{Name: name, Image: "alpine", Steps: []string{"true"}, Artifacts: artifacts})
	s.stubServer.mu.Lock()
	defer s.stubServer.mu.Unlock()
	s.queue = append(s.queue, claimedJob{ID: "j-" + name, RunID: "r1", Name: name, Attempt: 1, Spec: spec})
}

// Every file the executor leaves behind is uploaded once, with its
// Content-Length, under the attempt, before complete; the job then
// succeeds.
func TestAgentUploadsArtifactsBeforeComplete(t *testing.T) {
	s := newArtifactStub(t)
	s.enqueueWithArtifacts("build", "dist")
	f := executor.NewFake()
	f.Script("build", executor.Outcome{Artifacts: map[string]string{"dist/app.bin": "binary!", "dist/sub/x": "x"}})
	startAgent(t, s.stubServer, f, 1)

	waitFor(t, func() bool { return len(s.completed()) == 1 }, "completion")
	if c := s.completed()[0]; c.Req.Status != statusSucceeded {
		t.Fatalf("complete = %+v", c.Req)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if got := string(s.files["j-build/1/dist/app.bin"]); got != "binary!" || s.lengths["j-build/1/dist/app.bin"] != 7 {
		t.Fatalf("app.bin = %q len %d", got, s.lengths["j-build/1/dist/app.bin"])
	}
	if got := string(s.files["j-build/1/dist/sub/x"]); got != "x" {
		t.Fatalf("sub/x = %q", got)
	}
	ops := s.opsFor("j-build")
	if slices.Index(ops, "complete") != len(ops)-1 || count(ops, "artifact") != 2 {
		t.Fatalf("ops = %v: two uploads then complete last", ops)
	}
	// The executor was handed a directory to fill.
	if ex := f.Executions(); ex[0].Spec.ArtifactDir == "" {
		t.Fatal("executor was not given an artifact dir")
	}
}

// A 5xx on upload is retried; once it sticks the attempt is reported as
// an infra failure and never as succeeded.
func TestAgentArtifactUploadRetriesThenInfra(t *testing.T) {
	s := newArtifactStub(t)
	s.enqueueWithArtifacts("flaky", "out.txt")
	s.enqueueWithArtifacts("broken", "out.txt")
	f := executor.NewFake()
	f.Script("flaky", executor.Outcome{Artifacts: map[string]string{"out.txt": "1"}})
	f.Script("broken", executor.Outcome{Artifacts: map[string]string{"out.txt": "2"}})
	s.mu.Lock()
	s.statuses = []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusCreated}
	s.mu.Unlock()
	a, err := New(func() Config {
		c := testConfig(s.stubServer, 1)
		c.CompleteRetries = 3
		return c
	}(), f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := t.Context(), func() {}
	done := make(chan struct{})
	go func() { defer close(done); _ = a.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, func() bool { return len(s.completed()) == 1 }, "first completion")
	if c := s.completed()[0]; c.JobID != "j-flaky" || c.Req.Status != statusSucceeded {
		t.Fatalf("flaky = %+v", c)
	}
	s.mu.Lock()
	s.statuses = []int{500, 500, 500, 500, 500}
	s.mu.Unlock()
	// The second job is claimed after the first completes (capacity 1).
	waitFor(t, func() bool { return len(s.completed()) == 2 }, "second completion")
	c := s.completed()[1]
	if c.JobID != "j-broken" || c.Req.Status != statusFailed || c.Req.FailureKind != kindInfra {
		t.Fatalf("broken = %+v, want failed(infra)", c)
	}
	if n := count(s.opsFor("j-broken"), "artifact"); n != 3 {
		t.Fatalf("broken uploads = %d, want CompleteRetries (3)", n)
	}
}

// A 409 on upload means the attempt was superseded: nothing is reported.
func TestAgentArtifactUpload409Aborts(t *testing.T) {
	s := newArtifactStub(t)
	s.enqueueWithArtifacts("stale", "out.txt")
	f := executor.NewFake()
	f.Script("stale", executor.Outcome{Artifacts: map[string]string{"out.txt": "x"}})
	s.mu.Lock()
	s.statuses = []int{http.StatusConflict}
	s.mu.Unlock()
	a := startAgent(t, s.stubServer, f, 1)

	waitFor(t, func() bool { return len(f.Executions()) == 1 }, "execution")
	waitFor(t, func() bool { return len(a.activeRefs()) == 0 && len(s.opsFor("j-stale")) >= 1 }, "attempt to be forgotten")
	if c := s.completed(); len(c) != 0 {
		t.Fatalf("stale attempt was reported: %+v", c)
	}
	if ops := s.opsFor("j-stale"); slices.Contains(ops, "complete") || count(ops, "artifact") != 1 {
		t.Fatalf("ops = %v", ops)
	}
}

func count(ops []string, op string) int {
	n := 0
	for _, o := range ops {
		if o == op {
			n++
		}
	}
	return n
}
