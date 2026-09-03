package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"quarry/internal/artifact"
)

// fakeCoordinator speaks the subset of the coordinator's API the Store
// uses, with the same status codes and error bodies, plus fault
// injection: failPuts 503s that many PUTs, down 503s every PUT.
type fakeCoordinator struct {
	mu       sync.Mutex
	buckets  map[string]map[string][]byte
	limit    int // listing page size
	failPuts int
	down     bool
	puts     int      // object PUTs received
	lengths  []string // Content-Length header of each object PUT
	auths    []string // Authorization header of each request
	onPut    func()   // called on each object PUT, before it is handled
}

func newFake() *fakeCoordinator {
	return &fakeCoordinator{buckets: map[string]map[string][]byte{}, limit: 1000}
}

func (f *fakeCoordinator) writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func (f *fakeCoordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auths = append(f.auths, r.Header.Get("Authorization"))
	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	bucket, key, hasKey := strings.Cut(rest, "/")
	objs := f.buckets[bucket]
	switch {
	case !hasKey && r.Method == http.MethodPut: // create bucket
		if objs != nil {
			f.writeError(w, http.StatusConflict, "BucketAlreadyExists", "bucket exists")
			return
		}
		f.buckets[bucket] = map[string][]byte{}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"bucket": bucket})
	case objs == nil:
		f.writeError(w, http.StatusNotFound, "NoSuchBucket", "no such bucket")
	case key == "" && r.Method == http.MethodGet: // list
		q := r.URL.Query()
		var keys []string
		for k := range objs {
			if strings.HasPrefix(k, q.Get("prefix")) && k > q.Get("start_after") {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		truncated := len(keys) > f.limit
		if truncated {
			keys = keys[:f.limit]
		}
		page := map[string]any{"objects": []map[string]any{}, "truncated": truncated}
		for _, k := range keys {
			page["objects"] = append(page["objects"].([]map[string]any), map[string]any{"key": k, "size": len(objs[k])})
		}
		if truncated {
			page["next_start_after"] = keys[len(keys)-1]
		}
		json.NewEncoder(w).Encode(page)
	case r.Method == http.MethodPut:
		if f.onPut != nil {
			f.onPut()
		}
		f.puts++
		f.lengths = append(f.lengths, r.Header.Get("Content-Length"))
		if r.ContentLength < 0 {
			f.writeError(w, http.StatusLengthRequired, "LengthRequired", "Content-Length required")
			return
		}
		b, err := io.ReadAll(r.Body)
		if err != nil || int64(len(b)) != r.ContentLength {
			f.writeError(w, http.StatusBadRequest, "IncompleteBody", "body ended early")
			return
		}
		if f.down || f.failPuts > 0 {
			if f.failPuts > 0 {
				f.failPuts--
			}
			f.writeError(w, http.StatusServiceUnavailable, "InsufficientReplicas", "wrote 1 of 2 replicas")
			return
		}
		objs[key] = b
		json.NewEncoder(w).Encode(map[string]any{"bucket": bucket, "key": key, "size": len(b), "quorum": "2/3"})
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		b, ok := objs[key]
		if !ok {
			f.writeError(w, http.StatusNotFound, "NoSuchKey", "no such key")
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		w.Header().Set("X-Cairn-Replica", "node1")
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			w.Write(b)
		}
	case r.Method == http.MethodDelete: // idempotent, like the real one
		delete(objs, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

func (f *fakeCoordinator) keys(bucket string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k := range f.buckets[bucket] {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func newStore(t *testing.T, f *fakeCoordinator, cfg Config) *Store {
	s, _ := newStoreServer(t, f, cfg)
	return s
}

// newStoreServer also returns the fake's server, for tests that stop it.
func newStoreServer(t *testing.T, f *fakeCoordinator, cfg Config) (*Store, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	cfg.BaseURL = srv.URL
	cfg.SpoolDir = t.TempDir()
	if cfg.Backoff == 0 {
		cfg.Backoff = time.Millisecond
	}
	s, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s, srv
}

func readAll(t *testing.T, s *Store, key string) string {
	t.Helper()
	rc, err := s.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("Get(%s) = %v", key, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The bucket is created on first use and an existing one is fine.
func TestNewEnsuresBucket(t *testing.T) {
	f := newFake()
	s := newStore(t, f, Config{Bucket: "b"})
	if _, ok := f.buckets["b"]; !ok {
		t.Fatal("bucket not created")
	}
	if _, err := New(t.Context(), Config{BaseURL: s.base, Bucket: "b"}); err != nil {
		t.Fatalf("second New over an existing bucket = %v", err)
	}
	if _, err := New(t.Context(), Config{BaseURL: "storage:8080"}); err == nil {
		t.Fatal("scheme-less base url accepted")
	}
}

func TestPutGetDeleteList(t *testing.T) {
	ctx := t.Context()
	f := newFake()
	s := newStore(t, f, Config{})
	body := []byte("hello artifact")
	key := artifact.JobKey("r1", "j1", 1, "dist/app.bin")
	if err := s.Put(ctx, key, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, artifact.SourceKey("r1"), strings.NewReader("tar"), 3); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, key); got != string(body) {
		t.Fatalf("Get = %q, want %q", got, body)
	}
	if f.lengths[0] != strconv.Itoa(len(body)) {
		t.Fatalf("PUT Content-Length = %q, want %d", f.lengths[0], len(body))
	}
	keys, err := s.List(ctx, artifact.JobPrefix("r1", "j1", 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("List = %v", keys)
	}
	all, _ := s.List(ctx, "")
	if len(all) != 2 || all[0] != key || all[1] != "sources/r1.tar" {
		t.Fatalf("List(\"\") = %v", all)
	}
	if got, _ := s.List(ctx, "runs/r1/jobs/j1/2/"); len(got) != 0 {
		t.Fatalf("List other attempt = %v", got)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Get after delete = %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, key); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("second Delete = %v, want ErrNotFound", err)
	}
}

// A Put replaces the previous object whole and leaves one object.
func TestPutReplaces(t *testing.T) {
	ctx := t.Context()
	f := newFake()
	s := newStore(t, f, Config{})
	key := "runs/r1/jobs/j1/1/out.txt"
	if err := s.Put(ctx, key, strings.NewReader("one"), 3); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, key, strings.NewReader("second"), 6); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, key); got != "second" {
		t.Fatalf("Get = %q", got)
	}
	if k := f.keys(defaultBucket); len(k) != 1 || k[0] != key {
		t.Fatalf("objects after replace = %v", k)
	}
}

type failingReader struct {
	n   int
	err error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, r.err
	}
	k := min(len(p), r.n)
	for i := range p[:k] {
		p[i] = 'x'
	}
	r.n -= k
	return k, nil
}

// A reader that fails part-way never reaches the coordinator: no object,
// and a previous object under the key survives untouched.
func TestPutFailureLeavesNothing(t *testing.T) {
	ctx := t.Context()
	f := newFake()
	s := newStore(t, f, Config{})
	key := "runs/r1/jobs/j1/1/big.bin"
	boom := errors.New("boom")
	if err := s.Put(ctx, key, &failingReader{n: 100_000, err: boom}, 200_000); !errors.Is(err, boom) {
		t.Fatalf("Put = %v, want boom", err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("partial object visible: Get = %v", err)
	}
	if f.puts != 0 {
		t.Fatalf("coordinator saw %d PUTs from a failed reader", f.puts)
	}
	if err := s.Put(ctx, key, strings.NewReader("ok"), 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, key, &failingReader{n: 10, err: boom}, 20); !errors.Is(err, boom) {
		t.Fatalf("Put = %v, want boom", err)
	}
	if got := readAll(t, s, key); got != "ok" {
		t.Fatalf("previous object damaged: %q", got)
	}
	if entries, _ := os.ReadDir(s.cfg.SpoolDir); len(entries) != 0 {
		t.Fatalf("spool files left behind: %v", entries)
	}
}

// A size mismatch in either direction is rejected and nothing is sent.
func TestPutSizeMismatch(t *testing.T) {
	ctx := t.Context()
	f := newFake()
	s := newStore(t, f, Config{})
	if err := s.Put(ctx, "a/short", strings.NewReader("abc"), 5); err == nil {
		t.Fatal("short reader accepted")
	}
	if err := s.Put(ctx, "a/long", strings.NewReader("abcdef"), 5); err == nil {
		t.Fatal("long reader accepted")
	}
	if f.puts != 0 || len(f.keys(defaultBucket)) != 0 {
		t.Fatalf("PUTs = %d, objects = %v", f.puts, f.keys(defaultBucket))
	}
}

// A negative size means unknown: the reader is stored to EOF and the
// coordinator still gets an exact Content-Length.
func TestPutUnknownSize(t *testing.T) {
	ctx := t.Context()
	f := newFake()
	s := newStore(t, f, Config{})
	if err := s.Put(ctx, "sources/r1.tar", strings.NewReader("streamed"), -1); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, "sources/r1.tar"); got != "streamed" {
		t.Fatalf("Get = %q", got)
	}
	if f.lengths[0] != "8" {
		t.Fatalf("Content-Length = %q, want 8", f.lengths[0])
	}
}

// Every method rejects a key that could leave the bucket's key space,
// before any request is made.
func TestTraversalRejected(t *testing.T) {
	ctx := t.Context()
	f := newFake()
	s := newStore(t, f, Config{})
	before := len(f.auths)
	for _, key := range []string{"../escaped", "runs/../../escaped", "/escaped", "runs\\..\\escaped", "runs/./escaped", ""} {
		if err := s.Put(ctx, key, strings.NewReader("x"), 1); err == nil {
			t.Errorf("Put(%q) accepted", key)
		}
		if _, err := s.Get(ctx, key); err == nil || errors.Is(err, artifact.ErrNotFound) {
			t.Errorf("Get(%q) = %v, want validation error", key, err)
		}
		if err := s.Delete(ctx, key); err == nil || errors.Is(err, artifact.ErrNotFound) {
			t.Errorf("Delete(%q) = %v, want validation error", key, err)
		}
	}
	if _, err := s.List(ctx, "../"); err == nil {
		t.Error("List(\"../\") accepted")
	}
	if len(f.auths) != before {
		t.Fatalf("%d requests reached the coordinator for invalid keys", len(f.auths)-before)
	}
}

func TestPutHonoursContext(t *testing.T) {
	f := newFake()
	s := newStore(t, f, Config{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Put(ctx, "a/b", strings.NewReader("abc"), 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put = %v, want context.Canceled", err)
	}
	if f.puts != 0 {
		t.Fatal("cancelled Put reached the coordinator")
	}
}

// A PUT the cluster cannot commit (insufficient replicas, 503) is retried
// from the spool with the same Content-Length until it lands.
func TestPutRetriesInsufficientReplicas(t *testing.T) {
	f := newFake()
	f.failPuts = 2
	s := newStore(t, f, Config{Attempts: 3})
	if err := s.Put(t.Context(), "a/b", strings.NewReader("abc"), 3); err != nil {
		t.Fatal(err)
	}
	if f.puts != 3 || f.lengths[0] != "3" || f.lengths[1] != "3" || f.lengths[2] != "3" {
		t.Fatalf("PUTs = %d, lengths = %v", f.puts, f.lengths)
	}
	if got := readAll(t, s, "a/b"); got != "abc" {
		t.Fatalf("Get = %q", got)
	}
}

// The store-failure case: a cluster that never reaches quorum makes Put
// fail after the configured attempts with the coordinator's error code,
// and nothing is stored — the API turns this into the same 500 that the
// local backend's failure does (harness TestArtifactStoreFailureIsInfra).
func TestPutStoreDown(t *testing.T) {
	f := newFake()
	f.down = true
	s := newStore(t, f, Config{Attempts: 3})
	err := s.Put(t.Context(), "a/b", strings.NewReader("abc"), 3)
	var e *Error
	if !errors.As(err, &e) || e.Status != 503 || e.Code != "InsufficientReplicas" {
		t.Fatalf("Put = %v, want 503 InsufficientReplicas", err)
	}
	if !strings.Contains(err.Error(), "after 3 attempts") || f.puts != 3 {
		t.Fatalf("Put = %v after %d PUTs, want 3", err, f.puts)
	}
	if _, err := s.Get(t.Context(), "a/b"); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
	// A coordinator that is not there at all fails the same way.
	gone, srv := newStoreServer(t, newFake(), Config{Attempts: 2})
	srv.Close()
	if err := gone.Put(t.Context(), "a/b", strings.NewReader("abc"), 3); err == nil {
		t.Fatal("Put to a closed coordinator succeeded")
	}
	if _, err := gone.Get(t.Context(), "a/b"); err == nil || errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Get from a closed coordinator = %v, want a transport error", err)
	}
	if _, err := New(t.Context(), Config{BaseURL: srv.URL, Attempts: 1}); err == nil {
		t.Fatal("New against a closed coordinator succeeded")
	}
}

// A 4xx is the coordinator's final word: no retry, decoded error.
func TestClientErrorNotRetried(t *testing.T) {
	f := newFake()
	s := newStore(t, f, Config{Attempts: 5})
	// A bucket the fake has never seen makes every object request a 404
	// NoSuchBucket; Get and Delete map it to ErrNotFound like NoSuchKey.
	s.cfg.Bucket = "missing"
	before := len(f.auths)
	if _, err := s.Get(t.Context(), "a/b"); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Get = %v", err)
	}
	err := s.Put(t.Context(), "a/b", strings.NewReader("abc"), 3)
	var e *Error
	if !errors.As(err, &e) || e.Status != 404 || e.Code != "NoSuchBucket" || !strings.Contains(err.Error(), "NoSuchBucket") {
		t.Fatalf("Put = %v, want decoded 404 NoSuchBucket", err)
	}
	if len(f.auths)-before != 2 {
		t.Fatalf("%d requests for two 4xx calls, want 2 (no retries)", len(f.auths)-before)
	}
}

// Cancelling the context while a request is in flight or in its retry
// backoff returns at once instead of waiting out the backoff.
func TestRetryHonoursContext(t *testing.T) {
	f := newFake()
	f.down = true
	ctx, cancel := context.WithCancel(t.Context())
	f.onPut = cancel
	s := newStore(t, f, Config{Attempts: 5, Backoff: time.Hour, MaxBackoff: time.Hour})
	err := s.Put(ctx, "a/b", strings.NewReader("abc"), 3)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Put = %v, want context.Canceled", err)
	}
	if f.puts != 1 {
		t.Fatalf("PUTs = %d, want 1", f.puts)
	}
}

// List follows truncated pages and returns every key sorted.
func TestListPaginates(t *testing.T) {
	ctx := t.Context()
	f := newFake()
	f.limit = 2
	s := newStore(t, f, Config{})
	want := []string{"runs/r1/a", "runs/r1/b", "runs/r1/c", "runs/r1/d", "runs/r1/e"}
	for _, k := range []string{"runs/r1/c", "runs/r1/a", "runs/r1/e", "runs/r1/b", "runs/r1/d", "runs/r2/a"} {
		if err := s.Put(ctx, k, strings.NewReader("x"), 1); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.List(ctx, "runs/r1/")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("List = %v, want %v", got, want)
	}
	if got, _ := s.List(ctx, "nothing/"); len(got) != 0 {
		t.Fatalf("List(nothing/) = %v", got)
	}
}

// A configured token rides on every request as a bearer; none otherwise.
func TestBearerToken(t *testing.T) {
	f := newFake()
	s := newStore(t, f, Config{Token: "s3cret"})
	if err := s.Put(t.Context(), "a/b", strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	for _, a := range f.auths {
		if a != "Bearer s3cret" {
			t.Fatalf("Authorization = %q", a)
		}
	}
	plain := newFake()
	newStore(t, plain, Config{})
	if plain.auths[0] != "" {
		t.Fatalf("Authorization sent without a token: %q", plain.auths[0])
	}
}

// Keys with characters that need escaping round-trip unchanged.
func TestKeyEscaping(t *testing.T) {
	f := newFake()
	s := newStore(t, f, Config{})
	key := "runs/r1/jobs/j1/1/with space/ünï #1.txt"
	if err := s.Put(t.Context(), key, strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	if k := f.keys(defaultBucket); len(k) != 1 || k[0] != key {
		t.Fatalf("stored keys = %v, want %q", k, key)
	}
	if got := readAll(t, s, key); got != "x" {
		t.Fatalf("Get = %q", got)
	}
	if keys, _ := s.List(t.Context(), "runs/r1/jobs/j1/1/"); len(keys) != 1 || keys[0] != key {
		t.Fatalf("List = %v", keys)
	}
}
