package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"quarry/internal/artifact/remote"
	"quarry/internal/store"
)

func TestServerShutdownDrainsRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, release, shutting := make(chan struct{}), make(chan struct{}), make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			if r.Context().Err() != nil {
				t.Error("shutdown cancelled an in-flight request")
			}
			io.WriteString(w, "committed")
		case <-r.Context().Done():
			t.Error("request cancelled before drain")
		}
	})}
	srv.RegisterOnShutdown(func() { close(shutting) })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, srv, ln, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	reply := make(chan string, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err != nil {
			reply <- err.Error()
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		reply <- string(b)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request never entered")
	}
	cancel()
	<-shutting
	select {
	case err := <-done:
		t.Fatalf("returned before drain: %v", err)
	default:
	}
	close(release)
	if got := <-reply; got != "committed" {
		t.Fatalf("reply = %q", got)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestStartupStateCounts(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "quarry.db")
	st, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	err = st.Tx(ctx, func(tx *sql.Tx) error {
		return st.CreateRun(ctx, tx, &store.Run{ID: "r", PipelineYAML: "test", Trigger: "test"},
			[]store.Job{
				{ID: "a", Name: "a", State: store.JobQueued, SpecJSON: []byte("{}")},
				{ID: "b", Name: "b", State: store.JobPending, SpecJSON: []byte("{}")},
			}, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var out bytes.Buffer
	if err := logStateCounts(ctx, st, slog.New(slog.NewJSONHandler(&out, nil))); err != nil {
		t.Fatal(err)
	}
	var line struct {
		Msg     string           `json:"msg"`
		Runs    map[string]int64 `json:"runs"`
		Jobs    map[string]int64 `json:"jobs"`
		Runners map[string]int64 `json:"runners"`
	}
	if err := json.Unmarshal(out.Bytes(), &line); err != nil {
		t.Fatalf("not one JSON line: %q: %v", out.String(), err)
	}
	if line.Msg != "startup states" || line.Runs["pending"] != 1 || line.Jobs["pending"] != 1 || line.Jobs["queued"] != 1 || len(line.Runners) != 0 {
		t.Fatalf("startup line = %q", out.String())
	}
}

// The artifact backend is chosen by QUARRY_ARTIFACT_BACKEND: local by
// default, remote only with a coordinator URL, anything else refused.
func TestLoadConfigArtifactBackend(t *testing.T) {
	t.Setenv("QUARRY_API_TOKEN", "tok")
	t.Setenv("QUARRY_ARTIFACT_BACKEND", "")
	t.Setenv("QUARRY_ARTIFACT_REMOTE_URL", "")
	cfg, err := loadConfig()
	if err != nil || cfg.backend != "local" || cfg.blobDir != "quarry-artifacts" {
		t.Fatalf("default = %+v, %v", cfg, err)
	}
	t.Setenv("QUARRY_ARTIFACT_BACKEND", "remote")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "QUARRY_ARTIFACT_REMOTE_URL") {
		t.Fatalf("remote without a url: err = %v", err)
	}
	t.Setenv("QUARRY_ARTIFACT_REMOTE_URL", "http://storage:8080")
	t.Setenv("QUARRY_ARTIFACT_REMOTE_TOKEN", "s3cret")
	cfg, err = loadConfig()
	if err != nil || cfg.backend != "remote" || cfg.remote.BaseURL != "http://storage:8080" ||
		cfg.remote.Bucket != "quarry-artifacts" || cfg.remote.Token != "s3cret" {
		t.Fatalf("remote = %+v, %v", cfg, err)
	}
	t.Setenv("QUARRY_ARTIFACT_BACKEND", "s3")
	if _, err := loadConfig(); err == nil {
		t.Fatal("unknown backend accepted")
	}
}

// With the remote backend a coordinator that cannot be reached fails
// startup; the bucket is ensured before the server listens.
func TestOpenArtifactsRemote(t *testing.T) {
	ctx := t.Context()
	var created atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/quarry-artifacts" {
			created.Add(1)
			w.WriteHeader(http.StatusCreated)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	cfg := config{backend: "remote", remote: remote.Config{BaseURL: srv.URL, Bucket: "quarry-artifacts"}}
	blobs, desc, err := openArtifacts(ctx, cfg)
	if err != nil || blobs == nil || created.Load() != 1 || desc != srv.URL+"/v1/quarry-artifacts" {
		t.Fatalf("openArtifacts = %v, %q, %v (bucket puts %d)", blobs, desc, err, created.Load())
	}
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	cfg.remote = remote.Config{BaseURL: closed.URL, Attempts: 1}
	if _, _, err := openArtifacts(ctx, cfg); err == nil {
		t.Fatal("unreachable coordinator accepted at startup")
	}
	cfg = config{backend: "local", blobDir: filepath.Join(t.TempDir(), "blobs")}
	if _, desc, err := openArtifacts(ctx, cfg); err != nil || !strings.HasSuffix(desc, "blobs") {
		t.Fatalf("local openArtifacts = %q, %v", desc, err)
	}
}
