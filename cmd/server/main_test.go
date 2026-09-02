package main

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	go func() { done <- serve(ctx, srv, ln, log.New(io.Discard, "", 0)) }()
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
	if err := logStateCounts(ctx, st, log.New(&out, "", 0)); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"runs=map[pending:1]", "jobs=map[pending:1 queued:1]", "runners=map[]"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in %q", want, out.String())
		}
	}
}
