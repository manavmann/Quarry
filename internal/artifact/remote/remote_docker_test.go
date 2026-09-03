//go:build docker

package remote

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"quarry/internal/artifact"
)

// The integration test runs against a real storage cluster whose
// coordinator is at QUARRY_TEST_REMOTE_URL (the compose `remote` profile
// publishes it on http://127.0.0.1:8090); it is skipped when unset. Each
// run uses a fresh bucket so leftovers of an earlier run never count.
func clusterStore(t *testing.T) *Store {
	t.Helper()
	base := os.Getenv("QUARRY_TEST_REMOTE_URL")
	if base == "" {
		t.Skip("QUARRY_TEST_REMOTE_URL not set")
	}
	var suffix [4]byte
	rand.Read(suffix[:])
	s, err := New(t.Context(), Config{
		BaseURL:  base,
		Bucket:   "quarry-test-" + hex.EncodeToString(suffix[:]),
		SpoolDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The ArtifactStore contract, end to end against the cluster: put with
// Content-Length, get bytes back intact, list under a prefix, delete,
// ErrNotFound afterwards, and a replaced object is read back whole.
func TestClusterRoundTrip(t *testing.T) {
	ctx := t.Context()
	s := clusterStore(t)
	body := make([]byte, 3<<20) // spans several node chunks
	rand.Read(body)
	sum := sha256.Sum256(body)
	key := artifact.JobKey("r1", "j1", 1, "dist/app.bin")
	if err := s.Put(ctx, key, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, artifact.SourceKey("r1"), strings.NewReader("tar"), -1); err != nil {
		t.Fatal(err)
	}
	rc, err := s.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || sha256.Sum256(got) != sum {
		t.Fatalf("Get returned %d bytes, err %v; sha mismatch", len(got), err)
	}
	keys, err := s.List(ctx, artifact.JobPrefix("r1", "j1", 1))
	if err != nil || len(keys) != 1 || keys[0] != key {
		t.Fatalf("List = %v, %v", keys, err)
	}
	if all, _ := s.List(ctx, ""); len(all) != 2 {
		t.Fatalf("List(\"\") = %v", all)
	}
	if err := s.Put(ctx, key, strings.NewReader("replaced"), 8); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, key); got != "replaced" {
		t.Fatalf("after replace Get = %q", got)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Get after delete = %v", err)
	}
	if err := s.Delete(ctx, key); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("second Delete = %v", err)
	}
	if keys, _ := s.List(ctx, "runs/"); len(keys) != 0 {
		t.Fatalf("List after delete = %v", keys)
	}
}

// A reader that fails part-way stores nothing on the cluster and a
// previous object survives — the store-failure semantics of local.
func TestClusterPutFailureLeavesNothing(t *testing.T) {
	ctx := t.Context()
	s := clusterStore(t)
	key := "runs/r1/jobs/j1/1/big.bin"
	boom := errors.New("boom")
	if err := s.Put(ctx, key, &failingReader{n: 100_000, err: boom}, 200_000); !errors.Is(err, boom) {
		t.Fatalf("Put = %v, want boom", err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("partial object visible: Get = %v", err)
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
}

// Every object is on at least the write quorum of nodes, so losing one
// replica holder still serves the bytes: the cluster's locate endpoint
// confirms the replica count the Verify step relies on.
func TestClusterReplicated(t *testing.T) {
	ctx := t.Context()
	s := clusterStore(t)
	key := "runs/r1/jobs/j1/1/rep.bin"
	if err := s.Put(ctx, key, strings.NewReader("replicated"), 10); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/cluster/locate?bucket=%s&key=%s", s.base, s.cfg.Bucket, key), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var loc struct {
		Replicas []struct {
			NodeID string `json:"node_id"`
			Status string `json:"status"`
		} `json:"replicas"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&loc); err != nil || resp.StatusCode != 200 {
		t.Fatalf("locate: %d %v", resp.StatusCode, err)
	}
	if len(loc.Replicas) < 2 {
		t.Fatalf("object on %d replicas, want the write quorum of 2: %+v", len(loc.Replicas), loc.Replicas)
	}
	if got := readAll(t, s, key); got != "replicated" {
		t.Fatalf("Get = %q", got)
	}
}
