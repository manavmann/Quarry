package local

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"quarry/internal/artifact"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// entries lists every file under root (including temp files), for
// asserting that a failed Put left nothing behind.
func entries(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			rel, _ := filepath.Rel(root, p)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPutGetDeleteList(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	body := []byte("hello artifact")
	key := artifact.JobKey("r1", "j1", 1, "dist/app.bin")
	if err := s.Put(ctx, key, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, artifact.SourceKey("r1"), strings.NewReader("tar"), 3); err != nil {
		t.Fatal(err)
	}
	rc, err := s.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, body) {
		t.Fatalf("Get = %q, want %q", got, body)
	}
	keys, err := s.List(ctx, artifact.JobPrefix("r1", "j1", 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("List = %v", keys)
	}
	all, _ := s.List(ctx, "")
	if len(all) != 2 {
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

// A Put replaces the previous object whole: a concurrent reader sees
// either the old or the new bytes, never a mix, and no temp file remains.
func TestPutReplacesAtomically(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	key := "runs/r1/jobs/j1/1/out.txt"
	if err := s.Put(ctx, key, strings.NewReader("one"), 3); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, key, strings.NewReader("second"), 6); err != nil {
		t.Fatal(err)
	}
	rc, err := s.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "second" {
		t.Fatalf("Get = %q", got)
	}
	if e := entries(t, s.Root()); len(e) != 1 || e[0] != key {
		t.Fatalf("files after replace = %v", e)
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

// A reader that fails part-way leaves no object and no temp file; a
// previous object under the key survives untouched.
func TestPutFailureLeavesNothing(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	key := "runs/r1/jobs/j1/1/big.bin"
	boom := errors.New("boom")
	if err := s.Put(ctx, key, &failingReader{n: 100_000, err: boom}, 200_000); !errors.Is(err, boom) {
		t.Fatalf("Put = %v, want boom", err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("partial object visible: Get = %v", err)
	}
	if e := entries(t, s.Root()); len(e) != 0 {
		t.Fatalf("leftover files after failed put: %v", e)
	}

	if err := s.Put(ctx, key, strings.NewReader("ok"), 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, key, &failingReader{n: 10, err: boom}, 20); !errors.Is(err, boom) {
		t.Fatalf("Put = %v, want boom", err)
	}
	rc, err := s.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "ok" {
		t.Fatalf("previous object damaged: %q", got)
	}
	if e := entries(t, s.Root()); len(e) != 1 {
		t.Fatalf("files = %v", e)
	}
}

// A size mismatch in either direction is rejected and nothing is stored.
func TestPutSizeMismatch(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if err := s.Put(ctx, "a/short", strings.NewReader("abc"), 5); err == nil {
		t.Fatal("short reader accepted")
	}
	if err := s.Put(ctx, "a/long", strings.NewReader("abcdef"), 5); err == nil {
		t.Fatal("long reader accepted")
	}
	if e := entries(t, s.Root()); len(e) != 0 {
		t.Fatalf("files = %v", e)
	}
}

// Every method rejects a key that would leave the root.
func TestTraversalRejected(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	outside := filepath.Join(filepath.Dir(s.Root()), "escaped")
	for _, key := range []string{"../escaped", "runs/../../escaped", "/escaped", "runs\\..\\escaped", "runs/./escaped"} {
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
	if _, err := os.Stat(outside); err == nil {
		t.Fatal("a traversal key wrote outside the root")
	}
	if _, err := s.List(ctx, "../"); err == nil {
		t.Error("List(\"../\") accepted")
	}
}

func TestPutHonoursContext(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Put(ctx, "a/b", strings.NewReader("abc"), 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put = %v, want context.Canceled", err)
	}
	if e := entries(t, s.Root()); len(e) != 0 {
		t.Fatalf("files = %v", e)
	}
}

// A negative size means unknown: the reader is stored to EOF.
func TestPutUnknownSize(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if err := s.Put(ctx, "sources/r1.tar", strings.NewReader("streamed"), -1); err != nil {
		t.Fatal(err)
	}
	rc, err := s.Get(ctx, "sources/r1.tar")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "streamed" {
		t.Fatalf("Get = %q", got)
	}
}
