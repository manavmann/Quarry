package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func shaOf(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// artifactServer serves the two artifact endpoints for job "j1" from an
// in-memory map; sha256 values in the listing come from `sums`, so a test
// can lie about one to provoke a mismatch.
func artifactServer(t *testing.T, files map[string]string, sums map[string]string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/jobs/j1/artifacts", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "nope", http.StatusUnauthorized)
			return
		}
		var arts []Artifact
		for p, body := range files {
			arts = append(arts, Artifact{Path: p, SizeBytes: int64(len(body)), SHA256: sums[p]})
		}
		json.NewEncoder(w).Encode(map[string]any{"attempt": 1, "artifacts": arts})
	})
	mux.HandleFunc("GET /api/jobs/j1/artifacts/{path...}", func(w http.ResponseWriter, r *http.Request) {
		body, ok := files[r.PathValue("path")]
		if !ok {
			http.Error(w, `{"error":"artifact not found"}`, http.StatusNotFound)
			return
		}
		w.Write([]byte(body))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func runCLI(t *testing.T, srv *httptest.Server, args ...string) (string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	root := New(&out, &errOut)
	root.SetArgs(append([]string{"--server", srv.URL, "--token", "tok"}, args...))
	err := root.ExecuteContext(context.Background())
	return out.String(), err
}

func TestArtifactsListAndDownload(t *testing.T) {
	files := map[string]string{"dist/app.bin": "binary!", "report.txt": "ok\n"}
	sums := map[string]string{"dist/app.bin": shaOf("binary!"), "report.txt": shaOf("ok\n")}
	srv := artifactServer(t, files, sums)

	out, err := runCLI(t, srv, "artifacts", "j1")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"PATH", "SIZE", "SHA256", "dist/app.bin", "7", shaOf("binary!"), "report.txt"} {
		if !strings.Contains(out, want) {
			t.Errorf("listing lacks %q:\n%s", want, out)
		}
	}

	dir := filepath.Join(t.TempDir(), "out")
	if _, err := runCLI(t, srv, "artifacts", "j1", "--download", dir); err != nil {
		t.Fatal(err)
	}
	for p, want := range files {
		got, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(p)))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v; want %q", p, got, err, want)
		}
	}
	if leftovers, _ := filepath.Glob(filepath.Join(dir, "*", ".quarry-dl-*")); len(leftovers) != 0 {
		t.Errorf("temp files left: %v", leftovers)
	}
}

// A body whose sha256 differs from the listing is an error and leaves no
// file; a listing path that would escape the directory is refused.
func TestArtifactsDownloadVerifiesAndStaysInside(t *testing.T) {
	files := map[string]string{"report.txt": "ok\n"}
	srv := artifactServer(t, files, map[string]string{"report.txt": shaOf("tampered")})
	dir := filepath.Join(t.TempDir(), "out")
	_, err := runCLI(t, srv, "artifacts", "j1", "--download", dir)
	if err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("err = %v, want sha256 mismatch", err)
	}
	if _, serr := os.Stat(filepath.Join(dir, "report.txt")); serr == nil {
		t.Fatal("mismatched artifact was written")
	}

	base := t.TempDir()
	srv = artifactServer(t, map[string]string{"../escape": "x"}, map[string]string{"../escape": shaOf("x")})
	_, err = runCLI(t, srv, "artifacts", "j1", "--download", filepath.Join(base, "out"))
	if err == nil || !strings.Contains(err.Error(), "unsafe path") {
		t.Fatalf("err = %v, want unsafe path", err)
	}
	if _, serr := os.Stat(filepath.Join(base, "escape")); serr == nil {
		t.Fatal("artifact escaped the download directory")
	}
}

func TestSafeRelPath(t *testing.T) {
	for _, p := range []string{"a", "a/b/c", "a.b/c.d"} {
		if !safeRelPath(p) {
			t.Errorf("safeRelPath(%q) = false", p)
		}
	}
	for _, p := range []string{"", "/a", "../a", "a/../b", "a//b", "./a", "a\\b", "a\x00"} {
		if safeRelPath(p) {
			t.Errorf("safeRelPath(%q) = true", p)
		}
	}
}
