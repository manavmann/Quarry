package docker

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// dockerTar builds a tar shaped like CopyFromContainer's output: entries
// relative to the parent of the requested path, prefixed by its basename.
func dockerTar(t *testing.T, entries map[string]string) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, body := range entries {
		hdr := &tar.Header{Name: name, Mode: 0o644, Typeflag: tar.TypeReg, Size: int64(len(body))}
		if name[len(name)-1] == '/' {
			hdr.Typeflag, hdr.Mode, hdr.Size = tar.TypeDir, 0o755, 0
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if hdr.Typeflag == tar.TypeReg {
			if _, err := tw.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return string(b)
}

// The day-1 spike bug: requesting /workspace/dist yields dist/, dist/app.bin;
// extracting into a destination already named dist must not produce
// dist/dist/app.bin.
func TestExtractDirectoryArtifactStripsBasename(t *testing.T) {
	dir := t.TempDir()
	tarball := dockerTar(t, map[string]string{
		"dist/":          "",
		"dist/app.bin":   "binary",
		"dist/sub/":      "",
		"dist/sub/x.txt": "x",
	})
	n, err := extractArtifact(tarball, filepath.Join(dir, "dist"))
	if err != nil || n != 2 {
		t.Fatalf("extract: n=%d, err=%v", n, err)
	}
	if got := readFile(t, filepath.Join(dir, "dist", "app.bin")); got != "binary" {
		t.Fatalf("app.bin = %q", got)
	}
	if got := readFile(t, filepath.Join(dir, "dist", "sub", "x.txt")); got != "x" {
		t.Fatalf("sub/x.txt = %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "dist", "dist")); err == nil {
		t.Fatal("doubled path segment dist/dist exists")
	}
}

// A single-file artifact's tar has one entry named after the file; the
// stripped name is empty and the file becomes the destination itself.
func TestExtractSingleFileArtifact(t *testing.T) {
	dir := t.TempDir()
	n, err := extractArtifact(dockerTar(t, map[string]string{"out.txt": "top"}), filepath.Join(dir, "out.txt"))
	if err != nil || n != 1 {
		t.Fatalf("extract: n=%d, err=%v", n, err)
	}
	if got := readFile(t, filepath.Join(dir, "out.txt")); got != "top" {
		t.Fatalf("out.txt = %q", got)
	}
}

func TestExtractRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"dist/../../escape", "/abs", "dist/./x", "../x"} {
		if _, err := extractArtifact(dockerTar(t, map[string]string{name: "x"}), filepath.Join(dir, "dist")); err == nil {
			t.Errorf("entry %q accepted", name)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "escape")); err == nil {
		t.Fatal("traversal entry escaped the destination")
	}
}

func TestExtractSkipsSymlinks(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	_ = tw.WriteHeader(&tar.Header{Name: "dist/", Typeflag: tar.TypeDir, Mode: 0o755})
	_ = tw.WriteHeader(&tar.Header{Name: "dist/link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd", Mode: 0o777})
	_ = tw.WriteHeader(&tar.Header{Name: "dist/f", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1})
	_, _ = tw.Write([]byte("f"))
	_ = tw.Close()
	dir := t.TempDir()
	n, err := extractArtifact(&buf, filepath.Join(dir, "dist"))
	if err != nil || n != 1 {
		t.Fatalf("extract: n=%d, err=%v", n, err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "dist", "link")); err == nil {
		t.Fatal("symlink was written")
	}
}

func TestArtifactPaths(t *testing.T) {
	cases := []struct {
		in, rel, src string
		bad          bool
	}{
		{in: "dist", rel: "dist", src: "/workspace/dist"},
		{in: "./dist/", rel: "dist", src: "/workspace/dist"},
		{in: "out/a.txt", rel: "out/a.txt", src: "/workspace/out/a.txt"},
		{in: "/tmp/report.xml", rel: "tmp/report.xml", src: "/tmp/report.xml"},
		{in: "..", bad: true},
		{in: "../x", bad: true},
		{in: ".", bad: true},
		{in: "/", bad: true},
		{in: "", bad: true},
	}
	for _, c := range cases {
		rel, src, err := artifactPaths(c.in)
		if c.bad {
			if err == nil {
				t.Errorf("artifactPaths(%q) accepted: %q %q", c.in, rel, src)
			}
			continue
		}
		if err != nil || rel != c.rel || src != c.src {
			t.Errorf("artifactPaths(%q) = %q, %q, %v; want %q, %q", c.in, rel, src, err, c.rel, c.src)
		}
	}
}
