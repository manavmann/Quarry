package cli

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// entries reads a tar stream into name -> (type flag, content/linkname).
func entries(t *testing.T, r io.Reader) (map[string]byte, map[string]string, []string) {
	t.Helper()
	tr := tar.NewReader(r)
	types := map[string]byte{}
	bodies := map[string]string{}
	var order []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return types, bodies, order
		}
		if err != nil {
			t.Fatal(err)
		}
		order = append(order, hdr.Name)
		types[hdr.Name] = hdr.Typeflag
		if hdr.Typeflag == tar.TypeSymlink {
			bodies[hdr.Name] = hdr.Linkname
			continue
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		bodies[hdr.Name] = string(b)
	}
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestBundleExcludesGit(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, ".quarry.yml"), "name: demo\n")
	write(t, filepath.Join(dir, "src", "main.go"), "package main\n")
	write(t, filepath.Join(dir, ".git", "HEAD"), "ref: refs/heads/main\n")
	write(t, filepath.Join(dir, ".git", "objects", "ab", "cd"), "blob")
	write(t, filepath.Join(dir, "vendor", "dep", ".git", "config"), "nested")
	write(t, filepath.Join(dir, "vendor", "dep", "dep.go"), "package dep\n")
	write(t, filepath.Join(dir, ".gitignore"), "bin/\n") // not .git: kept

	var buf bytes.Buffer
	if err := Bundle(&buf, dir); err != nil {
		t.Fatal(err)
	}
	types, bodies, order := entries(t, &buf)

	for name := range types {
		if name == ".git/" || strings.Contains(name, ".git/") || strings.HasSuffix(name, "/.git") {
			t.Errorf("bundle contains %q", name)
		}
	}
	want := []string{".gitignore", ".quarry.yml", "src/", "src/main.go", "vendor/", "vendor/dep/", "vendor/dep/dep.go"}
	if strings.Join(order, " ") != strings.Join(want, " ") {
		t.Errorf("entries = %v, want %v", order, want)
	}
	if bodies["src/main.go"] != "package main\n" || bodies[".quarry.yml"] != "name: demo\n" {
		t.Errorf("bodies = %v", bodies)
	}
	if types["src/"] != tar.TypeDir || types["src/main.go"] != tar.TypeReg {
		t.Errorf("types = %v", types)
	}
}

func TestBundleDoesNotFollowSymlinks(t *testing.T) {
	outside := t.TempDir()
	write(t, filepath.Join(outside, "secret"), "do not ship")
	dir := t.TempDir()
	write(t, filepath.Join(dir, "a.txt"), "a")
	if err := os.Symlink(outside, filepath.Join(dir, "link-dir")); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink creation not permitted: %v", err)
		}
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(dir, "link-file")); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := Bundle(&buf, dir); err != nil {
		t.Fatal(err)
	}
	types, bodies, _ := entries(t, &buf)
	if types["link-dir"] != tar.TypeSymlink || types["link-file"] != tar.TypeSymlink {
		t.Fatalf("symlinks not recorded as symlinks: %v", types)
	}
	if _, ok := types["link-dir/secret"]; ok {
		t.Error("symlinked directory was walked")
	}
	for _, body := range bodies {
		if strings.Contains(body, "do not ship") {
			t.Error("symlink target content was bundled")
		}
	}
}

func TestBundleIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "b"), "2")
	write(t, filepath.Join(dir, "a"), "1")
	var one, two bytes.Buffer
	if err := Bundle(&one, dir); err != nil {
		t.Fatal(err)
	}
	if err := Bundle(&two, dir); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(one.Bytes(), two.Bytes()) {
		t.Error("two bundles of the same tree differ")
	}
}
