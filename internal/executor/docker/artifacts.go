package docker

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/docker/docker/errdefs"

	"quarry/internal/executor"
)

// Artifact collection: after a zero exit, each declared artifact path is
// copied out of the container with CopyFromContainer and unpacked under
// spec.ArtifactDir; the runner uploads from there. The executor never
// talks to the API.
//
// CopyFromContainer returns a tar whose entries are relative to the
// PARENT of the requested path and prefixed with its basename: asking for
// /workspace/dist yields dist/, dist/app.bin. extractArtifact strips that
// one leading component, otherwise a destination already named dist/
// would end up as dist/dist/app.bin.

// collectArtifacts copies every declared artifact of spec out of
// container id. A declared path that does not exist in the container is
// reported in the log and skipped; any other failure is returned and the
// runner reports the attempt as an infra failure.
func (e *Executor) collectArtifacts(ctx context.Context, id string, spec executor.JobSpec, logs io.Writer) error {
	for _, declared := range spec.Job.Artifacts {
		rel, src, err := artifactPaths(declared)
		if err != nil {
			return err
		}
		rc, _, err := e.cli.CopyFromContainer(ctx, id, src)
		if err != nil {
			if errdefs.IsNotFound(err) {
				fmt.Fprintf(logs, "[quarry] artifact %s: not found in container, skipped\n", declared)
				continue
			}
			return fmt.Errorf("docker: copy artifact %s: %w", declared, err)
		}
		n, err := extractArtifact(rc, filepath.Join(spec.ArtifactDir, filepath.FromSlash(rel)))
		rc.Close()
		if err != nil {
			return fmt.Errorf("docker: extract artifact %s: %w", declared, err)
		}
		fmt.Fprintf(logs, "[quarry] artifact %s: %d file(s) collected\n", declared, n)
	}
	return nil
}

// artifactPaths resolves a declared artifact path to the slash-relative
// name it is uploaded under (rel) and the absolute path inside the
// container to copy (src). Relative paths are under /workspace; absolute
// ones are used as they are with the leading slash dropped from rel.
func artifactPaths(declared string) (rel, src string, err error) {
	p := path.Clean(strings.TrimSpace(declared))
	if path.IsAbs(p) {
		src, rel = p, strings.TrimPrefix(p, "/")
	} else {
		src, rel = path.Join(workspace, p), p
	}
	if rel == "" || rel == "." || rel == ".." || strings.HasPrefix(rel, "../") || strings.Contains(rel, "\\") {
		return "", "", fmt.Errorf("docker: invalid artifact path %q", declared)
	}
	return rel, src, nil
}

// extractArtifact unpacks a CopyFromContainer tar under dest, stripping
// the leading path component of every entry. A tar for a single file has
// one entry whose stripped name is empty: that file becomes dest itself.
// Only directories and regular files are written; symlinks, devices and
// the like are skipped. Returns the number of files written.
func extractArtifact(r io.Reader, dest string) (int, error) {
	tr := tar.NewReader(r)
	files := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return files, nil
		}
		if err != nil {
			return files, err
		}
		name, ok := stripFirst(hdr.Name)
		if !ok {
			return files, fmt.Errorf("unsafe tar entry %q", hdr.Name)
		}
		target := dest
		if name != "" {
			target = filepath.Join(dest, filepath.FromSlash(name))
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return files, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return files, err
			}
			if err := writeFile(target, tr, hdr.Size); err != nil {
				return files, err
			}
			files++
		}
	}
}

// stripFirst removes the first path component of a tar entry name and
// reports whether what remains is safe to join under a destination: no
// absolute names, no "." or ".." segments.
func stripFirst(name string) (string, bool) {
	name = strings.TrimSuffix(name, "/")
	if name == "" || strings.HasPrefix(name, "/") || strings.Contains(name, "\\") {
		return "", false
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", false
		}
	}
	_, rest, _ := strings.Cut(name, "/")
	return rest, true
}

func writeFile(target string, r io.Reader, size int64) error {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, r)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil && n != size {
		err = errors.New("short tar entry")
	}
	return err
}
