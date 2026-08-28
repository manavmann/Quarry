// Package local is the filesystem artifact.Store: one file per key under
// a root directory. Put writes to a temporary file in the destination's
// own directory and renames it into place, so a reader that fails, a full
// disk or a crash never leaves a partial object under the key; the rename
// is on one filesystem and so atomic.
package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"quarry/internal/artifact"
)

// tmpPrefix marks in-flight files; List never reports them and a crashed
// Put's leftover is recognisable.
const tmpPrefix = ".quarry-put-"

// Store is an artifact.Store rooted at a directory.
type Store struct {
	root string
}

// New creates root if needed and returns a Store over it.
func New(root string) (*Store, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("local: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("local: %w", err)
	}
	return &Store{root: abs}, nil
}

// Root is the directory objects live under.
func (s *Store) Root() string { return s.root }

// path maps a validated key onto the filesystem.
func (s *Store) path(key string) (string, error) {
	if err := artifact.ValidateKey(key); err != nil {
		return "", err
	}
	return filepath.Join(s.root, filepath.FromSlash(key)), nil
}

// Put implements artifact.Store.
func (s *Store) Put(ctx context.Context, key string, r io.Reader, size int64) (err error) {
	dst, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("local: put %s: %w", key, err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), tmpPrefix+"*")
	if err != nil {
		return fmt.Errorf("local: put %s: %w", key, err)
	}
	// Any failure below removes the temp file; only a completed rename
	// makes the object visible.
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	var src io.Reader = readerCtx{ctx, r}
	if size >= 0 {
		src = io.LimitReader(src, size+1) // one extra byte detects a long reader
	}
	n, err := io.Copy(tmp, src)
	if err != nil {
		return fmt.Errorf("local: put %s: %w", key, err)
	}
	if size >= 0 && n != size {
		return fmt.Errorf("local: put %s: got %d bytes, want %d", key, n, size)
	}
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("local: put %s: %w", key, err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("local: put %s: %w", key, err)
	}
	if err = os.Rename(tmp.Name(), dst); err != nil {
		return fmt.Errorf("local: put %s: %w", key, err)
	}
	return nil
}

// Get implements artifact.Store.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, artifact.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("local: get %s: %w", key, err)
	}
	if info, err := f.Stat(); err != nil || info.IsDir() {
		f.Close()
		return nil, artifact.ErrNotFound
	}
	return f, nil
}

// Delete implements artifact.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if errors.Is(err, fs.ErrNotExist) {
		return artifact.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("local: delete %s: %w", key, err)
	}
	return nil
}

// List implements artifact.Store. The prefix is a plain string prefix on
// keys, so "runs/r1/" and "runs/r1" both match the same objects.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	if strings.Contains(prefix, "\\") || strings.Contains(prefix, "..") {
		return nil, errors.New("local: invalid prefix")
	}
	// Walk from the deepest directory the prefix fully names.
	dir := s.root
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		dir = filepath.Join(s.root, filepath.FromSlash(prefix[:i]))
	}
	var keys []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), tmpPrefix) {
			return nil
		}
		rel, err := filepath.Rel(s.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("local: list %s: %w", prefix, err)
	}
	sort.Strings(keys)
	return keys, nil
}

// readerCtx stops a copy once ctx is done, so a Put whose request went
// away does not keep writing.
type readerCtx struct {
	ctx context.Context
	r   io.Reader
}

func (r readerCtx) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
