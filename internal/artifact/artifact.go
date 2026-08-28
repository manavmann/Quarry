// Package artifact is the control plane's blob store: source bundles and
// job artifacts live behind the Store interface, keyed by slash-separated
// paths that the server alone constructs. The server is the only writer;
// runners upload through the API and clients download through it.
//
// Keys:
//
//	sources/<run>.tar
//	runs/<run>/jobs/<job>/<attempt>/<path>
package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ErrNotFound is returned by Get and Delete for a key that is not stored.
var ErrNotFound = errors.New("artifact: not found")

// Store is a blob store. Implementations must make Put atomic: a reader
// that fails part-way, or a crash, leaves no partial object under key.
// Keys are validated by ValidateKey before any call.
type Store interface {
	// Put stores size bytes read from r under key, replacing any previous
	// object. A reader that yields other than size bytes is an error and
	// nothing is stored. A negative size means the length is unknown and
	// r is read to EOF (a multipart part); uploads always know theirs.
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	// Get opens the object under key for reading. The caller closes it.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// Delete removes the object under key; ErrNotFound if there is none.
	Delete(ctx context.Context, key string) error
	// List returns every stored key with the given prefix, sorted.
	List(ctx context.Context, prefix string) ([]string, error)
}

// SourceKey is where a run's workspace bundle is stored.
func SourceKey(runID string) string { return "sources/" + runID + ".tar" }

// JobPrefix is the key prefix of every artifact of one attempt.
func JobPrefix(runID, jobID string, attempt int) string {
	return "runs/" + runID + "/jobs/" + jobID + "/" + strconv.Itoa(attempt) + "/"
}

// JobKey is where one artifact file of an attempt is stored. path must
// already satisfy ValidatePath.
func JobKey(runID, jobID string, attempt int, path string) string {
	return JobPrefix(runID, jobID, attempt) + path
}

// MaxPathLen bounds an artifact path.
const MaxPathLen = 1024

// ValidatePath checks a user-supplied artifact path (the part of a key
// after the attempt): slash-separated, relative, no empty, "." or ".."
// segments, no backslashes or control characters. It is what stops a
// runner-supplied name from escaping the store root or an attempt's
// prefix.
func ValidatePath(p string) error {
	if p == "" {
		return errors.New("artifact: empty path")
	}
	if len(p) > MaxPathLen {
		return fmt.Errorf("artifact: path longer than %d bytes", MaxPathLen)
	}
	for _, c := range p {
		if c < 0x20 || c == 0x7f || c == '\\' {
			return fmt.Errorf("artifact: path %q contains a forbidden character", p)
		}
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("artifact: path %q is absolute", p)
	}
	for _, seg := range strings.Split(p, "/") {
		switch seg {
		case "":
			return fmt.Errorf("artifact: path %q has an empty segment", p)
		case ".", "..":
			return fmt.Errorf("artifact: path %q has a %q segment", p, seg)
		}
	}
	return nil
}

// ValidateKey checks a full store key. The rules are those of
// ValidatePath: every key is a relative slash path with plain segments.
func ValidateKey(key string) error {
	if err := ValidatePath(key); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}
	return nil
}
