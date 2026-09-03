// Package remote is the artifact.Store over the replicated object store's
// coordinator HTTP API: one bucket, one object per key. Put spools the
// reader to a temporary file first (as local does) and then streams that
// file to the coordinator with Content-Length, so a reader that fails or
// lies about its size stores nothing, and a PUT that the cluster rejects
// for want of replicas can be retried from the spool without holding the
// object in memory. The coordinator commits an object only after write
// quorum, so a replaced key is never seen half-written.
//
// Transport errors and 5xx replies (the coordinator's InsufficientReplicas,
// NoHealthyReplica and TooManyUploads are 503) are retried with capped
// exponential backoff; 4xx replies are not. Every reply that is not 2xx is
// decoded as {"error":{"code","message"}} into an *Error.
package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"quarry/internal/artifact"
)

// Config is a Store's settings; zero values take the defaults below.
type Config struct {
	// BaseURL is the coordinator's address, e.g. http://storage:8080.
	BaseURL string
	// Bucket holds every object; ensured by New (default quarry-artifacts).
	Bucket string
	// Token, when set, is sent as a bearer token on every request.
	Token string
	// SpoolDir holds Put's temporary files (os.TempDir when empty).
	SpoolDir string
	// RequestTimeout bounds each bucket-ensure, DELETE and list request
	// and the wait for a GET's headers (default 30s).
	RequestTimeout time.Duration
	// TransferTimeout bounds a whole Put, retries included (default 10m).
	TransferTimeout time.Duration
	// Attempts is how many times a request is tried (default 5).
	Attempts int
	// Backoff is the first retry delay; it doubles up to MaxBackoff
	// (defaults 200ms and 5s).
	Backoff, MaxBackoff time.Duration
}

const (
	defaultBucket          = "quarry-artifacts"
	defaultRequestTimeout  = 30 * time.Second
	defaultTransferTimeout = 10 * time.Minute
	defaultAttempts        = 5
	defaultBackoff         = 200 * time.Millisecond
	defaultMaxBackoff      = 5 * time.Second
	spoolPrefix            = ".quarry-put-"
)

func (c Config) withDefaults() Config {
	if c.Bucket == "" {
		c.Bucket = defaultBucket
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = defaultRequestTimeout
	}
	if c.TransferTimeout <= 0 {
		c.TransferTimeout = defaultTransferTimeout
	}
	if c.Attempts <= 0 {
		c.Attempts = defaultAttempts
	}
	if c.Backoff <= 0 {
		c.Backoff = defaultBackoff
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = defaultMaxBackoff
	}
	return c
}

// Error is a non-2xx reply from the coordinator, decoded from its JSON
// body when it has one.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("coordinator replied %d", e.Status)
	}
	return fmt.Sprintf("coordinator replied %d %s: %s", e.Status, e.Code, e.Message)
}

// Store is an artifact.Store backed by one bucket of the coordinator.
type Store struct {
	cfg  Config
	base string // BaseURL without a trailing slash
	http *http.Client
}

// New checks the config, ensures the bucket exists (a 409
// BucketAlreadyExists is success) and returns the Store.
func New(ctx context.Context, cfg Config) (*Store, error) {
	cfg = cfg.withDefaults()
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("remote: base url %q is not an http(s) url", cfg.BaseURL)
	}
	s := &Store{
		cfg:  cfg,
		base: strings.TrimRight(cfg.BaseURL, "/"),
		http: &http.Client{Transport: http.DefaultTransport},
	}
	if err := s.ensureBucket(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// Describe names the backend for the server's startup log line.
func (s *Store) Describe() string { return s.bucketURL() }

func (s *Store) ensureBucket(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	resp, err := s.retry(ctx, func() (*http.Response, error) {
		req, err := s.newRequest(ctx, http.MethodPut, s.bucketURL(), nil)
		if err != nil {
			return nil, err
		}
		return s.http.Do(req)
	})
	if err != nil {
		var e *Error
		if errors.As(err, &e) && e.Status == http.StatusConflict {
			return nil
		}
		return fmt.Errorf("remote: ensure bucket %s: %w", s.cfg.Bucket, err)
	}
	drain(resp)
	return nil
}

func (s *Store) bucketURL() string { return s.base + "/v1/" + url.PathEscape(s.cfg.Bucket) }

// objectURL maps a validated key onto the coordinator; each segment is
// escaped on its own so the slashes survive.
func (s *Store) objectURL(key string) (string, error) {
	if err := artifact.ValidateKey(key); err != nil {
		return "", err
	}
	segs := strings.Split(key, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	return s.bucketURL() + "/" + strings.Join(segs, "/"), nil
}

func (s *Store) newRequest(ctx context.Context, method, u string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if s.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.Token)
	}
	return req, nil
}

// Put implements artifact.Store.
func (s *Store) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	u, err := s.objectURL(key)
	if err != nil {
		return err
	}
	spool, err := os.CreateTemp(s.cfg.SpoolDir, spoolPrefix+"*")
	if err != nil {
		return fmt.Errorf("remote: put %s: %w", key, err)
	}
	defer func() {
		spool.Close()
		os.Remove(spool.Name())
	}()
	var src io.Reader = readerCtx{ctx, r}
	if size >= 0 {
		src = io.LimitReader(src, size+1) // one extra byte detects a long reader
	}
	n, err := io.Copy(spool, src)
	if err != nil {
		return fmt.Errorf("remote: put %s: %w", key, err)
	}
	if size >= 0 && n != size {
		return fmt.Errorf("remote: put %s: got %d bytes, want %d", key, n, size)
	}
	size = n
	ctx, cancel := context.WithTimeout(ctx, s.cfg.TransferTimeout)
	defer cancel()
	resp, err := s.retry(ctx, func() (*http.Response, error) {
		if _, err := spool.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		// The body is wrapped so net/http cannot sniff a length of its
		// own; ContentLength is set explicitly and the coordinator never
		// sees a chunked upload.
		req, err := s.newRequest(ctx, http.MethodPut, u, io.NopCloser(io.LimitReader(spool, size)))
		if err != nil {
			return nil, err
		}
		req.ContentLength = size
		req.Header.Set("Content-Type", "application/octet-stream")
		return s.http.Do(req)
	})
	if err != nil {
		return fmt.Errorf("remote: put %s: %w", key, err)
	}
	drain(resp)
	return nil
}

// Get implements artifact.Store. The request timeout covers one attempt's
// wait for headers (a slow attempt is cancelled and retried); the body is
// read at the caller's pace and closing it releases the connection.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	u, err := s.objectURL(key)
	if err != nil {
		return nil, err
	}
	var cancel context.CancelFunc = func() {}
	resp, err := s.retry(ctx, func() (*http.Response, error) {
		cancel() // the previous attempt's context
		var actx context.Context
		actx, cancel = context.WithCancel(ctx)
		req, err := s.newRequest(actx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		headers := time.AfterFunc(s.cfg.RequestTimeout, cancel)
		defer headers.Stop()
		return s.http.Do(req)
	})
	if err != nil {
		cancel()
		if isNotFound(err) {
			return nil, artifact.ErrNotFound
		}
		return nil, fmt.Errorf("remote: get %s: %w", key, err)
	}
	return body{resp.Body, cancel}, nil
}

// Delete implements artifact.Store. The coordinator's DELETE is
// idempotent (204 for a key it never had), so a HEAD first is what makes
// a missing key ErrNotFound as it is on the local backend.
func (s *Store) Delete(ctx context.Context, key string) error {
	u, err := s.objectURL(key)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	for _, method := range []string{http.MethodHead, http.MethodDelete} {
		resp, err := s.retry(ctx, func() (*http.Response, error) {
			req, err := s.newRequest(ctx, method, u, nil)
			if err != nil {
				return nil, err
			}
			return s.http.Do(req)
		})
		if err != nil {
			if isNotFound(err) {
				return artifact.ErrNotFound
			}
			return fmt.Errorf("remote: delete %s: %w", key, err)
		}
		drain(resp)
	}
	return nil
}

// listPage is the coordinator's listing reply.
type listPage struct {
	Objects []struct {
		Key string `json:"key"`
	} `json:"objects"`
	Truncated      bool   `json:"truncated"`
	NextStartAfter string `json:"next_start_after"`
}

// List implements artifact.Store. Pages are followed until the
// coordinator reports no more; the prefix is a plain string prefix on keys.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	if strings.Contains(prefix, "\\") || strings.Contains(prefix, "..") {
		return nil, errors.New("remote: invalid prefix")
	}
	var keys []string
	startAfter := ""
	for {
		q := url.Values{"prefix": {prefix}}
		if startAfter != "" {
			q.Set("start_after", startAfter)
		}
		page, err := s.listPage(ctx, s.bucketURL()+"/?"+q.Encode())
		if err != nil {
			return nil, fmt.Errorf("remote: list %s: %w", prefix, err)
		}
		for _, o := range page.Objects {
			if strings.HasPrefix(o.Key, prefix) {
				keys = append(keys, o.Key)
			}
		}
		if !page.Truncated || page.NextStartAfter == "" {
			break
		}
		startAfter = page.NextStartAfter
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *Store) listPage(ctx context.Context, u string) (*listPage, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	resp, err := s.retry(ctx, func() (*http.Response, error) {
		req, err := s.newRequest(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		return s.http.Do(req)
	})
	if err != nil {
		return nil, err
	}
	defer drain(resp)
	var page listPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, fmt.Errorf("decoding listing: %w", err)
	}
	return &page, nil
}

// retry runs do until it returns a 2xx reply, a 4xx *Error, ctx ends or
// the attempts are spent. Transport errors and 5xx replies back off and
// try again; the last error is returned.
func (s *Store) retry(ctx context.Context, do func() (*http.Response, error)) (*http.Response, error) {
	delay := s.cfg.Backoff
	var last error
	for attempt := 1; ; attempt++ {
		resp, err := do()
		if err == nil {
			if resp.StatusCode/100 == 2 {
				return resp, nil
			}
			err = decodeError(resp)
			var e *Error
			if errors.As(err, &e) && e.Status < 500 {
				return nil, err
			}
		}
		last = err
		if ctx.Err() != nil {
			return nil, errors.Join(ctx.Err(), last)
		}
		if attempt >= s.cfg.Attempts {
			return nil, fmt.Errorf("after %d attempts: %w", attempt, last)
		}
		select {
		case <-ctx.Done():
			return nil, errors.Join(ctx.Err(), last)
		case <-time.After(delay):
		}
		delay = min(delay*2, s.cfg.MaxBackoff)
	}
}

// decodeError turns a non-2xx reply into an *Error, reading the JSON body
// when there is one and closing it.
func decodeError(resp *http.Response) error {
	defer drain(resp)
	e := &Error{Status: resp.StatusCode}
	var wire struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&wire); err == nil {
		e.Code, e.Message = wire.Error.Code, wire.Error.Message
	}
	return e
}

func isNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound
}

// drain discards and closes a reply body so the connection is reused.
func drain(resp *http.Response) {
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
}

// body is a GET's reply body; closing it also ends the request context.
type body struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b body) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

// readerCtx stops a copy once ctx is done, so a Put whose request went
// away does not keep spooling.
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
