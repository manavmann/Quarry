package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Wire types mirror internal/api's JSON. The CLI never links api or store:
// it is a client of the HTTP surface and nothing else.

// Run is one row of GET /api/runs and the "run" of GET /api/runs/{id}.
type Run struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	Trigger    string `json:"trigger"`
	CreatedAt  int64  `json:"created_at"`
	StartedAt  int64  `json:"started_at"`
	FinishedAt int64  `json:"finished_at"`
}

// Job is GET /api/jobs/{id} and each entry of a run detail's "jobs".
type Job struct {
	ID          string          `json:"id"`
	RunID       string          `json:"run_id"`
	Name        string          `json:"name"`
	State       string          `json:"state"`
	Attempt     int             `json:"attempt"`
	MaxAttempts int             `json:"max_attempts"`
	RunnerID    string          `json:"runner_id"`
	FailureKind string          `json:"failure_kind"`
	ExitCode    *int            `json:"exit_code"`
	Error       string          `json:"error"`
	QueuedAt    int64           `json:"queued_at"`
	StartedAt   int64           `json:"started_at"`
	FinishedAt  int64           `json:"finished_at"`
	Spec        json.RawMessage `json:"spec"`
}

// RunDetail is GET /api/runs/{id} and the POST /api/runs response.
type RunDetail struct {
	Run  Run   `json:"run"`
	Jobs []Job `json:"jobs"`
}

// Event is one row of GET /api/runs/{id}/events.
type Event struct {
	ID        int64           `json:"id"`
	RunID     string          `json:"run_id"`
	JobID     string          `json:"job_id"`
	Type      string          `json:"type"`
	Detail    json.RawMessage `json:"detail"`
	CreatedAt int64           `json:"created_at"`
}

// Runner is one row of GET /api/runners.
type Runner struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Labels     map[string]string `json:"labels"`
	Capacity   int               `json:"capacity"`
	State      string            `json:"state"`
	Version    string            `json:"version"`
	LastSeenAt int64             `json:"last_seen_at"`
}

// LogChunk is one chunk of GET /api/jobs/{id}/logs.
type LogChunk struct {
	Seq  int64  `json:"seq"`
	Data []byte `json:"data"`
}

// Logs is the GET /api/jobs/{id}/logs body; Next is the cursor to pass as
// after= on the following read.
type Logs struct {
	Attempt int        `json:"attempt"`
	Chunks  []LogChunk `json:"chunks"`
	Next    int64      `json:"next"`
}

// Terminal states, as the server reports them.
func runTerminal(state string) bool {
	return state == "succeeded" || state == "failed" || state == "cancelled"
}

func jobTerminal(state string) bool {
	return state == "succeeded" || state == "failed" || state == "cancelled" || state == "skipped"
}

// APIError is a non-2xx response the server explained.
type APIError struct {
	Status    int
	Message   string
	RequestID string
}

func (e *APIError) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("server: %s (HTTP %d, request %s)", e.Message, e.Status, e.RequestID)
	}
	return fmt.Sprintf("server: %s (HTTP %d)", e.Message, e.Status)
}

// Client talks to the control plane. Every call retries transient
// failures (connection errors, 502/503/504) with capped exponential
// backoff; 4xx and other 5xx are final.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
	// Retries is the number of attempts after the first; Backoff the first
	// delay, doubled per attempt and capped at 8×.
	Retries int
	Backoff time.Duration
}

// NewClient builds a Client with the default retry policy.
func NewClient(baseURL, token string) *Client {
	return &Client{BaseURL: baseURL, Token: token, HTTP: &http.Client{Timeout: 60 * time.Second},
		Retries: 5, Backoff: 200 * time.Millisecond}
}

// do performs one request per attempt; body, when non-nil, is called per
// attempt so retries send a fresh body. out receives the decoded 2xx JSON.
func (c *Client) do(ctx context.Context, method, path string, body func() (io.ReadCloser, string, error), out any) error {
	delay := c.Backoff
	for attempt := 0; ; attempt++ {
		err := c.once(ctx, method, path, body, out)
		if err == nil || attempt >= c.Retries || !transient(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay < 8*c.Backoff {
			delay *= 2
		}
	}
}

func (c *Client) once(ctx context.Context, method, path string, body func() (io.ReadCloser, string, error), out any) error {
	var rc io.ReadCloser
	var ct string
	if body != nil {
		var err error
		if rc, ct, err = body(); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rc)
	if err != nil {
		if rc != nil {
			rc.Close()
		}
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		e := &APIError{Status: resp.StatusCode, Message: http.StatusText(resp.StatusCode)}
		var msg struct {
			Error     string `json:"error"`
			RequestID string `json:"request_id"`
		}
		if json.Unmarshal(raw, &msg) == nil && msg.Error != "" {
			e.Message, e.RequestID = msg.Error, msg.RequestID
		}
		return e
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode %s %s: %w", method, path, err)
	}
	return nil
}

// transient reports whether an error is worth retrying: anything from the
// transport (refused, reset, timeout) and a gateway-style 5xx.
func transient(err error) bool {
	var api *APIError
	if errors.As(err, &api) {
		return api.Status == http.StatusBadGateway || api.Status == http.StatusServiceUnavailable || api.Status == http.StatusGatewayTimeout
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var urlErr *url.Error
	return errors.As(err, &urlErr)
}

// ---- endpoints -----------------------------------------------------------

// Submit posts a multipart run: the pipeline document and the workspace
// bundle. bundle is called per attempt and must return a fresh reader.
func (c *Client) Submit(ctx context.Context, body func() (io.ReadCloser, string, error)) (*RunDetail, error) {
	var d RunDetail
	if err := c.do(ctx, http.MethodPost, "/api/runs", body, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (c *Client) ListRuns(ctx context.Context, limit int) ([]Run, error) {
	var out struct {
		Runs []Run `json:"runs"`
	}
	err := c.do(ctx, http.MethodGet, "/api/runs?limit="+strconv.Itoa(limit), nil, &out)
	return out.Runs, err
}

func (c *Client) GetRun(ctx context.Context, id string) (*RunDetail, error) {
	var d RunDetail
	if err := c.do(ctx, http.MethodGet, "/api/runs/"+url.PathEscape(id), nil, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (c *Client) GetJob(ctx context.Context, id string) (*Job, error) {
	var j Job
	if err := c.do(ctx, http.MethodGet, "/api/jobs/"+url.PathEscape(id), nil, &j); err != nil {
		return nil, err
	}
	return &j, nil
}

func (c *Client) GetLogs(ctx context.Context, jobID string, attempt int, after int64) (*Logs, error) {
	q := url.Values{"after": {strconv.FormatInt(after, 10)}}
	if attempt > 0 {
		q.Set("attempt", strconv.Itoa(attempt))
	}
	var l Logs
	if err := c.do(ctx, http.MethodGet, "/api/jobs/"+url.PathEscape(jobID)+"/logs?"+q.Encode(), nil, &l); err != nil {
		return nil, err
	}
	return &l, nil
}

func (c *Client) ListEvents(ctx context.Context, runID string, after int64) ([]Event, error) {
	var out struct {
		Events []Event `json:"events"`
	}
	err := c.do(ctx, http.MethodGet, "/api/runs/"+url.PathEscape(runID)+"/events?after="+strconv.FormatInt(after, 10), nil, &out)
	return out.Events, err
}

func (c *Client) ListRunners(ctx context.Context) ([]Runner, error) {
	var out struct {
		Runners []Runner `json:"runners"`
	}
	err := c.do(ctx, http.MethodGet, "/api/runners", nil, &out)
	return out.Runners, err
}

// Cancel asks the server to cancel a run. The endpoint is not served yet
// (cancel is reserved in the protocol); until it lands this is a 404.
func (c *Client) Cancel(ctx context.Context, runID string) error {
	return c.do(ctx, http.MethodPost, "/api/runs/"+url.PathEscape(runID)+"/cancel", nil, nil)
}

// Artifact is one row of GET /api/jobs/{id}/artifacts.
type Artifact struct {
	Path        string `json:"path"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type"`
	CreatedAt   int64  `json:"created_at"`
}

// ListArtifacts returns a job's artifacts; attempt 0 means the current one.
func (c *Client) ListArtifacts(ctx context.Context, jobID string, attempt int) ([]Artifact, error) {
	q := url.Values{}
	if attempt > 0 {
		q.Set("attempt", strconv.Itoa(attempt))
	}
	var out struct {
		Artifacts []Artifact `json:"artifacts"`
	}
	path := "/api/jobs/" + url.PathEscape(jobID) + "/artifacts"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	err := c.do(ctx, http.MethodGet, path, nil, &out)
	return out.Artifacts, err
}

// DownloadArtifact streams one artifact into w and returns the sha256 of
// what was written, computed as it streams. It is a single request: a
// partial write is not retried, the caller sees the error.
func (c *Client) DownloadArtifact(ctx context.Context, jobID string, attempt int, path string, w io.Writer) (string, error) {
	segs := strings.Split(path, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	p := "/api/jobs/" + url.PathEscape(jobID) + "/artifacts/" + strings.Join(segs, "/")
	if attempt > 0 {
		p += "?attempt=" + strconv.Itoa(attempt)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+p, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		e := &APIError{Status: resp.StatusCode, Message: http.StatusText(resp.StatusCode)}
		var msg struct {
			Error     string `json:"error"`
			RequestID string `json:"request_id"`
		}
		if json.Unmarshal(raw, &msg) == nil && msg.Error != "" {
			e.Message, e.RequestID = msg.Error, msg.RequestID
		}
		return "", e
	}
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(w, h), resp.Body); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
