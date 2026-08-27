package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"quarry/internal/logship"
	"quarry/internal/pipeline"
)

// The wire types below mirror docs/protocol.md. They are declared here
// rather than imported from internal/api so the runner binary never links
// the control plane (or its SQLite driver): the agent's only dependency on
// the server is this JSON.

type registerRequest struct {
	Name     string            `json:"name"`
	Labels   map[string]string `json:"labels"`
	Capacity int               `json:"capacity"`
}

type claimRequest struct {
	RunnerID string            `json:"runner_id"`
	Name     string            `json:"name"`
	Labels   map[string]string `json:"labels"`
	Capacity int               `json:"capacity"`
	Version  string            `json:"version"`
}

type claimedJob struct {
	ID             string          `json:"id"`
	RunID          string          `json:"run_id"`
	Name           string          `json:"name"`
	Attempt        int             `json:"attempt"`
	LeaseExpiresAt int64           `json:"lease_expires_at"`
	LeaseTTLMillis int64           `json:"lease_ttl_ms"`
	Spec           json.RawMessage `json:"spec"`
}

// spec decodes the job's pipeline spec, which the server stores as the
// JSON encoding of a pipeline.Job.
func (j *claimedJob) spec() (pipeline.Job, error) {
	var pj pipeline.Job
	if err := json.Unmarshal(j.Spec, &pj); err != nil {
		return pj, fmt.Errorf("decode spec of job %s: %w", j.ID, err)
	}
	return pj, nil
}

type jobRef struct {
	JobID   string `json:"job_id"`
	Attempt int    `json:"attempt"`
}

type heartbeatRequest struct {
	RunnerID string   `json:"runner_id"`
	Jobs     []jobRef `json:"jobs"`
}

type directive struct {
	JobID     string `json:"job_id"`
	Attempt   int    `json:"attempt"`
	Directive string `json:"directive"`
}

type completeRequest struct {
	RunnerID    string `json:"runner_id"`
	Attempt     int    `json:"attempt"`
	Status      string `json:"status"`
	FailureKind string `json:"failure_kind,omitempty"`
	ExitCode    *int   `json:"exit_code,omitempty"`
	Error       string `json:"error,omitempty"`
}

type logChunk struct {
	Seq  int64  `json:"seq"`
	Data []byte `json:"data"`
}

type logsRequest struct {
	RunnerID string     `json:"runner_id"`
	Attempt  int        `json:"attempt"`
	Chunks   []logChunk `json:"chunks"`
}

// statusError is a non-2xx reply. Retryable reports whether the runner
// may resend the same request: server errors yes, client errors no.
type statusError struct {
	Code int
	Body string
}

func (e *statusError) Error() string { return fmt.Sprintf("server replied %d: %s", e.Code, e.Body) }

func (e *statusError) Retryable() bool { return e.Code >= 500 }

// errFenced is a 409 on complete: the attempt is no longer ours.
var errFenced = errors.New("agent: attempt is not the running attempt (409)")

// client is the agent's HTTP face. Every method is one request.
type client struct {
	base  string
	token string
	http  *http.Client
}

// post sends v as JSON and decodes a 2xx body into out (when non-nil).
// A 204 leaves out untouched and returns (false, nil).
func (c *client) post(ctx context.Context, path string, v, out any) (bool, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return false, err
	}
	switch {
	case resp.StatusCode == http.StatusNoContent:
		return false, nil
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return false, &statusError{Code: resp.StatusCode, Body: string(bytes.TrimSpace(raw))}
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return false, fmt.Errorf("decode %s reply: %w", path, err)
		}
	}
	return true, nil
}

// register resolves the runner name to its runner_id, minting one on
// first sight. It is idempotent per name.
func (c *client) register(ctx context.Context, req registerRequest) (string, error) {
	var reply struct {
		RunnerID string `json:"runner_id"`
	}
	if _, err := c.post(ctx, "/api/runner/register", req, &reply); err != nil {
		return "", err
	}
	if reply.RunnerID == "" {
		return "", errors.New("register: server returned no runner_id")
	}
	return reply.RunnerID, nil
}

// claim asks for one job; nil means nothing was claimable.
func (c *client) claim(ctx context.Context, req claimRequest) (*claimedJob, error) {
	var reply struct {
		Job *claimedJob `json:"job"`
	}
	ok, err := c.post(ctx, "/api/runner/claim", req, &reply)
	if err != nil || !ok {
		return nil, err
	}
	return reply.Job, nil
}

func (c *client) heartbeat(ctx context.Context, runnerID string, refs []jobRef) ([]directive, error) {
	var reply struct {
		Jobs []directive `json:"jobs"`
	}
	if _, err := c.post(ctx, "/api/runner/heartbeat", heartbeatRequest{RunnerID: runnerID, Jobs: refs}, &reply); err != nil {
		return nil, err
	}
	return reply.Jobs, nil
}

// complete reports one attempt's result. It maps 409 to errFenced; other
// non-2xx replies come back as *statusError.
func (c *client) complete(ctx context.Context, jobID string, req completeRequest) error {
	_, err := c.post(ctx, "/api/runner/jobs/"+jobID+"/complete", req, nil)
	var se *statusError
	if errors.As(err, &se) && se.Code == http.StatusConflict {
		return errFenced
	}
	return err
}

// logs ships one batch of chunks for an attempt. It maps the reply onto
// the shipper's contract: 409 → logship.ErrStale, any other 4xx →
// logship.ErrRejected, transport errors and 5xx as they are (retried).
func (c *client) logs(ctx context.Context, jobID string, req logsRequest) error {
	_, err := c.post(ctx, "/api/runner/jobs/"+jobID+"/logs", req, nil)
	var se *statusError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &se) && se.Code == http.StatusConflict:
		return logship.ErrStale
	case errors.As(err, &se) && !se.Retryable():
		return fmt.Errorf("%w: %v", logship.ErrRejected, err)
	}
	return err
}

// completeWithRetry resends complete on transport errors and 5xx replies
// with exponential backoff, up to attempts tries. Any other outcome is
// final: the server either accepted the result or will never accept it.
func (c *client) completeWithRetry(ctx context.Context, jobID string, req completeRequest, attempts int, backoff time.Duration) error {
	var err error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
			if backoff *= 2; backoff > maxCompleteBackoff {
				backoff = maxCompleteBackoff
			}
		}
		err = c.complete(ctx, jobID, req)
		if err == nil || errors.Is(err, errFenced) {
			return err
		}
		var se *statusError
		if errors.As(err, &se) && !se.Retryable() {
			return err
		}
		if ctx.Err() != nil {
			return err
		}
	}
	return fmt.Errorf("complete job %s: giving up after %d tries: %w", jobID, attempts, err)
}

// SourceFetcher returns a function that streams a run's source bundle
// from GET /api/runs/{id}/source as a tar, for the Docker executor. Until
// the server serves bundles (C10) the endpoint does not exist; a 404 is
// reported as "no bundle" (nil, nil) so jobs run in an empty workspace.
func SourceFetcher(cfg Config) func(ctx context.Context, runID string) (io.ReadCloser, error) {
	// Bundles can be large: no client timeout, the job context bounds it.
	c := &client{base: cfg.ServerURL, token: cfg.Token, http: &http.Client{}}
	return c.source
}

// source is one GET of the run's bundle. The caller closes the body.
func (c *client) source(ctx context.Context, runID string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/api/runs/"+runID+"/source", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		return resp.Body, nil
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return nil, nil
	default:
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return nil, &statusError{Code: resp.StatusCode, Body: string(bytes.TrimSpace(raw))}
	}
}
