// Package api is the control plane's HTTP surface. Handlers translate
// HTTP into store calls and store rows into JSON; they hold no business
// logic. This file covers the user endpoints (blueprint §10); runner.go
// covers the runner protocol under /api/runner/* (docs/protocol.md).
package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"quarry/internal/artifact"
	"quarry/internal/pipeline"
	"quarry/internal/scheduler"
	"quarry/internal/store"
)

// MaxPipelineBytes bounds a .quarry.yml document; larger ones get 413.
const MaxPipelineBytes = 1 << 20

// MaxSourceBytes bounds the workspace bundle of a multipart POST /api/runs.
const MaxSourceBytes = 256 << 20

// Config is everything the API needs from its caller.
type Config struct {
	// APIToken is the bearer token every /api/* request must present.
	APIToken string
	// Logger receives one line per request; nil means log.Default().
	Logger *log.Logger
	// Scheduler tunes the runner protocol (lease TTL, max attempts, monitor).
	Scheduler scheduler.Config
	// Artifacts holds source bundles and job artifacts. Required: the
	// server is its only writer.
	Artifacts artifact.Store
}

// Server serves the user API over a *store.Store.
type Server struct {
	st        *store.Store
	sched     *scheduler.Scheduler
	artifacts artifact.Store
	cfg       Config
	mux       *http.ServeMux
}

// New builds a Server whose routes are all registered on a fresh mux. It
// panics without an artifact store: there is no meaningful fallback.
func New(st *store.Store, cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.Artifacts == nil {
		panic("api: Config.Artifacts is required")
	}
	s := &Server{st: st, sched: scheduler.New(st, cfg.Scheduler), artifacts: cfg.Artifacts, cfg: cfg, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)

	api := http.NewServeMux()
	api.HandleFunc("POST /api/runs", s.handleSubmitRun)
	api.HandleFunc("GET /api/runs", s.handleListRuns)
	api.HandleFunc("GET /api/runs/{id}", s.handleGetRun)
	api.HandleFunc("POST /api/runs/{id}/cancel", s.handleCancelRun)
	api.HandleFunc("GET /api/runs/{id}/events", s.handleListEvents)
	api.HandleFunc("GET /api/runs/{id}/source", s.handleGetSource)
	api.HandleFunc("GET /api/jobs/{id}", s.handleGetJob)
	api.HandleFunc("GET /api/jobs/{id}/logs", s.handleGetLogs)
	api.HandleFunc("GET /api/jobs/{id}/artifacts", s.handleListArtifacts)
	api.HandleFunc("GET /api/jobs/{id}/artifacts/{path...}", s.handleGetArtifact)
	api.HandleFunc("GET /api/runners", s.handleListRunners)
	api.HandleFunc("POST /api/runner/register", s.handleRegister)
	api.HandleFunc("POST /api/runner/claim", s.handleClaim)
	api.HandleFunc("POST /api/runner/heartbeat", s.handleHeartbeat)
	api.HandleFunc("POST /api/runner/jobs/{id}/complete", s.handleComplete)
	api.HandleFunc("POST /api/runner/jobs/{id}/logs", s.handleAppendLogs)
	api.HandleFunc("POST /api/runner/jobs/{id}/attempts/{attempt}/artifacts/{path...}", s.handleUploadArtifact)
	s.mux.Handle("/api/", s.requireToken(api))
	return s
}

// RunMonitor runs the scheduler's lease monitor until ctx ends. The
// caller owns the goroutine; the server does not start it itself so a
// test can drive ticks at its own pace.
func (s *Server) RunMonitor(ctx context.Context) {
	s.sched.RunMonitor(ctx, s.cfg.Logger)
}

// ServeHTTP tags every request with an ID, then dispatches.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.withRequestID(s.mux).ServeHTTP(w, r)
}

// ---- middleware --------------------------------------------------------

type ctxKey int

const requestIDKey ctxKey = iota

// RequestID returns the request ID attached by the middleware, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// withRequestID honours an incoming X-Request-ID, otherwise mints one, and
// echoes it on the response so clients can quote it in bug reports.
func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = newID(8)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

// requireToken rejects any request whose Authorization header is not
// exactly "Bearer <cfg.APIToken>". The comparison is constant-time.
func (s *Server) requireToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || s.cfg.APIToken == "" || subtle.ConstantTimeCompare([]byte(tok), []byte(s.cfg.APIToken)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="quarry"`)
			writeError(w, r, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---- handlers ----------------------------------------------------------

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleSubmitRun accepts a .quarry.yml document either as the raw request
// body or as the "pipeline" part of a multipart/form-data upload whose
// "source" part is the CLI's workspace bundle. The run id is minted before
// the body is read so the bundle can stream straight into the artifact
// store under sources/<run>.tar; if the submission then fails for any
// reason the bundle is deleted again.
func (s *Server) handleSubmitRun(w http.ResponseWriter, r *http.Request) {
	runID := newID(8)
	var src []byte
	var err error
	stored := false
	if ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); ct == "multipart/form-data" {
		src, stored, err = s.readMultipartSubmit(w, r, runID)
	} else {
		src, err = io.ReadAll(http.MaxBytesReader(w, r.Body, MaxPipelineBytes))
	}
	if err != nil {
		if stored {
			s.discard(artifact.SourceKey(runID))
		}
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, r, http.StatusRequestEntityTooLarge, fmt.Sprintf("pipeline exceeds %d bytes", MaxPipelineBytes))
			return
		}
		writeError(w, r, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	p, err := pipeline.Parse(src)
	if err != nil {
		if stored {
			s.discard(artifact.SourceKey(runID))
		}
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	run, jobs, err := submitRun(r.Context(), s.st, runID, p, string(src), s.sched.MaxAttempts())
	if err != nil {
		if stored {
			s.discard(artifact.SourceKey(runID))
		}
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, runDetail(run, jobs))
}

// readMultipartSubmit returns the "pipeline" part of a multipart submit
// and streams the "source" part, when present, into the artifact store
// under runID's source key; stored reports whether a bundle was written
// (even when err is non-nil, so the caller can discard it). Any other part
// is an error. The whole body is bounded by MaxPipelineBytes +
// MaxSourceBytes.
func (s *Server) readMultipartSubmit(w http.ResponseWriter, r *http.Request, runID string) (src []byte, stored bool, err error) {
	r.Body = http.MaxBytesReader(w, r.Body, MaxPipelineBytes+MaxSourceBytes)
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, false, err
	}
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, stored, err
		}
		switch part.FormName() {
		case "pipeline":
			src, err = io.ReadAll(io.LimitReader(part, MaxPipelineBytes+1))
			if err == nil && len(src) > MaxPipelineBytes {
				err = &http.MaxBytesError{Limit: MaxPipelineBytes}
			}
		case "source":
			// The part's length is unknown; Put reads it to EOF. A Put
			// error may still be the body limit (the store surfaces the
			// reader's error), which the caller maps to 413.
			err = s.artifacts.Put(r.Context(), artifact.SourceKey(runID), part, -1)
			stored = true
		default:
			err = fmt.Errorf("unexpected multipart field %q", part.FormName())
		}
		if err != nil {
			return nil, stored, err
		}
	}
	if src == nil {
		return nil, stored, errors.New(`multipart body has no "pipeline" part`)
	}
	return src, stored, nil
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, r, http.StatusBadRequest, "limit must be a non-negative integer")
			return
		}
		limit = n
	}
	runs, err := s.st.ListRuns(r.Context(), s.st.Reader(), limit)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]runJSON, 0, len(runs))
	for i := range runs {
		out = append(out, toRunJSON(&runs[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.st.GetRun(r.Context(), s.st.Reader(), id)
	if err != nil {
		s.storeError(w, r, err, "run")
		return
	}
	jobs, err := s.st.ListJobs(r.Context(), s.st.Reader(), id)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, runDetail(run, jobs))
}

// handleCancelRun cancels a run: jobs that have not started are cancelled
// now, running attempts are told to stop on their next heartbeat. 202
// with the run and its jobs as stored (the run is terminal already when
// nothing was running), 404 unknown run, 409 run already finished.
func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.sched.CancelRun(r.Context(), id)
	switch {
	case errors.Is(err, scheduler.ErrRunFinished):
		writeError(w, r, http.StatusConflict, "run is already finished")
		return
	case err != nil:
		s.storeError(w, r, err, "run")
		return
	}
	jobs, err := s.st.ListJobs(r.Context(), s.st.Reader(), id)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, runDetail(run, jobs))
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.st.GetRun(r.Context(), s.st.Reader(), id); err != nil {
		s.storeError(w, r, err, "run")
		return
	}
	var after int64
	if v := r.URL.Query().Get("after"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeError(w, r, http.StatusBadRequest, "after must be a non-negative integer")
			return
		}
		after = n
	}
	evs, err := s.st.ListEvents(r.Context(), s.st.Reader(), id, after, 0)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]eventJSON, 0, len(evs))
	for i := range evs {
		out = append(out, toEventJSON(&evs[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.st.GetJob(r.Context(), s.st.Reader(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, r, err, "job")
		return
	}
	writeJSON(w, http.StatusOK, toJobJSON(job))
}

func (s *Server) handleListRunners(w http.ResponseWriter, r *http.Request) {
	rs, err := s.st.ListRunners(r.Context(), s.st.Reader())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]runnerJSON, 0, len(rs))
	for i := range rs {
		out = append(out, toRunnerJSON(&rs[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"runners": out})
}

// ---- JSON shapes -------------------------------------------------------

type runJSON struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	Trigger    string `json:"trigger"`
	CommitSHA  string `json:"commit_sha,omitempty"`
	Ref        string `json:"ref,omitempty"`
	CreatedAt  int64  `json:"created_at"`
	StartedAt  int64  `json:"started_at,omitempty"`
	FinishedAt int64  `json:"finished_at,omitempty"`
}

type jobJSON struct {
	ID             string          `json:"id"`
	RunID          string          `json:"run_id"`
	Name           string          `json:"name"`
	State          string          `json:"state"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	RunnerID       string          `json:"runner_id,omitempty"`
	LeaseExpiresAt int64           `json:"lease_expires_at,omitempty"`
	FailureKind    string          `json:"failure_kind,omitempty"`
	ExitCode       *int            `json:"exit_code,omitempty"`
	Error          string          `json:"error,omitempty"`
	QueuedAt       int64           `json:"queued_at"`
	StartedAt      int64           `json:"started_at,omitempty"`
	FinishedAt     int64           `json:"finished_at,omitempty"`
	Spec           json.RawMessage `json:"spec"`
}

type eventJSON struct {
	ID        int64           `json:"id"`
	RunID     string          `json:"run_id"`
	JobID     string          `json:"job_id,omitempty"`
	Type      string          `json:"type"`
	Detail    json.RawMessage `json:"detail"`
	CreatedAt int64           `json:"created_at"`
}

type runnerJSON struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Labels       map[string]string `json:"labels"`
	Capacity     int               `json:"capacity"`
	State        string            `json:"state"`
	Version      string            `json:"version"`
	LastSeenAt   int64             `json:"last_seen_at"`
	RegisteredAt int64             `json:"registered_at"`
}

func toRunJSON(r *store.Run) runJSON {
	return runJSON{ID: r.ID, State: r.State, Trigger: r.Trigger, CommitSHA: r.CommitSHA, Ref: r.Ref,
		CreatedAt: r.CreatedAt, StartedAt: r.StartedAt, FinishedAt: r.FinishedAt}
}

func toJobJSON(j *store.Job) jobJSON {
	return jobJSON{ID: j.ID, RunID: j.RunID, Name: j.Name, State: j.State, Attempt: j.Attempt,
		MaxAttempts: j.MaxAttempts, RunnerID: j.RunnerID, LeaseExpiresAt: j.LeaseExpiresAt,
		FailureKind: j.FailureKind, ExitCode: j.ExitCode, Error: j.Error, QueuedAt: j.QueuedAt,
		StartedAt: j.StartedAt, FinishedAt: j.FinishedAt, Spec: json.RawMessage(j.SpecJSON)}
}

func toEventJSON(e *store.Event) eventJSON {
	return eventJSON{ID: e.ID, RunID: e.RunID, JobID: e.JobID, Type: e.Type,
		Detail: json.RawMessage(e.DetailJSON), CreatedAt: e.CreatedAt}
}

func toRunnerJSON(r *store.Runner) runnerJSON {
	labels := r.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return runnerJSON{ID: r.ID, Name: r.Name, Labels: labels, Capacity: r.Capacity, State: r.State,
		Version: r.Version, LastSeenAt: r.LastSeenAt, RegisteredAt: r.RegisteredAt}
}

// runDetail is the GET /api/runs/{id} and POST /api/runs body: the run plus
// a summary of each job.
func runDetail(run *store.Run, jobs []store.Job) map[string]any {
	js := make([]jobJSON, 0, len(jobs))
	for i := range jobs {
		js = append(js, toJobJSON(&jobs[i]))
	}
	return map[string]any{"run": toRunJSON(run), "jobs": js}
}

// ---- helpers -----------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg, "request_id": RequestID(r.Context())})
}

// storeError maps ErrNotFound to 404 and everything else to 500.
func (s *Server) storeError(w http.ResponseWriter, r *http.Request, err error, what string) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, what+" not found")
		return
	}
	s.internalError(w, r, err)
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.cfg.Logger.Printf("request %s: %s %s: %v", RequestID(r.Context()), r.Method, r.URL.Path, err)
	writeError(w, r, http.StatusInternalServerError, "internal error")
}

// newID returns 2*n hex characters from crypto/rand.
func newID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("api: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b)
}
