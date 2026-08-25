package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"quarry/internal/scheduler"
	"quarry/internal/store"
)

// Runner protocol (docs/protocol.md): /api/runner/register, /claim,
// /heartbeat and /jobs/{id}/complete. Handlers decode, call the store or
// scheduler, encode; the fencing and advancement rules live in
// internal/scheduler.

// MaxRunnerBodyBytes bounds a runner request body.
const MaxRunnerBodyBytes = 64 << 10

type claimRequest struct {
	RunnerID string            `json:"runner_id"`
	Name     string            `json:"name"`
	Labels   map[string]string `json:"labels"`
	Capacity int               `json:"capacity"`
	Version  string            `json:"version"`
}

type claimedJobJSON struct {
	jobJSON
	LeaseTTLMillis int64 `json:"lease_ttl_ms"`
}

type heartbeatRequest struct {
	RunnerID string       `json:"runner_id"`
	Jobs     []jobRefJSON `json:"jobs"`
}

type jobRefJSON struct {
	JobID   string `json:"job_id"`
	Attempt int    `json:"attempt"`
}

type directiveJSON struct {
	JobID     string `json:"job_id"`
	Attempt   int    `json:"attempt"`
	Directive string `json:"directive"`
}

type completeRequest struct {
	RunnerID    string `json:"runner_id"`
	Attempt     int    `json:"attempt"`
	Status      string `json:"status"`
	FailureKind string `json:"failure_kind"`
	ExitCode    *int   `json:"exit_code"`
	Error       string `json:"error"`
}

// decodeRunnerBody reads a bounded JSON body into v, writing 400/413 and
// returning false on failure.
func (s *Server) decodeRunnerBody(w http.ResponseWriter, r *http.Request, v any) bool {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxRunnerBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, r, http.StatusRequestEntityTooLarge, "body too large")
			return false
		}
		writeError(w, r, http.StatusBadRequest, "read body: "+err.Error())
		return false
	}
	if err := json.Unmarshal(raw, v); err != nil {
		writeError(w, r, http.StatusBadRequest, "decode body: "+err.Error())
		return false
	}
	return true
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req claimRequest
	if !s.decodeRunnerBody(w, r, &req) {
		return
	}
	if req.RunnerID == "" {
		writeError(w, r, http.StatusBadRequest, "runner_id is required")
		return
	}
	job, err := s.sched.Claim(r.Context(), scheduler.RunnerInfo{
		ID: req.RunnerID, Name: req.Name, Labels: req.Labels, Capacity: req.Capacity, Version: req.Version,
	})
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if job == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": claimedJobJSON{
		jobJSON: toJobJSON(job), LeaseTTLMillis: s.sched.LeaseTTL().Milliseconds(),
	}})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req heartbeatRequest
	if !s.decodeRunnerBody(w, r, &req) {
		return
	}
	if req.RunnerID == "" {
		writeError(w, r, http.StatusBadRequest, "runner_id is required")
		return
	}
	refs := make([]scheduler.JobRef, 0, len(req.Jobs))
	for _, j := range req.Jobs {
		refs = append(refs, scheduler.JobRef{JobID: j.JobID, Attempt: j.Attempt})
	}
	ds, err := s.sched.Heartbeat(r.Context(), req.RunnerID, refs)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]directiveJSON, 0, len(ds))
	for _, d := range ds {
		out = append(out, directiveJSON{JobID: d.JobID, Attempt: d.Attempt, Directive: d.Directive})
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	var req completeRequest
	if !s.decodeRunnerBody(w, r, &req) {
		return
	}
	if req.RunnerID == "" || req.Attempt <= 0 {
		writeError(w, r, http.StatusBadRequest, "runner_id and a positive attempt are required")
		return
	}
	job, err := s.sched.Complete(r.Context(), r.PathValue("id"), req.Attempt, scheduler.Result{
		Status: req.Status, FailureKind: req.FailureKind, ExitCode: req.ExitCode, Error: req.Error,
	})
	switch {
	case errors.Is(err, scheduler.ErrFenced):
		writeError(w, r, http.StatusConflict, "attempt is not the running attempt")
		return
	case errors.Is(err, store.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "job not found")
		return
	case err != nil && isBadResult(err):
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	case err != nil:
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toJobJSON(job))
}

// isBadResult reports whether a Complete error is the scheduler rejecting
// the caller's status/failure_kind (a 400) rather than a store failure.
func isBadResult(err error) bool {
	var e *scheduler.InvalidResultError
	return errors.As(err, &e)
}

type registerRequest struct {
	Name     string            `json:"name"`
	Labels   map[string]string `json:"labels"`
	Capacity int               `json:"capacity"`
}

// handleRegister resolves a runner name to its runner_id, minting one on
// first sight. Re-registering the same name is idempotent: the id is
// reused and labels and capacity are refreshed; version is not part of
// the body (claim reports it), so an existing row keeps its value. The lookup and
// the upsert share one transaction, and runners.name is unique, so two
// concurrent first registrations of one name cannot both mint an id.
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if !s.decodeRunnerBody(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, r, http.StatusBadRequest, "name is required")
		return
	}
	if req.Capacity < 0 {
		writeError(w, r, http.StatusBadRequest, "capacity must not be negative")
		return
	}
	runner := &store.Runner{Name: req.Name, Labels: req.Labels, Capacity: req.Capacity}
	err := s.st.Tx(r.Context(), func(tx *sql.Tx) error {
		existing, err := s.st.GetRunnerByName(r.Context(), tx, req.Name)
		switch {
		case err == nil:
			runner.ID, runner.Version = existing.ID, existing.Version
		case errors.Is(err, store.ErrNotFound):
			runner.ID = newID(8)
		default:
			return err
		}
		return s.st.UpsertRunner(r.Context(), tx, runner)
	})
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"runner_id": runner.ID})
}
