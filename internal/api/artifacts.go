package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"quarry/internal/artifact"
	"quarry/internal/store"
)

// Artifact endpoints (blueprint §11, docs/protocol.md). The server is the
// only writer to the artifact store: runners stream each file to
// POST /api/runner/jobs/{id}/attempts/{attempt}/artifacts/{path...}, the
// body goes straight through to Store.Put (never buffered), and only then
// is the artifacts row written — inside one transaction with the
// (state='running', attempt) fence every runner-side write carries.
// Source bundles arrive with the run (api.go) and are served to runners
// from GET /api/runs/{id}/source.

// MaxArtifactBytes bounds one uploaded file.
const MaxArtifactBytes = 1 << 30

// DefaultContentType is recorded when an upload names none.
const DefaultContentType = "application/octet-stream"

type artifactJSON struct {
	Path        string `json:"path"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type"`
	CreatedAt   int64  `json:"created_at"`
}

func toArtifactJSON(a *store.Artifact) artifactJSON {
	return artifactJSON{Path: a.Path, SizeBytes: a.SizeBytes, SHA256: a.SHA256, ContentType: a.ContentType, CreatedAt: a.CreatedAt}
}

// handleUploadArtifact stores one file of an attempt. Content-Length is
// required (411) and bounded (413); the path is validated against
// traversal (400). The fence is checked before the write, so a stale
// attempt cannot fill the store, and again in the transaction that
// records the row; an object written for an attempt that lost the fence
// in between is deleted. 201 with the row on success; a redelivery
// overwrites the object and the row.
func (s *Server) handleUploadArtifact(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	attempt, err := strconv.Atoi(r.PathValue("attempt"))
	if err != nil || attempt <= 0 {
		writeError(w, r, http.StatusBadRequest, "attempt must be a positive integer")
		return
	}
	path := r.PathValue("path")
	if err := artifact.ValidatePath(path); err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	size := r.ContentLength
	switch {
	case size < 0:
		writeError(w, r, http.StatusLengthRequired, "Content-Length is required")
		return
	case size > MaxArtifactBytes:
		writeError(w, r, http.StatusRequestEntityTooLarge, fmt.Sprintf("artifact exceeds %d bytes", MaxArtifactBytes))
		return
	}
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		ct = DefaultContentType
	}

	job, err := s.st.GetJob(r.Context(), s.st.Reader(), jobID)
	if err != nil {
		s.storeError(w, r, err, "job")
		return
	}
	if job.State != store.JobRunning || job.Attempt != attempt {
		writeError(w, r, http.StatusConflict, "attempt is not the running attempt")
		return
	}

	key := artifact.JobKey(job.RunID, jobID, attempt, path)
	h := sha256.New()
	body := io.TeeReader(http.MaxBytesReader(w, r.Body, size), h)
	if err := s.artifacts.Put(r.Context(), key, body, size); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, r, http.StatusBadRequest, "body longer than Content-Length")
			return
		}
		s.internalError(w, r, fmt.Errorf("artifact put %s: %w", key, err))
		return
	}
	row := &store.Artifact{JobID: jobID, Attempt: attempt, Path: path, SizeBytes: size,
		SHA256: hex.EncodeToString(h.Sum(nil)), ContentType: ct}
	err = s.st.Tx(r.Context(), func(tx *sql.Tx) error {
		job, err := s.st.GetJob(r.Context(), tx, jobID)
		if err != nil {
			return err
		}
		if job.State != store.JobRunning || job.Attempt != attempt {
			return errFenced
		}
		return s.st.UpsertArtifact(r.Context(), tx, row)
	})
	switch {
	case errors.Is(err, errFenced):
		s.discard(key)
		writeError(w, r, http.StatusConflict, "attempt is not the running attempt")
	case errors.Is(err, store.ErrNotFound):
		s.discard(key)
		writeError(w, r, http.StatusNotFound, "job not found")
	case err != nil:
		s.discard(key)
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusCreated, toArtifactJSON(row))
	}
}

// errFenced is the in-handler signal that the row transaction failed the
// attempt fence.
var errFenced = errors.New("api: attempt is not the running attempt")

// discard removes an object whose row was never written. Best effort: a
// leftover object without a row is unreachable through the API.
func (s *Server) discard(key string) {
	if err := s.artifacts.Delete(context.WithoutCancel(context.Background()), key); err != nil && !errors.Is(err, artifact.ErrNotFound) {
		s.cfg.Logger.Warn("artifact discard failed", "key", key, "err", err)
	}
}

// attemptParam reads attempt= (default: the job's current attempt).
func attemptParam(r *http.Request, job *store.Job) (int, error) {
	v := r.URL.Query().Get("attempt")
	if v == "" {
		return job.Attempt, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, errors.New("attempt must be a positive integer")
	}
	return n, nil
}

// handleListArtifacts returns one attempt's artifacts, path-ordered.
func (s *Server) handleListArtifacts(w http.ResponseWriter, r *http.Request) {
	job, err := s.st.GetJob(r.Context(), s.st.Reader(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, r, err, "job")
		return
	}
	attempt, err := attemptParam(r, job)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	rows, err := s.st.ListArtifacts(r.Context(), s.st.Reader(), job.ID, attempt)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]artifactJSON, 0, len(rows))
	for i := range rows {
		out = append(out, toArtifactJSON(&rows[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"attempt": attempt, "artifacts": out})
}

// handleGetArtifact streams one artifact from the store with the row's
// size and content type; the sha256 travels as the ETag.
func (s *Server) handleGetArtifact(w http.ResponseWriter, r *http.Request) {
	job, err := s.st.GetJob(r.Context(), s.st.Reader(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, r, err, "job")
		return
	}
	attempt, err := attemptParam(r, job)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	path := r.PathValue("path")
	if err := artifact.ValidatePath(path); err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	row, err := s.st.GetArtifact(r.Context(), s.st.Reader(), job.ID, attempt, path)
	if err != nil {
		s.storeError(w, r, err, "artifact")
		return
	}
	rc, err := s.artifacts.Get(r.Context(), artifact.JobKey(job.RunID, job.ID, attempt, path))
	if errors.Is(err, artifact.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "artifact not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", row.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(row.SizeBytes, 10))
	w.Header().Set("ETag", `"`+row.SHA256+`"`)
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// handleGetSource streams a run's workspace bundle; 404 when the run does
// not exist or was submitted without one.
func (s *Server) handleGetSource(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.st.GetRun(r.Context(), s.st.Reader(), id); err != nil {
		s.storeError(w, r, err, "run")
		return
	}
	rc, err := s.artifacts.Get(r.Context(), artifact.SourceKey(id))
	if errors.Is(err, artifact.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "run has no source bundle")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/x-tar")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}
