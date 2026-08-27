package api

import (
	"errors"
	"net/http"
	"strconv"

	"quarry/internal/scheduler"
	"quarry/internal/store"
)

// Log endpoints (docs/protocol.md): runners ship chunks to
// POST /api/runner/jobs/{id}/logs and clients page through them with
// GET /api/jobs/{id}/logs?after=. Chunk bytes travel base64-encoded in
// JSON ([]byte's default encoding).

// MaxLogBodyBytes bounds one log POST. The shipper sends at most 512 KiB
// of chunk data per request, which base64 grows by a third.
const MaxLogBodyBytes = 1 << 20

type logChunkJSON struct {
	Seq  int64  `json:"seq"`
	Data []byte `json:"data"`
}

type logsRequest struct {
	RunnerID string         `json:"runner_id"`
	Attempt  int            `json:"attempt"`
	Chunks   []logChunkJSON `json:"chunks"`
}

type logsResponse struct {
	Attempt int            `json:"attempt"`
	Chunks  []logChunkJSON `json:"chunks"`
	// Next is the cursor to pass as after= to read what follows: the last
	// seq returned, or the after= that was asked for when nothing was.
	Next int64 `json:"next"`
}

// handleAppendLogs ingests one batch for an attempt. 204 on success
// (including a redelivered batch), 409 when the attempt is not the running
// one, 404 unknown job, 400 bad body or seq < 1.
func (s *Server) handleAppendLogs(w http.ResponseWriter, r *http.Request) {
	var req logsRequest
	if !s.decodeBody(w, r, &req, MaxLogBodyBytes) {
		return
	}
	if req.RunnerID == "" || req.Attempt <= 0 {
		writeError(w, r, http.StatusBadRequest, "runner_id and a positive attempt are required")
		return
	}
	chunks := make([]store.LogChunk, 0, len(req.Chunks))
	for _, c := range req.Chunks {
		chunks = append(chunks, store.LogChunk{Seq: c.Seq, Data: c.Data})
	}
	err := s.sched.AppendLogs(r.Context(), r.PathValue("id"), req.Attempt, chunks)
	switch {
	case errors.Is(err, scheduler.ErrFenced):
		writeError(w, r, http.StatusConflict, "attempt is not the running attempt")
	case errors.Is(err, store.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "job not found")
	case err != nil && isBadResult(err):
		writeError(w, r, http.StatusBadRequest, err.Error())
	case err != nil:
		s.internalError(w, r, err)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleGetLogs returns the chunks of one attempt with seq > after, in seq
// order. attempt= defaults to the job's current attempt.
func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	job, err := s.st.GetJob(r.Context(), s.st.Reader(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, r, err, "job")
		return
	}
	attempt := job.Attempt
	if v := r.URL.Query().Get("attempt"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, r, http.StatusBadRequest, "attempt must be a positive integer")
			return
		}
		attempt = n
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
	chunks, err := s.st.ListLogChunks(r.Context(), s.st.Reader(), job.ID, attempt, after)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := logsResponse{Attempt: attempt, Chunks: make([]logChunkJSON, 0, len(chunks)), Next: after}
	for _, c := range chunks {
		out.Chunks = append(out.Chunks, logChunkJSON{Seq: c.Seq, Data: c.Data})
		out.Next = c.Seq
	}
	writeJSON(w, http.StatusOK, out)
}
