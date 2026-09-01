package api

import (
	"net/http"
	"testing"

	"quarry/internal/store"
)

// POST /api/runs/{id}/cancel: 202 with the run as stored, jobs that had
// not started are cancelled, the running one gets the cancel directive on
// its next heartbeat and its failed(cancelled) report finishes the run as
// cancelled; 404 unknown, 409 once terminal.
func TestCancelRunEndpoint(t *testing.T) {
	srv, _ := newTestServer(t)
	var created runDetailJSON
	do(t, srv, "POST", "/api/runs", validYAML, &created)
	const claimBody = `{"runner_id":"r1","name":"one","capacity":2}`
	var claimed claimedJSON
	do(t, srv, "POST", "/api/runner/claim", claimBody, &claimed) // build (lint stays queued)
	if claimed.Job == nil || claimed.Job.Name != "build" {
		t.Fatalf("claimed %+v, want build", claimed.Job)
	}

	var detail runDetailJSON
	if resp := do(t, srv, "POST", "/api/runs/"+created.Run.ID+"/cancel", "", &detail); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel: %d, want 202", resp.StatusCode)
	}
	states := map[string]string{}
	for _, j := range detail.Jobs {
		states[j.Name] = j.State
	}
	if states["build"] != store.JobRunning || states["lint"] != store.JobCancelled ||
		states["test"] != store.JobCancelled || states["deploy"] != store.JobCancelled || detail.Run.State != store.RunRunning {
		t.Fatalf("after cancel: run %s, jobs %v", detail.Run.State, states)
	}

	var hb struct {
		Jobs []directiveJSON `json:"jobs"`
	}
	do(t, srv, "POST", "/api/runner/heartbeat", `{"runner_id":"r1","jobs":[{"job_id":"`+claimed.Job.ID+`","attempt":1}]}`, &hb)
	if len(hb.Jobs) != 1 || hb.Jobs[0].Directive != "cancel" {
		t.Fatalf("directives = %+v, want cancel", hb.Jobs)
	}
	var done jobJSON
	if resp := do(t, srv, "POST", "/api/runner/jobs/"+claimed.Job.ID+"/complete",
		`{"runner_id":"r1","attempt":1,"status":"failed","failure_kind":"cancelled","error":"job cancelled by user"}`, &done); resp.StatusCode != http.StatusOK {
		t.Fatalf("complete: %d", resp.StatusCode)
	}
	if done.State != store.JobCancelled || done.FailureKind != store.FailureCancelled {
		t.Fatalf("complete body = %+v", done)
	}
	do(t, srv, "GET", "/api/runs/"+created.Run.ID, "", &detail)
	if detail.Run.State != store.RunCancelled || detail.Run.FinishedAt == 0 {
		t.Fatalf("final run = %+v", detail.Run)
	}

	if resp := do(t, srv, "POST", "/api/runs/"+created.Run.ID+"/cancel", "", nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("cancel finished run: %d, want 409", resp.StatusCode)
	}
	if resp := do(t, srv, "POST", "/api/runs/nope/cancel", "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cancel unknown run: %d, want 404", resp.StatusCode)
	}
}
