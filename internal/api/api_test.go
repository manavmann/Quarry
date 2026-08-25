package api

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"quarry/internal/store"
)

const testToken = "secret-token"

const validYAML = `name: demo
jobs:
  - name: build
    image: alpine
    steps: ["echo build"]
  - name: lint
    image: alpine
    steps: ["echo lint"]
  - name: test
    image: alpine
    steps: ["echo test"]
    needs: [build]
  - name: deploy
    image: alpine
    steps: ["echo deploy"]
    needs: [build, test]
`

func newTestServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	var ms int64 = 1_700_000_000_000
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "quarry.db"),
		store.WithClock(func() int64 { ms++; return ms }))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	srv := httptest.NewServer(New(st, Config{APIToken: testToken, Logger: log.New(io.Discard, "", 0)}))
	t.Cleanup(srv.Close)
	return srv, st
}

// do sends a request with the API token and decodes the JSON body into out
// (when out is non-nil). It returns the response for status/header checks.
func do(t *testing.T, srv *httptest.Server, method, path, body string, out any, hdr ...string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s %s: decode %q: %v", method, path, raw, err)
		}
	}
	return resp
}

type runDetailJSON struct {
	Run  runJSON   `json:"run"`
	Jobs []jobJSON `json:"jobs"`
}

func TestSubmitValidQueuesRoots(t *testing.T) {
	srv, st := newTestServer(t)

	var got runDetailJSON
	resp := do(t, srv, http.MethodPost, "/api/runs", validYAML, &got)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if resp.Header.Get("X-Request-ID") == "" {
		t.Error("missing X-Request-ID on response")
	}
	if got.Run.ID == "" || got.Run.State != store.RunPending || got.Run.Trigger != TriggerAPI {
		t.Fatalf("run = %+v", got.Run)
	}
	if len(got.Jobs) != 4 {
		t.Fatalf("jobs = %d, want 4", len(got.Jobs))
	}

	// Roots (build, lint) are queued; test and deploy wait on their needs.
	want := map[string]string{"build": store.JobQueued, "lint": store.JobQueued,
		"test": store.JobPending, "deploy": store.JobPending}
	jobs, err := st.ListJobs(context.Background(), st.Reader(), got.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range jobs {
		if j.State != want[j.Name] {
			t.Errorf("job %s state = %s, want %s", j.Name, j.State, want[j.Name])
		}
		if j.MaxAttempts != 1 || j.Attempt != 0 {
			t.Errorf("job %s attempts = %d/%d, want 0/1", j.Name, j.Attempt, j.MaxAttempts)
		}
	}
	deps, err := st.ListJobDeps(context.Background(), st.Reader(), got.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(deps) != 3 {
		t.Errorf("deps = %d, want 3 (test->build, deploy->build, deploy->test)", len(deps))
	}
	run, err := st.GetRun(context.Background(), st.Reader(), got.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.PipelineYAML != validYAML {
		t.Error("pipeline_yaml not stored verbatim")
	}

	// The submission is on the timeline.
	var evs struct {
		Events []eventJSON `json:"events"`
	}
	do(t, srv, http.MethodGet, "/api/runs/"+got.Run.ID+"/events", "", &evs)
	if len(evs.Events) != 1 || evs.Events[0].Type != "run.created" {
		t.Errorf("events = %+v, want one run.created", evs.Events)
	}
}

func TestSubmitInvalid(t *testing.T) {
	srv, _ := newTestServer(t)
	cases := map[string]string{
		"bad yaml":      "jobs: [",
		"empty":         "",
		"unknown needs": "jobs:\n  - {name: a, image: x, steps: [go], needs: [zzz]}\n",
		"cycle":         "jobs:\n  - {name: a, image: x, steps: [go], needs: [b]}\n  - {name: b, image: x, steps: [go], needs: [a]}\n",
		"no image":      "jobs:\n  - {name: a, steps: [go]}\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			var e struct {
				Error string `json:"error"`
			}
			resp := do(t, srv, http.MethodPost, "/api/runs", body, &e)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			if e.Error == "" {
				t.Error("error body is empty")
			}
		})
	}
	var runs struct {
		Runs []runJSON `json:"runs"`
	}
	do(t, srv, http.MethodGet, "/api/runs", "", &runs)
	if len(runs.Runs) != 0 {
		t.Errorf("invalid submissions created %d runs", len(runs.Runs))
	}
}

func TestSubmitTooLarge(t *testing.T) {
	srv, _ := newTestServer(t)
	body := "name: big\njobs: []\n# " + strings.Repeat("x", MaxPipelineBytes)
	resp := do(t, srv, http.MethodPost, "/api/runs", body, nil)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}

func TestAuth(t *testing.T) {
	srv, _ := newTestServer(t)
	for _, auth := range []string{"", "Bearer wrong", "Basic " + testToken, testToken} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/runs", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("Authorization %q: status = %d, want 401", auth, resp.StatusCode)
		}
	}
	// Unauthenticated POST must not create anything either.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/runs", strings.NewReader(validYAML))
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("unauthenticated POST: status = %d, want 401", resp.StatusCode)
	}
	var runs struct {
		Runs []runJSON `json:"runs"`
	}
	do(t, srv, http.MethodGet, "/api/runs", "", &runs)
	if len(runs.Runs) != 0 {
		t.Errorf("unauthenticated POST created %d runs", len(runs.Runs))
	}

	// /healthz needs no token.
	resp, err = http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz: status = %d, want 200", resp.StatusCode)
	}
}

func TestReads(t *testing.T) {
	srv, _ := newTestServer(t)
	var created runDetailJSON
	do(t, srv, http.MethodPost, "/api/runs", validYAML, &created)

	var got runDetailJSON
	resp := do(t, srv, http.MethodGet, "/api/runs/"+created.Run.ID, "", &got)
	if resp.StatusCode != http.StatusOK || got.Run.ID != created.Run.ID || len(got.Jobs) != 4 {
		t.Fatalf("GET run: status=%d run=%+v jobs=%d", resp.StatusCode, got.Run, len(got.Jobs))
	}

	var job jobJSON
	resp = do(t, srv, http.MethodGet, "/api/jobs/"+created.Jobs[0].ID, "", &job)
	if resp.StatusCode != http.StatusOK || job.Name != "build" || job.RunID != created.Run.ID {
		t.Fatalf("GET job: status=%d job=%+v", resp.StatusCode, job)
	}
	var spec struct {
		Image string `json:"Image"`
	}
	if err := json.Unmarshal(job.Spec, &spec); err != nil || spec.Image != "alpine" {
		t.Errorf("job spec = %s (%v), want image alpine", job.Spec, err)
	}

	var runs struct {
		Runs []runJSON `json:"runs"`
	}
	do(t, srv, http.MethodPost, "/api/runs", validYAML, nil)
	do(t, srv, http.MethodGet, "/api/runs?limit=1", "", &runs)
	if len(runs.Runs) != 1 {
		t.Errorf("limit=1 returned %d runs", len(runs.Runs))
	}
	if resp := do(t, srv, http.MethodGet, "/api/runs?limit=x", "", nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("limit=x: status = %d, want 400", resp.StatusCode)
	}

	for _, p := range []string{"/api/runs/nope", "/api/runs/nope/events", "/api/jobs/nope"} {
		if resp := do(t, srv, http.MethodGet, p, "", nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: status = %d, want 404", p, resp.StatusCode)
		}
	}

	var runners struct {
		Runners []runnerJSON `json:"runners"`
	}
	if resp := do(t, srv, http.MethodGet, "/api/runners", "", &runners); resp.StatusCode != http.StatusOK || len(runners.Runners) != 0 {
		t.Errorf("GET runners: status=%d n=%d", resp.StatusCode, len(runners.Runners))
	}
}

func TestRequestIDIsHonoured(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := do(t, srv, http.MethodGet, "/api/runs", "", nil, "X-Request-ID", "abc-123")
	if got := resp.Header.Get("X-Request-ID"); got != "abc-123" {
		t.Errorf("X-Request-ID = %q, want abc-123", got)
	}
}

// ---- runner protocol -----------------------------------------------------

type claimedJSON struct {
	Job *struct {
		jobJSON
		LeaseTTLMillis int64 `json:"lease_ttl_ms"`
	} `json:"job"`
}

func TestRunnerClaimHeartbeatComplete(t *testing.T) {
	srv, _ := newTestServer(t)
	var created runDetailJSON
	do(t, srv, "POST", "/api/runs", validYAML, &created)

	// build and lint are roots → two claims succeed, the third is 204.
	// Every claim carries the full registration, as a real runner does.
	const claimBody = `{"runner_id":"r1","name":"one","labels":{"os":"linux"},"capacity":2}`
	var first, second claimedJSON
	if resp := do(t, srv, "POST", "/api/runner/claim", claimBody, &first); resp.StatusCode != http.StatusOK {
		t.Fatalf("claim 1: %d", resp.StatusCode)
	}
	if first.Job == nil || first.Job.Attempt != 1 || first.Job.State != store.JobRunning || first.Job.LeaseTTLMillis != 30_000 {
		t.Fatalf("claim 1 body = %+v", first.Job)
	}
	if resp := do(t, srv, "POST", "/api/runner/claim", claimBody, &second); resp.StatusCode != http.StatusOK {
		t.Fatalf("claim 2: %d", resp.StatusCode)
	}
	if resp := do(t, srv, "POST", "/api/runner/claim", claimBody, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("claim 3: %d, want 204", resp.StatusCode)
	}
	var runners struct {
		Runners []runnerJSON `json:"runners"`
	}
	do(t, srv, "GET", "/api/runners", "", &runners)
	if len(runners.Runners) != 1 || runners.Runners[0].Name != "one" || runners.Runners[0].Capacity != 2 {
		t.Fatalf("runners = %+v", runners.Runners)
	}

	// Heartbeat: live attempt continues, stale attempt aborts.
	var hb struct {
		Jobs []directiveJSON `json:"jobs"`
	}
	body := `{"runner_id":"r1","jobs":[{"job_id":"` + first.Job.ID + `","attempt":1},{"job_id":"` + second.Job.ID + `","attempt":7}]}`
	if resp := do(t, srv, "POST", "/api/runner/heartbeat", body, &hb); resp.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat: %d", resp.StatusCode)
	}
	if len(hb.Jobs) != 2 || hb.Jobs[0].Directive != "continue" || hb.Jobs[1].Directive != "abort" {
		t.Fatalf("directives = %+v", hb.Jobs)
	}

	// Complete build → test becomes queued; a stale attempt is 409;
	// a duplicate is 200 and changes nothing.
	build, lint := first.Job, second.Job
	if build.Name != "build" {
		build, lint = lint, build
	}
	var done jobJSON
	if resp := do(t, srv, "POST", "/api/runner/jobs/"+build.ID+"/complete",
		`{"runner_id":"r1","attempt":1,"status":"succeeded","exit_code":0}`, &done); resp.StatusCode != http.StatusOK {
		t.Fatalf("complete: %d", resp.StatusCode)
	}
	if done.State != store.JobSucceeded {
		t.Fatalf("complete body = %+v", done)
	}
	if resp := do(t, srv, "POST", "/api/runner/jobs/"+build.ID+"/complete",
		`{"runner_id":"r1","attempt":1,"status":"failed","failure_kind":"exit_code","exit_code":1}`, &done); resp.StatusCode != http.StatusOK || done.State != store.JobSucceeded {
		t.Fatalf("duplicate complete: %d %+v", resp.StatusCode, done)
	}
	if resp := do(t, srv, "POST", "/api/runner/jobs/"+build.ID+"/complete",
		`{"runner_id":"r1","attempt":2,"status":"succeeded"}`, nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale attempt: %d, want 409", resp.StatusCode)
	}
	if resp := do(t, srv, "POST", "/api/runner/jobs/nope/complete",
		`{"runner_id":"r1","attempt":1,"status":"succeeded"}`, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown job: %d, want 404", resp.StatusCode)
	}
	if resp := do(t, srv, "POST", "/api/runner/jobs/"+lint.ID+"/complete",
		`{"runner_id":"r1","attempt":1,"status":"failed"}`, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("failed without kind: %d, want 400", resp.StatusCode)
	}

	var detail runDetailJSON
	do(t, srv, "GET", "/api/runs/"+created.Run.ID, "", &detail)
	states := map[string]string{}
	for _, j := range detail.Jobs {
		states[j.Name] = j.State
	}
	if states["build"] != store.JobSucceeded || states["test"] != store.JobQueued || states["deploy"] != store.JobPending || states["lint"] != store.JobRunning {
		t.Fatalf("states = %v", states)
	}
	if detail.Run.State != store.RunRunning {
		t.Fatalf("run state = %s", detail.Run.State)
	}

	// Finish the rest: lint fails → run fails once test and deploy settle.
	do(t, srv, "POST", "/api/runner/jobs/"+lint.ID+"/complete",
		`{"runner_id":"r1","attempt":1,"status":"failed","failure_kind":"exit_code","exit_code":3}`, nil)
	var testJob claimedJSON
	do(t, srv, "POST", "/api/runner/claim", claimBody, &testJob)
	if testJob.Job == nil || testJob.Job.Name != "test" {
		t.Fatalf("expected to claim test, got %+v", testJob.Job)
	}
	do(t, srv, "POST", "/api/runner/jobs/"+testJob.Job.ID+"/complete",
		`{"runner_id":"r1","attempt":1,"status":"succeeded"}`, nil)
	var deploy claimedJSON
	do(t, srv, "POST", "/api/runner/claim", claimBody, &deploy)
	do(t, srv, "POST", "/api/runner/jobs/"+deploy.Job.ID+"/complete",
		`{"runner_id":"r1","attempt":1,"status":"succeeded"}`, nil)
	do(t, srv, "GET", "/api/runs/"+created.Run.ID, "", &detail)
	if detail.Run.State != store.RunFailed || detail.Run.FinishedAt == 0 {
		t.Fatalf("final run = %+v", detail.Run)
	}
}

func TestRunnerEndpointsValidateInput(t *testing.T) {
	srv, _ := newTestServer(t)
	for _, tc := range []struct {
		path, body string
		want       int
	}{
		{"/api/runner/claim", `{}`, http.StatusBadRequest},
		{"/api/runner/claim", `not json`, http.StatusBadRequest},
		{"/api/runner/heartbeat", `{"jobs":[]}`, http.StatusBadRequest},
		{"/api/runner/jobs/x/complete", `{"runner_id":"r1","attempt":0,"status":"succeeded"}`, http.StatusBadRequest},
		{"/api/runner/claim", `{"runner_id":"` + strings.Repeat("x", MaxRunnerBodyBytes) + `"}`, http.StatusRequestEntityTooLarge},
	} {
		if resp := do(t, srv, "POST", tc.path, tc.body, nil); resp.StatusCode != tc.want {
			t.Errorf("POST %s %.40q: %d, want %d", tc.path, tc.body, resp.StatusCode, tc.want)
		}
	}
	// Runner endpoints sit behind the same bearer check.
	req, _ := http.NewRequest("POST", srv.URL+"/api/runner/claim", strings.NewReader(`{"runner_id":"r1"}`))
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: %d, want 401", resp.StatusCode)
	}
}

func TestRunnerRegisterIsIdempotentByName(t *testing.T) {
	srv, _ := newTestServer(t)

	var first, again, other struct {
		RunnerID string `json:"runner_id"`
	}
	if resp := do(t, srv, "POST", "/api/runner/register",
		`{"name":"box-a","labels":{"os":"linux"},"capacity":2}`, &first); resp.StatusCode != http.StatusOK || first.RunnerID == "" {
		t.Fatalf("register: status=%d body=%+v", resp.StatusCode, first)
	}
	// Same name → same id; labels and capacity are refreshed.
	if resp := do(t, srv, "POST", "/api/runner/register",
		`{"name":"box-a","labels":{"os":"linux","gpu":"true"},"capacity":4}`, &again); resp.StatusCode != http.StatusOK {
		t.Fatalf("re-register: status=%d", resp.StatusCode)
	}
	if again.RunnerID != first.RunnerID {
		t.Fatalf("re-register minted a new id: %s -> %s", first.RunnerID, again.RunnerID)
	}
	// A different name gets a different id.
	do(t, srv, "POST", "/api/runner/register", `{"name":"box-b","capacity":1}`, &other)
	if other.RunnerID == "" || other.RunnerID == first.RunnerID {
		t.Fatalf("box-b id = %q (box-a %q)", other.RunnerID, first.RunnerID)
	}

	var runners struct {
		Runners []runnerJSON `json:"runners"`
	}
	do(t, srv, "GET", "/api/runners", "", &runners)
	if len(runners.Runners) != 2 {
		t.Fatalf("runners = %+v", runners.Runners)
	}
	byName := map[string]runnerJSON{}
	for _, r := range runners.Runners {
		byName[r.Name] = r
	}
	a := byName["box-a"]
	if a.ID != first.RunnerID || a.Capacity != 4 || a.Labels["gpu"] != "true" {
		t.Fatalf("box-a after re-register = %+v", a)
	}
	if b := byName["box-b"]; b.ID != other.RunnerID || b.Capacity != 1 {
		t.Fatalf("box-b = %+v", b)
	}

	if resp := do(t, srv, "POST", "/api/runner/register", `{"labels":{}}`, nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("register without name: status=%d, want 400", resp.StatusCode)
	}
	if resp := do(t, srv, "POST", "/api/runner/register", `{"name":"x","capacity":-1}`, nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("register with negative capacity: status=%d, want 400", resp.StatusCode)
	}
}
