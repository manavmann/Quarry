package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"quarry/internal/artifact/local"
	"quarry/internal/scheduler"
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
	srv, st, _ := newTestServerWithBlobs(t)
	return srv, st
}

// newTestServerWithBlobs also returns the server's artifact store.
func newTestServerWithBlobs(t *testing.T) (*httptest.Server, *store.Store, *local.Store) {
	t.Helper()
	var ms int64 = 1_700_000_000_000
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "quarry.db"),
		store.WithClock(func() int64 { ms++; return ms }))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	blobs, err := local.New(filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	srv := httptest.NewServer(New(st, Config{APIToken: testToken, Logger: log.New(io.Discard, "", 0), Artifacts: blobs}))
	t.Cleanup(srv.Close)
	return srv, st, blobs
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
		if j.MaxAttempts != scheduler.DefaultMaxAttempts || j.Attempt != 0 {
			t.Errorf("job %s attempts = %d/%d, want 0/%d", j.Name, j.Attempt, j.MaxAttempts, scheduler.DefaultMaxAttempts)
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

func TestRunnerLogsIngestAndCursorRead(t *testing.T) {
	srv, _ := newTestServer(t)
	var created runDetailJSON
	do(t, srv, "POST", "/api/runs", validYAML, &created)
	var claimed claimedJSON
	do(t, srv, "POST", "/api/runner/claim", `{"runner_id":"r1","name":"one","capacity":1}`, &claimed)
	job := claimed.Job
	logsPath := "/api/runner/jobs/" + job.ID + "/logs"
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	// Out of order, then a duplicate batch: both 204.
	body := `{"runner_id":"r1","attempt":1,"chunks":[{"seq":2,"data":"` + b64("two\n") + `"},{"seq":3,"data":"` + b64("three\n") + `"}]}`
	if resp := do(t, srv, "POST", logsPath, body, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("logs 1: %d", resp.StatusCode)
	}
	if resp := do(t, srv, "POST", logsPath, body, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("logs redelivery: %d", resp.StatusCode)
	}
	if resp := do(t, srv, "POST", logsPath, `{"runner_id":"r1","attempt":1,"chunks":[{"seq":1,"data":"`+b64("one\n")+`"}]}`, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("logs 2: %d", resp.StatusCode)
	}
	// Fence and validation.
	if resp := do(t, srv, "POST", logsPath, `{"runner_id":"r1","attempt":2,"chunks":[{"seq":4,"data":"eA=="}]}`, nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale attempt: %d, want 409", resp.StatusCode)
	}
	if resp := do(t, srv, "POST", "/api/runner/jobs/nope/logs", `{"runner_id":"r1","attempt":1,"chunks":[]}`, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown job: %d, want 404", resp.StatusCode)
	}
	if resp := do(t, srv, "POST", logsPath, `{"runner_id":"r1","attempt":1,"chunks":[{"seq":0,"data":"eA=="}]}`, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("seq 0: %d, want 400", resp.StatusCode)
	}
	if resp := do(t, srv, "POST", logsPath, `{"attempt":1,"chunks":[]}`, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("no runner_id: %d, want 400", resp.StatusCode)
	}

	// Reads: ordered, next round-trips, after is strictly greater-than.
	var got logsResponse
	do(t, srv, "GET", "/api/jobs/"+job.ID+"/logs", "", &got)
	if got.Attempt != 1 || got.Next != 3 || len(got.Chunks) != 3 {
		t.Fatalf("logs = %+v", got)
	}
	var text string
	for i, c := range got.Chunks {
		if c.Seq != int64(i+1) {
			t.Errorf("chunk %d has seq %d", i, c.Seq)
		}
		text += string(c.Data)
	}
	if text != "one\ntwo\nthree\n" {
		t.Errorf("text = %q", text)
	}
	do(t, srv, "GET", "/api/jobs/"+job.ID+"/logs?after=2", "", &got)
	if len(got.Chunks) != 1 || got.Chunks[0].Seq != 3 || got.Next != 3 {
		t.Errorf("after=2: %+v", got)
	}
	do(t, srv, "GET", "/api/jobs/"+job.ID+"/logs?after=3", "", &got)
	if len(got.Chunks) != 0 || got.Next != 3 {
		t.Errorf("after=3: %+v", got)
	}
	do(t, srv, "GET", "/api/jobs/"+job.ID+"/logs?attempt=2", "", &got)
	if len(got.Chunks) != 0 || got.Attempt != 2 || got.Next != 0 {
		t.Errorf("attempt=2: %+v", got)
	}
	if resp := do(t, srv, "GET", "/api/jobs/"+job.ID+"/logs?after=-1", "", nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("after=-1: %d", resp.StatusCode)
	}
	if resp := do(t, srv, "GET", "/api/jobs/nope/logs", "", nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown job read: %d", resp.StatusCode)
	}

	// Once the attempt has completed, a late chunk is fenced.
	do(t, srv, "POST", "/api/runner/jobs/"+job.ID+"/complete", `{"runner_id":"r1","attempt":1,"status":"succeeded"}`, nil)
	if resp := do(t, srv, "POST", logsPath, `{"runner_id":"r1","attempt":1,"chunks":[{"seq":4,"data":"eA=="}]}`, nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("after complete: %d, want 409", resp.StatusCode)
	}
	do(t, srv, "GET", "/api/jobs/"+job.ID+"/logs", "", &got)
	if got.Next != 3 {
		t.Errorf("log grew after complete: %+v", got)
	}
}

// TestSubmitMultipart covers the CLI's submit shape: the pipeline as the
// "pipeline" part and the workspace bundle as "source", which is drained
// until C10 stores it. A multipart body without a pipeline part is 400.
func TestSubmitMultipart(t *testing.T) {
	srv, _ := newTestServer(t)

	post := func(t *testing.T, parts map[string]string, out any) *http.Response {
		t.Helper()
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		for name, body := range parts {
			fw, err := mw.CreateFormFile(name, name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(fw, body); err != nil {
				t.Fatal(err)
			}
		}
		if err := mw.Close(); err != nil {
			t.Fatal(err)
		}
		return do(t, srv, http.MethodPost, "/api/runs", buf.String(), out, "Content-Type", mw.FormDataContentType())
	}

	var got runDetailJSON
	resp := post(t, map[string]string{"pipeline": validYAML, "source": strings.Repeat("x", 4096)}, &got)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if got.Run.ID == "" || len(got.Jobs) != 4 {
		t.Fatalf("run = %+v jobs = %d", got.Run, len(got.Jobs))
	}

	var e struct {
		Error string `json:"error"`
	}
	resp = post(t, map[string]string{"source": "x"}, &e)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(e.Error, "pipeline") {
		t.Fatalf("status = %d err = %q, want 400 naming the pipeline part", resp.StatusCode, e.Error)
	}
}
