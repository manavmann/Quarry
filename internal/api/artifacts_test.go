package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// upload POSTs body as an artifact of job/attempt with the given
// Content-Length (-1 sends none) and decodes the JSON reply into out.
func upload(t *testing.T, srv *httptest.Server, jobID string, attempt int, path, body string, length int64, out any) *http.Response {
	t.Helper()
	url := srv.URL + "/api/runner/jobs/" + jobID + "/attempts/" + strconv.Itoa(attempt) + "/artifacts/" + path
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = length
	if length < 0 {
		req.TransferEncoding = []string{"chunked"}
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "text/plain")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("decode %q: %v", raw, err)
		}
	}
	return resp
}

func sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func TestArtifactUploadListDownload(t *testing.T) {
	srv, _ := newTestServer(t)
	var created runDetailJSON
	do(t, srv, "POST", "/api/runs", validYAML, &created)
	var claimed claimedJSON
	do(t, srv, "POST", "/api/runner/claim", `{"runner_id":"r1","name":"one","capacity":1}`, &claimed)
	job := claimed.Job

	var a artifactJSON
	if resp := upload(t, srv, job.ID, 1, "dist/app.bin", "binary!", 7, &a); resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload: %d", resp.StatusCode)
	}
	if a.Path != "dist/app.bin" || a.SizeBytes != 7 || a.SHA256 != sum("binary!") || a.ContentType != "text/plain" || a.CreatedAt == 0 {
		t.Fatalf("reply = %+v", a)
	}
	if resp := upload(t, srv, job.ID, 1, "out.txt", "top", 3, nil); resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload 2: %d", resp.StatusCode)
	}
	// Redelivery overwrites object and row.
	if resp := upload(t, srv, job.ID, 1, "out.txt", "top2", 4, &a); resp.StatusCode != http.StatusCreated || a.SHA256 != sum("top2") {
		t.Fatalf("redelivery: %d %+v", resp.StatusCode, a)
	}

	var list struct {
		Attempt   int            `json:"attempt"`
		Artifacts []artifactJSON `json:"artifacts"`
	}
	do(t, srv, "GET", "/api/jobs/"+job.ID+"/artifacts", "", &list)
	if list.Attempt != 1 || len(list.Artifacts) != 2 || list.Artifacts[0].Path != "dist/app.bin" || list.Artifacts[1].SizeBytes != 4 {
		t.Fatalf("list = %+v", list)
	}
	do(t, srv, "GET", "/api/jobs/"+job.ID+"/artifacts?attempt=2", "", &list)
	if len(list.Artifacts) != 0 {
		t.Fatalf("other attempt = %+v", list)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/jobs/"+job.ID+"/artifacts/dist/app.bin", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "binary!" {
		t.Fatalf("download: %d %q", resp.StatusCode, body)
	}
	if resp.Header.Get("Content-Type") != "text/plain" || resp.ContentLength != 7 || resp.Header.Get("ETag") != `"`+sum("binary!")+`"` {
		t.Fatalf("download headers: %v len=%d", resp.Header, resp.ContentLength)
	}
	if resp := do(t, srv, "GET", "/api/jobs/"+job.ID+"/artifacts/nope", "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing artifact: %d", resp.StatusCode)
	}
	if resp := do(t, srv, "GET", "/api/jobs/nope/artifacts", "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing job: %d", resp.StatusCode)
	}
}

func TestArtifactUploadIsFencedAndValidated(t *testing.T) {
	srv, _, blobs := newTestServerWithBlobs(t)
	var created runDetailJSON
	do(t, srv, "POST", "/api/runs", validYAML, &created)
	var claimed claimedJSON
	do(t, srv, "POST", "/api/runner/claim", `{"runner_id":"r1","name":"one","capacity":1}`, &claimed)
	job := claimed.Job

	if resp := upload(t, srv, job.ID, 2, "a", "x", 1, nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale attempt: %d, want 409", resp.StatusCode)
	}
	if resp := upload(t, srv, "nope", 1, "a", "x", 1, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown job: %d, want 404", resp.StatusCode)
	}
	if resp := upload(t, srv, job.ID, 1, "a", "x", -1, nil); resp.StatusCode != http.StatusLengthRequired {
		t.Fatalf("no Content-Length: %d, want 411", resp.StatusCode)
	}
	// Traversal: percent-encoded forms reach the handler and are rejected
	// by artifact.ValidatePath; literal ".." segments never match a route
	// because the mux cleans them first. Either way nothing is stored.
	for _, p := range []string{"..%2Fescape", "a%2F..%2F..%2Fb", "%2e%2e/x", "a/%2e%2e/b", "a%5Cb", "a%2F%2Fb", "a%00b"} {
		var e struct {
			Error string `json:"error"`
		}
		if resp := upload(t, srv, job.ID, 1, p, "x", 1, &e); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("path %q: %d %q, want 400", p, resp.StatusCode, e.Error)
		}
	}
	for _, p := range []string{"../escape", "a/../../b"} {
		if resp := upload(t, srv, job.ID, 1, p, "x", 1, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("path %q: %d, want 404", p, resp.StatusCode)
		}
	}
	if keys, err := blobs.List(t.Context(), ""); err != nil || len(keys) != 0 {
		t.Fatalf("objects after rejected uploads = %v, %v", keys, err)
	}
	// A job that is no longer running rejects uploads (fence in the row tx
	// as well as up front).
	do(t, srv, "POST", "/api/runner/jobs/"+job.ID+"/complete", `{"runner_id":"r1","attempt":1,"status":"succeeded"}`, nil)
	if resp := upload(t, srv, job.ID, 1, "late", "x", 1, nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("after complete: %d, want 409", resp.StatusCode)
	}
	var list struct {
		Artifacts []artifactJSON `json:"artifacts"`
	}
	do(t, srv, "GET", "/api/jobs/"+job.ID+"/artifacts", "", &list)
	if len(list.Artifacts) != 0 {
		t.Fatalf("fenced uploads left rows: %+v", list)
	}
}

// The multipart source part is stored under the run and served back to
// runners; a run without one is a 404.
func TestSubmitStoresSourceBundle(t *testing.T) {
	srv, _, blobs := newTestServerWithBlobs(t)
	bundle := strings.Repeat("tar bytes ", 1000)
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("pipeline", "p")
	io.WriteString(fw, validYAML)
	fw, _ = mw.CreateFormFile("source", "s")
	io.WriteString(fw, bundle)
	mw.Close()
	var got runDetailJSON
	if resp := do(t, srv, http.MethodPost, "/api/runs", buf.String(), &got, "Content-Type", mw.FormDataContentType()); resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit: %d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/runs/"+got.Run.ID+"/source", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != bundle || resp.Header.Get("Content-Type") != "application/x-tar" {
		t.Fatalf("source: %d len=%d ct=%q", resp.StatusCode, len(body), resp.Header.Get("Content-Type"))
	}

	var plain runDetailJSON
	do(t, srv, "POST", "/api/runs", validYAML, &plain)
	if resp := do(t, srv, "GET", "/api/runs/"+plain.Run.ID+"/source", "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("no bundle: %d, want 404", resp.StatusCode)
	}
	if resp := do(t, srv, "GET", "/api/runs/nope/source", "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("no run: %d, want 404", resp.StatusCode)
	}

	// An invalid pipeline leaves no orphaned bundle behind.
	buf.Reset()
	mw = multipart.NewWriter(&buf)
	fw, _ = mw.CreateFormFile("pipeline", "p")
	io.WriteString(fw, "jobs: [{name: a, image: x, steps: [x], needs: [a]}]")
	fw, _ = mw.CreateFormFile("source", "s")
	io.WriteString(fw, bundle)
	mw.Close()
	if resp := do(t, srv, http.MethodPost, "/api/runs", buf.String(), nil, "Content-Type", mw.FormDataContentType()); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid pipeline: %d", resp.StatusCode)
	}
	keys, err := blobs.List(t.Context(), "sources/")
	if err != nil || len(keys) != 1 {
		t.Fatalf("sources after rejected submit = %v, %v; want only the accepted run's", keys, err)
	}
}
