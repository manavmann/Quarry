package agent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"quarry/internal/executor"
	"quarry/internal/pipeline"
)

type reconnectTransport func(*http.Request) (*http.Response, error)

func (f reconnectTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func reconnectReply(body string) *http.Response {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestAgentConnectionBackoff(t *testing.T) {
	file := filepath.Join(t.TempDir(), "artifact")
	if err := os.WriteFile(file, []byte("artifact"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"register", "claim", "heartbeat", "complete", "source", "artifact"} {
		t.Run(endpoint, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls []time.Time
				c := &client{base: "http://server", backoff: 500 * time.Millisecond}
				c.http = &http.Client{Transport: reconnectTransport(func(r *http.Request) (*http.Response, error) {
					if r.Body != nil {
						defer r.Body.Close()
						io.Copy(io.Discard, r.Body)
					}
					calls = append(calls, time.Now())
					if len(calls) <= 10 {
						return nil, errors.New("connection refused")
					}
					switch endpoint {
					case "register":
						return reconnectReply(`{"runner_id":"r"}`), nil
					case "claim":
						return reconnectReply(`{"job":{"id":"j","attempt":1}}`), nil
					case "heartbeat":
						return reconnectReply(`{"jobs":[{"job_id":"j","attempt":1,"directive":"continue"}]}`), nil
					case "artifact":
						b, _ := json.Marshal(artifactReply{SizeBytes: 8, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("artifact")))})
						return reconnectReply(string(b)), nil
					case "source":
						return reconnectReply("bundle"), nil
					default:
						return reconnectReply("{}"), nil
					}
				})}
				var err error
				switch endpoint {
				case "register":
					_, err = c.register(t.Context(), registerRequest{Name: "r"})
				case "claim":
					_, err = c.claim(t.Context(), claimRequest{RunnerID: "r"})
				case "heartbeat":
					_, err = c.heartbeat(t.Context(), "r", []jobRef{{JobID: "j", Attempt: 1}})
				case "complete":
					err = c.completeWithRetry(t.Context(), "j", completeRequest{Attempt: 1, Status: statusSucceeded}, 1, time.Millisecond)
				case "artifact":
					err = c.uploadArtifact(t.Context(), "j", 1, "out", file)
				case "source":
					var body io.ReadCloser
					body, err = c.source(t.Context(), "r")
					if err == nil {
						b, readErr := io.ReadAll(body)
						err = readErr
						body.Close()
						if string(b) != "bundle" {
							t.Fatalf("source = %q", b)
						}
					}
				}
				if err != nil || len(calls) != 11 {
					t.Fatalf("calls=%d err=%v", len(calls), err)
				}
				delay := 500 * time.Millisecond
				for i := 1; i < len(calls); i++ {
					if got := calls[i].Sub(calls[i-1]); got != delay {
						t.Fatalf("retry %d delay=%s want=%s", i, got, delay)
					}
					delay = min(delay*2, 10*time.Second)
				}
			})
		})
	}
	t.Run("cancel and initial cap", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			c := &client{backoff: time.Hour}
			start, calls := time.Now(), 0
			err := c.reconnect(ctx, func() error {
				calls++
				if calls == 2 {
					cancel()
				}
				return &connectionError{io.ErrUnexpectedEOF}
			})
			if !errors.Is(err, context.Canceled) || calls != 2 || time.Since(start) != 10*time.Second {
				t.Fatalf("cancel: err=%v calls=%d elapsed=%s", err, calls, time.Since(start))
			}
		})
	})
}

type brokenRead struct{}

func TestAgentShutdownDuringOutage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failed := make(chan struct{})
		var once sync.Once
		claimed := false
		hc := &http.Client{Transport: reconnectTransport(func(r *http.Request) (*http.Response, error) {
			if r.Body != nil {
				defer r.Body.Close()
			}
			switch {
			case strings.HasSuffix(r.URL.Path, "/register"):
				return reconnectReply(`{"runner_id":"r"}`), nil
			case strings.HasSuffix(r.URL.Path, "/claim"):
				if claimed {
					resp := reconnectReply("")
					resp.StatusCode = 204
					return resp, nil
				}
				claimed = true
				return reconnectReply(`{"job":{"id":"j","run_id":"r","attempt":1,"spec":{"name":"build"}}}`), nil
			case strings.HasSuffix(r.URL.Path, "/complete"):
				once.Do(func() { close(failed) })
				return nil, errors.New("server down")
			default:
				return reconnectReply("{}"), nil
			}
		})}
		a, err := New(Config{ServerURL: "http://server", Name: "r", HTTPClient: hc, LogFlushTimeout: time.Second, Logger: log.New(io.Discard, "", 0)}, executor.NewFake())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- a.Run(ctx) }()
		<-failed
		start := time.Now()
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Fatalf("shutdown drain took %s", elapsed)
		}
	})
}

func (brokenRead) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestAgentSourceReconnectAfterPartialRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		c := &client{base: "http://server", http: &http.Client{Transport: reconnectTransport(func(*http.Request) (*http.Response, error) {
			calls++
			resp := reconnectReply("whole bundle")
			if calls == 1 {
				resp.Body = io.NopCloser(io.MultiReader(strings.NewReader("whole"), brokenRead{}))
			}
			return resp, nil
		})}}
		body, err := c.source(t.Context(), "r")
		if err != nil {
			t.Fatal(err)
		}
		name := body.(*sourceFile).Name()
		b, err := io.ReadAll(body)
		body.Close()
		if err != nil || string(b) != "whole bundle" || calls != 2 {
			t.Fatalf("body=%q err=%v calls=%d", b, err, calls)
		}
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("source spool not removed: %v", err)
		}
	})
}

func TestAgentDeliverySurvivesConnectionLoss(t *testing.T) {
	for _, endpoint := range []string{"/logs", "/complete", "/artifacts/out"} {
		t.Run(endpoint, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				claimed := false
				completed := make(chan completeRequest, 1)
				hc := &http.Client{Transport: reconnectTransport(func(r *http.Request) (*http.Response, error) {
					if r.Body != nil {
						defer r.Body.Close()
					}
					if strings.HasSuffix(r.URL.Path, endpoint) && time.Since(start) < time.Minute {
						return nil, errors.New("server down")
					}
					switch {
					case strings.HasSuffix(r.URL.Path, "/register"):
						return reconnectReply(`{"runner_id":"r"}`), nil
					case strings.HasSuffix(r.URL.Path, "/claim"):
						if claimed {
							resp := reconnectReply("")
							resp.StatusCode = 204
							return resp, nil
						}
						claimed = true
						b, _ := json.Marshal(map[string]any{"job": claimedJob{ID: "j", RunID: "r", Attempt: 1, Spec: json.RawMessage(`{"name":"build","artifacts":["out"]}`)}})
						return reconnectReply(string(b)), nil
					case strings.HasSuffix(r.URL.Path, "/heartbeat"):
						return reconnectReply(`{"jobs":[{"job_id":"j","attempt":1,"directive":"continue"}]}`), nil
					case strings.HasSuffix(r.URL.Path, "/artifacts/out"):
						b, _ := io.ReadAll(r.Body)
						reply, _ := json.Marshal(artifactReply{SizeBytes: int64(len(b)), SHA256: fmt.Sprintf("%x", sha256.Sum256(b))})
						return reconnectReply(string(reply)), nil
					case strings.HasSuffix(r.URL.Path, "/complete"):
						var req completeRequest
						json.NewDecoder(r.Body).Decode(&req)
						completed <- req
					}
					return reconnectReply("{}"), nil
				})}
				f := executor.NewFake()
				f.Script("build", executor.Outcome{LogBytes: 10, Artifacts: map[string]string{"out": "artifact"}})
				a, err := New(Config{ServerURL: "http://server", Name: "r", HTTPClient: hc, CompleteRetries: 1, LogFlushTimeout: time.Second, Logger: log.New(io.Discard, "", 0)}, f)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- a.Run(ctx) }()
				req := <-completed
				cancel()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if req.Status != statusSucceeded || req.Attempt != 1 || time.Since(start) < time.Minute || len(f.Executions()) != 1 {
					t.Fatalf("completion=%+v elapsed=%s executions=%d", req, time.Since(start), len(f.Executions()))
				}
			})
		})
	}
}

// stubServer is a scripted control plane: it registers runners as
// "id-<name>", hands out queued jobs one per claim, answers heartbeats
// with a scripted directive, and records every completion. It exists so
// the agent can be tested without a store.
type stubServer struct {
	t   *testing.T
	srv *httptest.Server

	mu         sync.Mutex
	registers  []registerRequest
	queue      []claimedJob
	claims     []claimRequest
	heartbeats [][]jobRef
	completes  []completeCall
	// ops is the interleaving of log and complete arrivals per job, e.g.
	// ["logs", "logs", "complete"], for ordering assertions.
	ops map[string][]string
	// logs is every chunk received per job, in arrival order.
	logs map[string][]logChunk
	// logsStatus, when non-zero, is returned to every log POST.
	logsStatus int
	directive  string // applied to every heartbeated attempt; "" = continue
	// completeStatus is popped per complete call; empty means 200.
	completeStatus []int
}

type completeCall struct {
	JobID string
	Req   completeRequest
}

func newStub(t *testing.T) *stubServer {
	t.Helper()
	s := &stubServer{t: t, ops: map[string][]string{}, logs: map[string][]logChunk{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/runner/register", s.handleRegister)
	mux.HandleFunc("POST /api/runner/claim", s.handleClaim)
	mux.HandleFunc("POST /api/runner/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("POST /api/runner/jobs/{id}/complete", s.handleComplete)
	mux.HandleFunc("POST /api/runner/jobs/{id}/logs", s.handleLogs)
	s.srv = httptest.NewServer(mux)
	t.Cleanup(s.srv.Close)
	return s
}

func (s *stubServer) enqueue(name string, timeout time.Duration) {
	spec, _ := json.Marshal(pipeline.Job{Name: name, Image: "alpine", Steps: []string{"true"}, Timeout: timeout})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(s.queue, claimedJob{ID: "j-" + name, RunID: "r1", Name: name, Attempt: 1, Spec: spec})
}

func (s *stubServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer tok" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req registerRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registers = append(s.registers, req)
	if len(s.claims) > 0 {
		s.t.Errorf("register arrived after %d claims", len(s.claims))
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"runner_id": "id-" + req.Name})
}

func (s *stubServer) handleClaim(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer tok" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req claimRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claims = append(s.claims, req)
	if len(s.queue) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	job := s.queue[0]
	s.queue = s.queue[1:]
	_ = json.NewEncoder(w).Encode(map[string]any{"job": job})
}

func (s *stubServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req heartbeatRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats = append(s.heartbeats, req.Jobs)
	out := make([]directive, 0, len(req.Jobs))
	for _, j := range req.Jobs {
		d := s.directive
		if d == "" {
			d = "continue"
		}
		out = append(out, directive{JobID: j.JobID, Attempt: j.Attempt, Directive: d})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"jobs": out})
}

func (s *stubServer) handleComplete(w http.ResponseWriter, r *http.Request) {
	var req completeRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completes = append(s.completes, completeCall{JobID: r.PathValue("id"), Req: req})
	s.ops[r.PathValue("id")] = append(s.ops[r.PathValue("id")], "complete")
	if len(s.completeStatus) > 0 {
		code := s.completeStatus[0]
		s.completeStatus = s.completeStatus[1:]
		if code != http.StatusOK {
			http.Error(w, "scripted", code)
			return
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"id": r.PathValue("id")})
}

func (s *stubServer) completed() []completeCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]completeCall(nil), s.completes...)
}

func testConfig(s *stubServer, capacity int) Config {
	return Config{
		ServerURL: s.srv.URL, Token: "tok", Name: "r1", Labels: map[string]string{"os": "test"},
		Capacity: capacity, PollInterval: 5 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond,
		CompleteBackoff: time.Millisecond, Logger: log.New(io.Discard, "", 0),
	}
}

// startAgent runs an agent against s until the test ends.
func startAgent(t *testing.T, s *stubServer, exec executor.Executor, capacity int) *Agent {
	t.Helper()
	a, err := New(testConfig(s, capacity), exec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = a.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return a
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		case <-tick.C:
		}
	}
}

func TestAgentRegistersThenClaimsRunsAndReports(t *testing.T) {
	s := newStub(t)
	s.enqueue("ok", 0)
	s.enqueue("bad", 0)
	f := executor.NewFake()
	f.Script("bad", executor.Outcome{ExitCode: 3})
	a := startAgent(t, s, f, 2)

	waitFor(t, func() bool { return len(s.completed()) == 2 }, "two completions")
	got := map[string]completeRequest{}
	for _, c := range s.completed() {
		got[c.JobID] = c.Req
	}
	if r := got["j-ok"]; r.Status != statusSucceeded || r.RunnerID != "id-r1" || r.Attempt != 1 {
		t.Errorf("ok: %+v", r)
	}
	if r := got["j-bad"]; r.Status != statusFailed || r.FailureKind != kindExitCode || r.ExitCode == nil || *r.ExitCode != 3 {
		t.Errorf("bad: %+v", r)
	}
	s.mu.Lock()
	regs, first := s.registers, s.claims[0]
	s.mu.Unlock()
	if len(regs) != 1 || regs[0].Name != "r1" || regs[0].Capacity != 2 || regs[0].Labels["os"] != "test" {
		t.Errorf("registrations = %+v", regs)
	}
	if first.RunnerID != "id-r1" || first.Name != "r1" || first.Capacity != 2 {
		t.Errorf("first claim = %+v", first)
	}
	if a.RunnerID() != "id-r1" {
		t.Errorf("RunnerID() = %q", a.RunnerID())
	}
	if ex := f.Executions(); len(ex) != 2 || ex[0].Spec.Job.Image != "alpine" {
		t.Errorf("executions = %+v", ex)
	}
}

func TestAgentRetriesRegisterUntilServerIsUp(t *testing.T) {
	// The server answers 503 to the first two registrations, as a control
	// plane still starting would; the agent keeps trying, then claims.
	s := newStub(t)
	s.enqueue("ok", 0)
	var failures atomic.Int32
	failures.Store(2)
	orig := s.srv.Config.Handler
	s.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/runner/register" && failures.Add(-1) >= 0 {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		orig.ServeHTTP(w, r)
	})
	a := startAgent(t, s, executor.NewFake(), 1)

	waitFor(t, func() bool { return len(s.completed()) == 1 }, "completion")
	if a.RunnerID() != "id-r1" || s.completed()[0].Req.RunnerID != "id-r1" {
		t.Fatalf("runner id = %q, complete = %+v", a.RunnerID(), s.completed()[0].Req)
	}
}

func TestAgentRegisterRejectionIsFatal(t *testing.T) {
	s := newStub(t)
	s.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusBadRequest)
	})
	a, err := New(testConfig(s, 1), executor.NewFake())
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Run(context.Background()); err == nil {
		t.Fatal("Run returned nil after a 400 on register")
	}
}

func TestAgentHonoursCapacity(t *testing.T) {
	s := newStub(t)
	s.enqueue("a", 0)
	s.enqueue("b", 0)
	f := executor.NewFake()
	f.Script("a", executor.Outcome{Hang: true})
	f.Script("b", executor.Outcome{Hang: true})
	startAgent(t, s, f, 1)

	waitFor(t, func() bool { return len(f.Executions()) == 1 }, "first job to start")
	// A full runner stops polling, so let several heartbeats (which only
	// happen while a job is active) mark time before checking nothing else
	// was claimed.
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.heartbeats) >= 5 }, "heartbeats")
	s.mu.Lock()
	claims := len(s.claims)
	s.mu.Unlock()
	if n := len(f.Executions()); n != 1 || claims != 1 {
		t.Fatalf("%d jobs running, %d claims with capacity 1", n, claims)
	}
	f.Release("a")
	waitFor(t, func() bool { return len(f.Executions()) == 2 }, "second job to start after first ends")
	f.Release("b")
	waitFor(t, func() bool { return len(s.completed()) == 2 }, "both completions")
}

func TestAgentAbortDirectiveKillsAndDiscards(t *testing.T) {
	s := newStub(t)
	s.enqueue("hang", 0)
	f := executor.NewFake()
	f.Script("hang", executor.Outcome{Hang: true})
	a := startAgent(t, s, f, 1)

	waitFor(t, func() bool { return len(f.Executions()) == 1 }, "job to start")
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.heartbeats) >= 1 }, "a heartbeat")
	s.mu.Lock()
	hb := s.heartbeats[0]
	s.directive = "abort"
	s.mu.Unlock()
	if len(hb) != 1 || hb[0].JobID != "j-hang" || hb[0].Attempt != 1 {
		t.Fatalf("heartbeat refs = %+v", hb)
	}

	waitFor(t, func() bool { return f.Executions()[0].Err != nil }, "executor to be killed")
	if err := f.Executions()[0].Err; !errors.Is(err, context.Canceled) {
		t.Fatalf("executor err = %v", err)
	}
	// The attempt is forgotten (any report happens before that) and was
	// never completed.
	waitFor(t, func() bool { return len(a.activeRefs()) == 0 }, "attempt to be forgotten")
	if c := s.completed(); len(c) != 0 {
		t.Fatalf("aborted attempt was reported: %+v", c)
	}
}

func TestAgentCancelDirectiveReportsCancelled(t *testing.T) {
	s := newStub(t)
	s.enqueue("hang", 0)
	s.directive = "cancel"
	f := executor.NewFake()
	f.Script("hang", executor.Outcome{Hang: true})
	startAgent(t, s, f, 1)

	waitFor(t, func() bool { return len(s.completed()) == 1 }, "completion")
	if r := s.completed()[0].Req; r.Status != statusFailed || r.FailureKind != kindCancelled {
		t.Fatalf("got %+v", r)
	}
}

func TestAgentTimeoutReportsTimeout(t *testing.T) {
	s := newStub(t)
	s.enqueue("slow", 10*time.Millisecond)
	f := executor.NewFake()
	f.Script("slow", executor.Outcome{Hang: true})
	startAgent(t, s, f, 1)

	waitFor(t, func() bool { return len(s.completed()) == 1 }, "completion")
	if r := s.completed()[0].Req; r.Status != statusFailed || r.FailureKind != kindTimeout {
		t.Fatalf("got %+v", r)
	}
}

// An executor can return an exit result as cancellation races with its
// completion. The context cause must still decide cancellation/timeout.
type exitAfterContext struct{ code int }

func (e exitAfterContext) Run(ctx context.Context, _ executor.JobSpec, _ io.Writer) (executor.Result, error) {
	<-ctx.Done()
	return executor.Result{ExitCode: e.code}, nil
}

func TestAgentCancelAndTimeoutCauseOverridesExitResult(t *testing.T) {
	for _, cause := range []struct {
		name, directive, kind string
		timeout               time.Duration
	}{
		{name: "cancel", directive: "cancel", kind: kindCancelled},
		{name: "timeout", timeout: 10 * time.Millisecond, kind: kindTimeout},
	} {
		for _, result := range []struct {
			name string
			code int
		}{{"success", 0}, {"killed", 137}} {
			t.Run(cause.name+"/"+result.name, func(t *testing.T) {
				s := newStub(t)
				s.enqueue("race", cause.timeout)
				s.directive = cause.directive
				startAgent(t, s, exitAfterContext{code: result.code}, 1)
				waitFor(t, func() bool { return len(s.completed()) == 1 }, "completion")
				if r := s.completed()[0].Req; r.Status != statusFailed || r.FailureKind != cause.kind || r.ExitCode != nil {
					t.Fatalf("got %+v, want failed(%s) without an exit code", r, cause.kind)
				}
			})
		}
	}
}

func TestAgentInfraErrorAndCompleteRetry(t *testing.T) {
	s := newStub(t)
	s.enqueue("infra", 0)
	s.mu.Lock()
	s.completeStatus = []int{500, 503, 200}
	s.mu.Unlock()
	f := executor.NewFake()
	f.Script("infra", executor.Outcome{Err: executor.ErrInfra})
	startAgent(t, s, f, 1)

	waitFor(t, func() bool { return len(s.completed()) == 3 }, "three complete attempts")
	for _, c := range s.completed() {
		if c.Req.Status != statusFailed || c.Req.FailureKind != kindInfra {
			t.Fatalf("got %+v", c.Req)
		}
	}
}

func TestAgentFencedCompleteIsNotRetried(t *testing.T) {
	s := newStub(t)
	s.enqueue("ok", 0)
	s.mu.Lock()
	s.completeStatus = []int{409, 200}
	s.mu.Unlock()
	startAgent(t, s, executor.NewFake(), 1)

	waitFor(t, func() bool { return len(s.completed()) == 1 }, "one complete")
	// Several more polls pass without a second delivery.
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.claims) >= 5 }, "polls")
	if n := len(s.completed()); n != 1 {
		t.Fatalf("409 was retried: %d completes", n)
	}
}

func TestAgentShutdownReportsInfra(t *testing.T) {
	s := newStub(t)
	s.enqueue("hang", 0)
	f := executor.NewFake()
	f.Script("hang", executor.Outcome{Hang: true})
	a, err := New(testConfig(s, 1), f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = a.Run(ctx) }()

	waitFor(t, func() bool { return len(f.Executions()) == 1 }, "job to start")
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	c := s.completed()
	if len(c) != 1 || c[0].Req.FailureKind != kindInfra || c[0].Req.Error != errShutdown.Error() {
		t.Fatalf("completes = %+v", c)
	}
}

func (s *stubServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	var req logsRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	defer s.mu.Unlock()
	id := r.PathValue("id")
	s.ops[id] = append(s.ops[id], "logs")
	if s.logsStatus != 0 {
		http.Error(w, "scripted", s.logsStatus)
		return
	}
	if req.RunnerID != "id-r1" || req.Attempt != 1 {
		s.t.Errorf("log POST for %s carries runner %q attempt %d", id, req.RunnerID, req.Attempt)
	}
	s.logs[id] = append(s.logs[id], req.Chunks...)
	w.WriteHeader(http.StatusNoContent)
}

// logText joins job id's chunks in seq order, failing on a gap or a
// duplicate: the shipper's seqs are 1-based and gapless.
func (s *stubServer) logText(t *testing.T, id string) string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []byte
	for i, c := range s.logs[id] {
		if c.Seq != int64(i+1) {
			t.Fatalf("chunk %d of %s has seq %d", i, id, c.Seq)
		}
		out = append(out, c.Data...)
	}
	return string(out)
}

func (s *stubServer) opsFor(id string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ops[id]...)
}

func TestAgentShipsLogsBeforeCompleteAndNeverAfter(t *testing.T) {
	s := newStub(t)
	s.enqueue("chatty", 0)
	s.enqueue("quiet", 0)
	f := executor.NewFake()
	// 200 KB spans several 64 KB chunks; the 1 h interval means only the
	// size threshold and the close-before-complete flush can ship them.
	f.Script("chatty", executor.Outcome{LogBytes: 200_000})
	a, err := New(func() Config {
		c := testConfig(s, 2)
		c.LogFlushInterval = time.Hour
		return c
	}(), f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = a.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, func() bool { return len(s.completed()) == 2 }, "two completions")
	// Stop the agent so nothing can still be in flight, then check order.
	cancel()
	<-done

	text := s.logText(t, "j-chatty")
	if len(text) != 200_000 || !strings.HasPrefix(text, "fake executor output line\n") {
		t.Fatalf("chatty log: %d bytes, prefix %q", len(text), text[:min(len(text), 30)])
	}
	ops := s.opsFor("j-chatty")
	if len(ops) < 2 || ops[len(ops)-1] != "complete" {
		t.Fatalf("ops for chatty = %v: complete must be last", ops)
	}
	for _, op := range ops[:len(ops)-1] {
		if op != "logs" {
			t.Fatalf("ops for chatty = %v", ops)
		}
	}
	// A job with no output ships nothing at all.
	if ops := s.opsFor("j-quiet"); len(ops) != 1 || ops[0] != "complete" {
		t.Fatalf("ops for quiet = %v", ops)
	}
}

func TestAgentLogs409AbortsAttempt(t *testing.T) {
	s := newStub(t)
	s.mu.Lock()
	s.logsStatus = http.StatusConflict
	s.mu.Unlock()
	s.enqueue("hang", 0)
	f := executor.NewFake()
	// Output before hanging: the first flush meets the 409 while the job
	// is still running, which must kill it like a heartbeat abort.
	f.Script("hang", executor.Outcome{Hang: true, LogBytes: 10})
	a, err := New(func() Config {
		c := testConfig(s, 1)
		c.LogFlushInterval = time.Millisecond
		return c
	}(), f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = a.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, func() bool { ex := f.Executions(); return len(ex) == 1 && ex[0].Err != nil }, "executor to be killed")
	if err := f.Executions()[0].Err; !errors.Is(err, context.Canceled) {
		t.Fatalf("executor err = %v", err)
	}
	waitFor(t, func() bool { return len(a.activeRefs()) == 0 }, "attempt to be forgotten")
	if c := s.completed(); len(c) != 0 {
		t.Fatalf("stale attempt was reported: %+v", c)
	}
	if ops := s.opsFor("j-hang"); len(ops) != 1 {
		t.Fatalf("ops = %v: exactly one log POST (the 409) and nothing after", ops)
	}
}

func TestAgentLogs409AfterFinishDoesNotSuppressComplete(t *testing.T) {
	// The job exits on its own; only its final flush meets a 409. That
	// 409 belongs to a finished execution and must not become an abort:
	// the result is still reported (the server will fence it itself).
	s := newStub(t)
	s.mu.Lock()
	s.logsStatus = http.StatusConflict
	s.mu.Unlock()
	s.enqueue("out", 0)
	f := executor.NewFake()
	f.Script("out", executor.Outcome{LogBytes: 10, ExitCode: 0})
	a, err := New(func() Config {
		c := testConfig(s, 1)
		c.LogFlushInterval = time.Hour
		return c
	}(), f)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = a.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, func() bool { return len(s.completed()) == 1 }, "completion")
	if c := s.completed()[0]; c.Req.Status != statusSucceeded {
		t.Fatalf("complete = %+v", c.Req)
	}
	if ops := s.opsFor("j-out"); len(ops) != 2 || ops[0] != "logs" || ops[1] != "complete" {
		t.Fatalf("ops = %v", ops)
	}
}
