// Package harness runs a whole Quarry deployment inside one test process:
// a control plane on a temp database and an ephemeral port, plus N agents
// driven by scripted FakeExecutors. Tests submit pipelines through the
// real HTTP API and wait on outcomes with deadline polling; nothing here
// sleeps for a fixed time.
//
// Goroutines: each agent's Run loop is owned by the Harness and stopped in
// t.Cleanup (or by Kill); the server's are owned by httptest.Server.
package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"quarry/internal/agent"
	"quarry/internal/api"
	"quarry/internal/executor"
	"quarry/internal/scheduler"
	"quarry/internal/store"
)

// Token is the bearer token the harness server accepts.
const Token = "harness-token"

// Opts sizes and times a harness. Zero values take the defaults below.
type Opts struct {
	// Agents is how many runners to start. Default 1.
	Agents int
	// Capacity is each runner's concurrent job slots. Default 2.
	Capacity int
	// Labels, when set, gives runner i its labels; nil means none.
	Labels func(i int) map[string]string
	// PollInterval is how often an idle runner claims. Default 10ms.
	PollInterval time.Duration
	// HeartbeatInterval is how often a busy runner heartbeats. Default 20ms.
	HeartbeatInterval time.Duration
	// LeaseTTL is the scheduler's lease duration. Default 30s; the clock
	// is fake, so leases only expire when a test advances it.
	LeaseTTL time.Duration
	// CompleteBackoff is the runner's first completion retry delay.
	// Default 5ms.
	CompleteBackoff time.Duration
}

// Harness is a running server plus its agents. Methods must be called
// from the test goroutine except Client, Clock and the executors, which
// are safe from anywhere.
type Harness struct {
	t     *testing.T
	opts  Opts
	log   *log.Logger
	st    *store.Store
	srv   *httptest.Server
	http  *http.Client
	clock *Clock
	execs []*executor.FakeExecutor
	ags   []*runner
}

// runner is one agent and the harness state around it. exec and name
// outlive Kill so Restart brings the same runner back under the same
// registered id.
type runner struct {
	name  string
	exec  *executor.FakeExecutor
	agent *agent.Agent
	stop  context.CancelFunc
	done  <-chan struct{}
}

// Clock is the fake time source injected into the store (and so the
// scheduler). It only moves when a test calls Advance.
type Clock struct{ ms atomic.Int64 }

// Now is the current fake time in Unix milliseconds.
func (c *Clock) Now() int64 { return c.ms.Load() }

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

// New starts the server and opts.Agents agents and returns once the
// server answers /healthz. Everything is torn down by t.Cleanup.
func New(t *testing.T, opts Opts) *Harness {
	t.Helper()
	if opts.Agents == 0 {
		opts.Agents = 1
	}
	if opts.Capacity == 0 {
		opts.Capacity = 2
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = 10 * time.Millisecond
	}
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = 20 * time.Millisecond
	}
	if opts.LeaseTTL == 0 {
		opts.LeaseTTL = 30 * time.Second
	}
	if opts.CompleteBackoff == 0 {
		opts.CompleteBackoff = 5 * time.Millisecond
	}
	h := &Harness{
		t: t, opts: opts, log: log.New(io.Discard, "", 0),
		clock: &Clock{}, http: &http.Client{Timeout: 10 * time.Second},
	}
	h.clock.ms.Store(1_700_000_000_000)
	// TempDir registers its own cleanup; ours must run first so the store
	// is closed before the directory is removed.
	dir := t.TempDir()
	t.Cleanup(h.close)

	st, err := store.Open(context.Background(), filepath.Join(dir, "quarry.db"), store.WithClock(h.clock.Now))
	if err != nil {
		t.Fatalf("harness: open store: %v", err)
	}
	h.st = st
	h.srv = httptest.NewServer(api.New(st, api.Config{
		APIToken: Token, Logger: h.log, Scheduler: scheduler.Config{LeaseTTL: opts.LeaseTTL},
	}))
	WaitFor(t, func() bool {
		resp, err := h.http.Get(h.srv.URL + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, "server healthy")

	for i := 0; i < opts.Agents; i++ {
		r := &runner{name: "agent-" + strconv.Itoa(i), exec: executor.NewFake()}
		h.ags = append(h.ags, r)
		h.execs = append(h.execs, r.exec)
		h.startAgent(i)
	}
	return h
}

// close stops agents first so nothing is mid-request when the server
// goes, then the server, then the store.
func (h *Harness) close() {
	for i := range h.ags {
		h.stopAgent(i)
	}
	if h.srv != nil {
		h.srv.Close()
	}
	if h.st != nil {
		if err := h.st.Close(); err != nil {
			h.t.Errorf("harness: close store: %v", err)
		}
	}
}

func (h *Harness) startAgent(i int) {
	h.t.Helper()
	r := h.ags[i]
	var labels map[string]string
	if h.opts.Labels != nil {
		labels = h.opts.Labels(i)
	}
	a, err := agent.New(agent.Config{
		ServerURL: h.srv.URL, Token: Token, Name: r.name, Labels: labels,
		Capacity: h.opts.Capacity, Version: "harness",
		PollInterval: h.opts.PollInterval, HeartbeatInterval: h.opts.HeartbeatInterval,
		CompleteBackoff: h.opts.CompleteBackoff, Logger: h.log, HTTPClient: h.http,
	}, r.exec)
	if err != nil {
		h.t.Fatalf("harness: agent %d: %v", i, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.agent, r.stop, r.done = a, cancel, done
	go func() {
		defer close(done)
		_ = a.Run(ctx)
	}()
}

// stopAgent is a no-op for an agent that is already stopped.
func (h *Harness) stopAgent(i int) {
	r := h.ags[i]
	if r.stop == nil {
		return
	}
	r.stop()
	<-r.done
	r.stop, r.done = nil, nil
}

// Kill stops agent i: its running attempts are killed and reported as
// infra failures, then its loops end. Its id and executor are kept for
// Restart.
func (h *Harness) Kill(i int) {
	h.stopAgent(i)
}

// Restart brings agent i back with the same id and executor.
func (h *Harness) Restart(i int) {
	h.t.Helper()
	if h.ags[i].stop != nil {
		h.t.Fatalf("Restart(%d): agent is running", i)
	}
	h.startAgent(i)
}

// AgentName is the name agent i registers with.
func (h *Harness) AgentName(i int) string { return h.ags[i].name }

// AgentID is the runner_id the server assigned to agent i, waiting for
// registration to finish if it has not yet.
func (h *Harness) AgentID(i int) string {
	h.t.Helper()
	a := h.ags[i].agent
	var id string
	WaitFor(h.t, func() bool { id = a.RunnerID(); return id != "" }, "agent "+h.ags[i].name+" to register")
	return id
}

// Exec is agent i's executor, for scripting outcomes on one runner.
func (h *Harness) Exec(i int) *executor.FakeExecutor { return h.execs[i] }

// Script sets the outcome of job name on every agent, so it applies
// whichever runner claims the job.
func (h *Harness) Script(name string, o executor.Outcome) {
	for _, e := range h.execs {
		e.Script(name, o)
	}
}

// Release unblocks a hanging job on every agent.
func (h *Harness) Release(name string) {
	for _, e := range h.execs {
		e.Release(name)
	}
}

// Executions returns every Run call across all agents, tagged with the
// index of the agent that made it.
func (h *Harness) Executions() []Execution {
	var out []Execution
	for i, e := range h.execs {
		for _, x := range e.Executions() {
			out = append(out, Execution{Agent: i, Execution: x})
		}
	}
	return out
}

// Execution is one executor Run call and the agent it happened on.
type Execution struct {
	Agent int
	executor.Execution
}

// Clock is the fake clock the server runs on.
func (h *Harness) Clock() *Clock { return h.clock }

// Store is the server's store, for tests that need to inspect rows
// directly. Agents never touch it.
func (h *Harness) Store() *store.Store { return h.st }

// URL is the server's base URL.
func (h *Harness) URL() string { return h.srv.URL }

// Client talks to the running server over HTTP with the harness token.
func (h *Harness) Client() *Client { return &Client{h: h} }

// ---- API client -------------------------------------------------------

// Run, Job and Event are the subsets of the API's JSON shapes tests
// assert on.
type Run struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	CreatedAt  int64  `json:"created_at"`
	StartedAt  int64  `json:"started_at"`
	FinishedAt int64  `json:"finished_at"`
}

type Job struct {
	ID          string `json:"id"`
	RunID       string `json:"run_id"`
	Name        string `json:"name"`
	State       string `json:"state"`
	Attempt     int    `json:"attempt"`
	RunnerID    string `json:"runner_id"`
	FailureKind string `json:"failure_kind"`
	ExitCode    *int   `json:"exit_code"`
	Error       string `json:"error"`
}

type Event struct {
	ID        int64           `json:"id"`
	JobID     string          `json:"job_id"`
	Type      string          `json:"type"`
	Detail    json.RawMessage `json:"detail"`
	CreatedAt int64           `json:"created_at"`
}

// RunDetail is GET /api/runs/{id}.
type RunDetail struct {
	Run  Run   `json:"run"`
	Jobs []Job `json:"jobs"`
}

// Job returns the job named name, or nil.
func (d *RunDetail) Job(name string) *Job {
	for i := range d.Jobs {
		if d.Jobs[i].Name == name {
			return &d.Jobs[i]
		}
	}
	return nil
}

// Client is a thin typed wrapper over the user API. Its methods are safe
// from any goroutine.
type Client struct{ h *Harness }

// do sends one request and decodes a 2xx JSON body into out.
func (c *Client) do(method, path, body string, out any) error {
	req, err := http.NewRequest(method, c.h.srv.URL+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+Token)
	resp, err := c.h.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// Submit posts a .quarry.yml document and returns the created run.
func (c *Client) Submit(yaml string) (*RunDetail, error) {
	var d RunDetail
	if err := c.do(http.MethodPost, "/api/runs", yaml, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// GetRun fetches a run and its jobs.
func (c *Client) GetRun(id string) (*RunDetail, error) {
	var d RunDetail
	if err := c.do(http.MethodGet, "/api/runs/"+id, "", &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// LogChunk is one stored piece of an attempt's output.
type LogChunk struct {
	Seq  int64  `json:"seq"`
	Data []byte `json:"data"`
}

// Logs is GET /api/jobs/{id}/logs?after=: the chunks after the cursor and
// the cursor to continue from.
type Logs struct {
	Attempt int        `json:"attempt"`
	Chunks  []LogChunk `json:"chunks"`
	Next    int64      `json:"next"`
}

// Logs reads job id's log chunks with seq > after.
func (c *Client) Logs(id string, after int64) (*Logs, error) {
	var l Logs
	if err := c.do(http.MethodGet, "/api/jobs/"+id+"/logs?after="+strconv.FormatInt(after, 10), "", &l); err != nil {
		return nil, err
	}
	return &l, nil
}

// Events lists a run's events in id order.
func (c *Client) Events(id string) ([]Event, error) {
	var reply struct {
		Events []Event `json:"events"`
	}
	if err := c.do(http.MethodGet, "/api/runs/"+id+"/events", "", &reply); err != nil {
		return nil, err
	}
	return reply.Events, nil
}

// ---- test helpers -----------------------------------------------------

// Submit submits yaml and fails the test on error.
func (h *Harness) Submit(yaml string) *RunDetail {
	h.t.Helper()
	d, err := h.Client().Submit(yaml)
	if err != nil {
		h.t.Fatalf("submit: %v", err)
	}
	return d
}

// WaitRun polls until run id is terminal and returns it.
func (h *Harness) WaitRun(id string) *RunDetail {
	h.t.Helper()
	var last *RunDetail
	WaitFor(h.t, func() bool {
		d, err := h.Client().GetRun(id)
		if err != nil {
			return false
		}
		last = d
		return d.Run.State == store.RunSucceeded || d.Run.State == store.RunFailed || d.Run.State == store.RunCancelled
	}, "run "+id+" to finish")
	return last
}

// WaitJob polls until job name of run id is in state and returns it.
func (h *Harness) WaitJob(id, name, state string) *Job {
	h.t.Helper()
	var last *Job
	WaitFor(h.t, func() bool {
		d, err := h.Client().GetRun(id)
		if err != nil {
			return false
		}
		last = d.Job(name)
		return last != nil && last.State == state
	}, fmt.Sprintf("job %s of run %s to be %s", name, id, state))
	return last
}

// Events fetches run id's events and fails the test on error.
func (h *Harness) Events(id string) []Event {
	h.t.Helper()
	evs, err := h.Client().Events(id)
	if err != nil {
		h.t.Fatalf("events: %v", err)
	}
	return evs
}

// EventTypes is the sequence of event types for run id, for assertions.
func (h *Harness) EventTypes(id string) []string {
	h.t.Helper()
	evs := h.Events(id)
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Type)
	}
	return out
}

// WaitTimeout bounds every WaitFor.
const WaitTimeout = 10 * time.Second

// WaitFor polls cond every millisecond until it holds, failing the test
// after WaitTimeout.
func WaitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.NewTimer(WaitTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-deadline.C:
			t.Fatalf("timed out after %s waiting for %s", WaitTimeout, what)
		case <-tick.C:
		}
	}
}

// LogText reads the whole log of job id through the cursor API, one
// round-trip per chunk batch, and fails the test on error or on a seq gap.
func (h *Harness) LogText(id string) string {
	h.t.Helper()
	var out []byte
	var after int64
	var next int64 = 1
	for {
		l, err := h.Client().Logs(id, after)
		if err != nil {
			h.t.Fatalf("logs %s: %v", id, err)
		}
		if len(l.Chunks) == 0 {
			return string(out)
		}
		for _, c := range l.Chunks {
			if c.Seq != next {
				h.t.Fatalf("logs %s: seq %d after %d", id, c.Seq, next-1)
			}
			out = append(out, c.Data...)
			next++
		}
		if l.Next != l.Chunks[len(l.Chunks)-1].Seq {
			h.t.Fatalf("logs %s: next=%d, want %d", id, l.Next, l.Chunks[len(l.Chunks)-1].Seq)
		}
		after = l.Next
	}
}
