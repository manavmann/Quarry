// Package agent is the runner: it registers with the control plane, then
// claims and runs each claimed attempt through an Executor, heartbeats the
// attempts it holds and reports their results. It talks to the server over
// HTTP only and never sees the store.
//
// Goroutines and their owners:
//   - the poll loop runs on the goroutine that called Run, until ctx ends;
//   - Run owns the heartbeat goroutine and one goroutine per claimed
//     (job, attempt); all are joined before Run returns.
package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"quarry/internal/executor"
)

// Defaults for Config's zero values.
const (
	DefaultPollInterval      = time.Second
	DefaultHeartbeatInterval = 5 * time.Second
	DefaultCapacity          = 2
	DefaultCompleteRetries   = 8
	DefaultCompleteBackoff   = 500 * time.Millisecond
	maxCompleteBackoff       = 10 * time.Second
	// pollJitter is the fraction of PollInterval by which each wait varies,
	// so a fleet started together does not poll in lockstep.
	pollJitter = 0.25
)

// Statuses and failure kinds reported on complete (docs/protocol.md).
const (
	statusSucceeded = "succeeded"
	statusFailed    = "failed"
	kindExitCode    = "exit_code"
	kindTimeout     = "timeout"
	kindInfra       = "infra"
	kindCancelled   = "cancelled"
)

// Config is everything an Agent needs. Zero values take the defaults above.
type Config struct {
	ServerURL string
	Token     string
	// Name is the runner's identity: register resolves it to a runner_id,
	// and the same name always gets the same id back.
	Name     string
	Labels   map[string]string
	Capacity int
	Version  string

	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	CompleteRetries   int
	CompleteBackoff   time.Duration

	// Logger receives one line per notable event; nil means log.Default().
	Logger *log.Logger
	// HTTPClient defaults to a client with a 30s timeout.
	HTTPClient *http.Client
}

// Agent is one runner process's worth of state.
type Agent struct {
	cfg    Config
	exec   executor.Executor
	client *client
	sem    chan struct{} // one token per busy capacity slot

	mu       sync.Mutex
	runnerID string // assigned by register; "" until then
	active   map[attemptKey]context.CancelCauseFunc
	jobs     sync.WaitGroup
}

type attemptKey struct {
	jobID   string
	attempt int
}

// Cancellation causes: the job goroutine reads context.Cause to learn why
// its executor was killed and what, if anything, to report.
var (
	errTimeout   = errors.New("job timed out")
	errCancelled = errors.New("job cancelled by user")
	errAbort     = errors.New("attempt aborted by server")
	errShutdown  = errors.New("runner shutting down")
)

// New validates cfg and builds an Agent that runs jobs with exec.
func New(cfg Config, exec executor.Executor) (*Agent, error) {
	if cfg.ServerURL == "" || cfg.Name == "" {
		return nil, errors.New("agent: ServerURL and Name are required")
	}
	if exec == nil {
		return nil, errors.New("agent: executor is required")
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = DefaultCapacity
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.CompleteRetries <= 0 {
		cfg.CompleteRetries = DefaultCompleteRetries
	}
	if cfg.CompleteBackoff <= 0 {
		cfg.CompleteBackoff = DefaultCompleteBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Agent{
		cfg:    cfg,
		exec:   exec,
		client: &client{base: cfg.ServerURL, token: cfg.Token, http: cfg.HTTPClient},
		sem:    make(chan struct{}, cfg.Capacity),
		active: map[attemptKey]context.CancelCauseFunc{},
	}, nil
}

// RunnerID is the id the server assigned at registration, or "" before
// Run has registered.
func (a *Agent) RunnerID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.runnerID
}

// Run registers, then polls for work until ctx is cancelled, then kills
// every running attempt, reports each as an infra failure so it can be
// retried, and returns once every goroutine it started has ended. Run is
// not reentrant.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.register(ctx); err != nil {
		return err
	}
	// The heartbeat outlives ctx so attempts still draining keep their
	// leases; it stops once the last job goroutine has reported.
	hbCtx, stopHB := context.WithCancel(context.Background())
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		a.heartbeatLoop(hbCtx)
	}()

	a.pollLoop(ctx)
	a.jobs.Wait()
	stopHB()
	<-hbDone
	return nil
}

// register asks the server for this runner's id, retrying transport
// errors and 5xx replies with capped backoff until ctx ends: a runner
// started before its control plane just waits. A 4xx is final.
func (a *Agent) register(ctx context.Context) error {
	req := registerRequest{Name: a.cfg.Name, Labels: a.cfg.Labels, Capacity: a.cfg.Capacity}
	backoff := a.cfg.CompleteBackoff
	for {
		id, err := a.client.register(ctx, req)
		if err == nil {
			a.mu.Lock()
			a.runnerID = id
			a.mu.Unlock()
			a.cfg.Logger.Printf("registered as %s (runner_id %s)", a.cfg.Name, id)
			return nil
		}
		var se *statusError
		if errors.As(err, &se) && !se.Retryable() {
			return fmt.Errorf("register: %w", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a.cfg.Logger.Printf("register: %v (retrying in %s)", err, backoff)
		if !a.wait(ctx, backoff) {
			return ctx.Err()
		}
		if backoff *= 2; backoff > maxCompleteBackoff {
			backoff = maxCompleteBackoff
		}
	}
}

// pollLoop claims whenever a capacity slot is free. A successful claim is
// followed by another attempt at once; an empty reply or an error waits
// one jittered PollInterval.
func (a *Agent) pollLoop(ctx context.Context) {
	req := claimRequest{
		RunnerID: a.RunnerID(), Name: a.cfg.Name, Labels: a.cfg.Labels,
		Capacity: a.cfg.Capacity, Version: a.cfg.Version,
	}
	for {
		select {
		case <-ctx.Done():
			return
		case a.sem <- struct{}{}:
		}
		job, err := a.client.claim(ctx, req)
		switch {
		case err != nil:
			<-a.sem
			if ctx.Err() != nil {
				return
			}
			a.cfg.Logger.Printf("claim: %v", err)
		case job != nil:
			a.start(ctx, job) // the goroutine releases the slot
			continue
		default:
			<-a.sem
		}
		if !a.wait(ctx, a.jitter(a.cfg.PollInterval)) {
			return
		}
	}
}

// jitter returns d varied by ±pollJitter.
func (a *Agent) jitter(d time.Duration) time.Duration {
	f := 1 + pollJitter*(2*rand.Float64()-1)
	return time.Duration(float64(d) * f)
}

// wait blocks for d or until ctx ends, reporting whether ctx is still live.
func (a *Agent) wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// start spawns the goroutine for one claimed attempt. It owns the
// capacity slot pollLoop acquired and returns it when the attempt is done.
func (a *Agent) start(ctx context.Context, job *claimedJob) {
	a.jobs.Add(1)
	go func() {
		defer a.jobs.Done()
		defer func() { <-a.sem }()
		a.runAttempt(ctx, job)
	}()
}

// runAttempt executes one attempt and reports its result. The executor's
// context is cancelled by timeout, by a heartbeat directive, or by runner
// shutdown; context.Cause says which, and that decides what is reported.
func (a *Agent) runAttempt(ctx context.Context, job *claimedJob) {
	key := attemptKey{job.ID, job.Attempt}
	logger := a.cfg.Logger

	// jctx ends on shutdown (parent) or on a directive (cancel). The
	// executor gets the timeout-wrapped child.
	jctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	a.mu.Lock()
	a.active[key] = cancel
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.active, key)
		a.mu.Unlock()
	}()

	pj, err := job.spec()
	if err != nil {
		logger.Printf("job %s attempt %d: %v", job.ID, job.Attempt, err)
		a.report(job, completeRequest{Status: statusFailed, FailureKind: kindInfra, Error: err.Error()})
		return
	}
	spec := executor.JobSpec{JobID: job.ID, RunID: job.RunID, Attempt: job.Attempt, Job: pj}

	ectx := jctx
	if pj.Timeout > 0 {
		var tcancel context.CancelFunc
		ectx, tcancel = context.WithTimeoutCause(jctx, pj.Timeout, errTimeout)
		defer tcancel()
	}

	// Logs go nowhere until the log shipper (C08) lands.
	res, execErr := a.exec.Run(ectx, spec, io.Discard)

	var req completeRequest
	switch cause := context.Cause(ectx); {
	case execErr == nil:
		if res.ExitCode == 0 {
			req = completeRequest{Status: statusSucceeded}
		} else {
			code := res.ExitCode
			msg := fmt.Sprintf("exit code %d", code)
			if res.OOMKilled {
				msg += " (out of memory)"
			}
			req = completeRequest{Status: statusFailed, FailureKind: kindExitCode, ExitCode: &code, Error: msg}
		}
	case errors.Is(cause, errAbort):
		logger.Printf("job %s attempt %d: aborted by server, result discarded", job.ID, job.Attempt)
		return
	case errors.Is(cause, errTimeout):
		req = completeRequest{Status: statusFailed, FailureKind: kindTimeout, Error: errTimeout.Error()}
	case errors.Is(cause, errCancelled):
		req = completeRequest{Status: statusFailed, FailureKind: kindCancelled, Error: errCancelled.Error()}
	case ctx.Err() != nil:
		req = completeRequest{Status: statusFailed, FailureKind: kindInfra, Error: errShutdown.Error()}
	default:
		req = completeRequest{Status: statusFailed, FailureKind: kindInfra, Error: execErr.Error()}
	}
	a.report(job, req)
}

// report delivers a completion with retries. It uses its own context so a
// shutting-down runner still reports what it killed; the bound is the
// retry budget, not the caller's context.
func (a *Agent) report(job *claimedJob, req completeRequest) {
	req.RunnerID = a.RunnerID()
	req.Attempt = job.Attempt
	err := a.client.completeWithRetry(context.Background(), job.ID, req, a.cfg.CompleteRetries, a.cfg.CompleteBackoff)
	switch {
	case err == nil:
	case errors.Is(err, errFenced):
		a.cfg.Logger.Printf("job %s attempt %d: %v", job.ID, job.Attempt, err)
	default:
		a.cfg.Logger.Printf("job %s attempt %d: report %s: %v", job.ID, job.Attempt, req.Status, err)
	}
}

// heartbeatLoop extends the lease of every active attempt once per
// HeartbeatInterval and applies the server's directives. Idle runners do
// not heartbeat: claiming already refreshes last_seen_at.
func (a *Agent) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(a.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		refs := a.activeRefs()
		if len(refs) == 0 {
			continue
		}
		ds, err := a.client.heartbeat(ctx, a.RunnerID(), refs)
		if err != nil {
			if ctx.Err() == nil {
				a.cfg.Logger.Printf("heartbeat: %v", err)
			}
			continue
		}
		for _, d := range ds {
			a.apply(d)
		}
	}
}

func (a *Agent) activeRefs() []jobRef {
	a.mu.Lock()
	defer a.mu.Unlock()
	refs := make([]jobRef, 0, len(a.active))
	for k := range a.active {
		refs = append(refs, jobRef{JobID: k.jobID, Attempt: k.attempt})
	}
	return refs
}

// apply acts on one heartbeat directive. Directives for attempts that
// have already finished are ignored.
func (a *Agent) apply(d directive) {
	var cause error
	switch d.Directive {
	case "abort":
		cause = errAbort
	case "cancel":
		cause = errCancelled
	default:
		return
	}
	a.mu.Lock()
	cancel := a.active[attemptKey{d.JobID, d.Attempt}]
	a.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}
