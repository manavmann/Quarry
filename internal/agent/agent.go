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
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"quarry/internal/executor"
	"quarry/internal/logship"
	"quarry/internal/metrics"
)

// Defaults for Config's zero values.
const (
	DefaultPollInterval      = time.Second
	DefaultHeartbeatInterval = 5 * time.Second
	DefaultCapacity          = 2
	DefaultCompleteRetries   = 8
	DefaultCompleteBackoff   = 500 * time.Millisecond
	DefaultLogFlushTimeout   = 30 * time.Second
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

	// LogFlushInterval and LogFlushBytes tune the per-attempt log shipper
	// (logship.Config); LogFlushTimeout bounds delivery after runner shutdown.
	// While alive, final flush and completion wait through connection loss.
	// Zero values take logship's / the agent's defaults.
	LogFlushInterval time.Duration
	LogFlushBytes    int
	LogFlushTimeout  time.Duration

	// Logger receives one line per notable event; nil means slog.Default().
	// Lines carry runner_id once registered and run_id/job_id/attempt
	// inside an attempt.
	Logger *slog.Logger
	// Metrics is the set this runner updates; nil means a private one.
	Metrics *metrics.Runner
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
	runnerID string       // assigned by register; "" until then
	log      *slog.Logger // cfg.Logger, plus runner_id once registered
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
	cfg.CompleteBackoff = min(cfg.CompleteBackoff, maxCompleteBackoff)
	if cfg.LogFlushTimeout <= 0 {
		cfg.LogFlushTimeout = DefaultLogFlushTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewRunner()
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Agent{
		cfg:    cfg,
		log:    cfg.Logger,
		exec:   exec,
		client: &client{base: cfg.ServerURL, token: cfg.Token, http: cfg.HTTPClient, backoff: cfg.CompleteBackoff},
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

// logger is the agent-wide logger, carrying runner_id once registered.
func (a *Agent) logger() *slog.Logger {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.log
}

// Run registers, then polls for work until ctx is cancelled, then kills
// every running attempt, reports each as an infra failure so it can be
// retried, and returns once every goroutine it started has ended. Run is
// not reentrant.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.register(ctx); err != nil {
		return err
	}
	// Run owns this shutdown watcher. Delivery has no outage deadline while
	// the runner is alive; after shutdown it gets LogFlushTimeout to drain.
	deliveryCtx, stopDelivery := context.WithCancel(context.Background())
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		select {
		case <-deliveryCtx.Done():
			return
		case <-ctx.Done():
		}
		t := time.NewTimer(a.cfg.LogFlushTimeout)
		defer t.Stop()
		select {
		case <-deliveryCtx.Done():
		case <-t.C:
			stopDelivery()
		}
	}()
	// The heartbeat outlives ctx so attempts still draining keep their
	// leases; it stops once the last job goroutine has reported.
	hbCtx, stopHB := context.WithCancel(context.Background())
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		a.heartbeatLoop(hbCtx)
	}()

	a.pollLoop(ctx, deliveryCtx)
	a.jobs.Wait()
	stopDelivery()
	<-drainDone
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
			a.log = a.log.With("runner_id", id)
			a.mu.Unlock()
			a.logger().Info("registered", "name", a.cfg.Name)
			return nil
		}
		var se *statusError
		if errors.As(err, &se) && !se.Retryable() {
			return fmt.Errorf("register: %w", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a.logger().Warn("register failed, retrying", "err", err, "retry_in", backoff.String())
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
func (a *Agent) pollLoop(ctx, deliveryCtx context.Context) {
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
			a.logger().Warn("claim failed", "err", err)
		case job != nil:
			a.start(ctx, deliveryCtx, job) // the goroutine releases the slot
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
func (a *Agent) start(ctx, deliveryCtx context.Context, job *claimedJob) {
	a.jobs.Add(1)
	go func() {
		defer a.jobs.Done()
		defer func() { <-a.sem }()
		a.runAttempt(ctx, deliveryCtx, job)
	}()
}

// runAttempt executes one attempt and reports its result. The executor's
// context is cancelled by timeout, by a heartbeat directive, or by runner
// shutdown; context.Cause says which, and that decides what is reported.
func (a *Agent) runAttempt(ctx, deliveryCtx context.Context, job *claimedJob) {
	key := attemptKey{job.ID, job.Attempt}
	logger := a.logger().With("run_id", job.RunID, "job_id", job.ID, "attempt", job.Attempt)
	a.cfg.Metrics.ActiveJobs.Inc()
	defer a.cfg.Metrics.ActiveJobs.Dec()

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
		logger.Error("attempt failed before start", "err", err)
		a.report(deliveryCtx, job, logger, completeRequest{Status: statusFailed, FailureKind: kindInfra, Error: err.Error()})
		return
	}
	spec := executor.JobSpec{JobID: job.ID, RunID: job.RunID, Attempt: job.Attempt, Job: pj}
	if len(pj.Artifacts) > 0 {
		// The executor fills this directory on success; it is uploaded
		// from and removed here, whatever happens.
		dir, err := os.MkdirTemp("", "quarry-artifacts-")
		if err != nil {
			logger.Error("attempt failed before start", "err", err)
			a.report(deliveryCtx, job, logger, completeRequest{Status: statusFailed, FailureKind: kindInfra, Error: "artifact dir: " + err.Error()})
			return
		}
		defer os.RemoveAll(dir)
		spec.ArtifactDir = dir
	}

	ectx := jctx
	if pj.Timeout > 0 {
		var tcancel context.CancelFunc
		ectx, tcancel = context.WithTimeoutCause(jctx, pj.Timeout, errTimeout)
		defer tcancel()
	}

	// The executor streams into a shipper for this attempt. A 409 on a
	// chunk means the attempt was superseded: kill it, exactly as a
	// heartbeat abort would.
	shipper := logship.New(logship.Config{
		FlushInterval: a.cfg.LogFlushInterval, FlushBytes: a.cfg.LogFlushBytes,
		OnStale: func() { cancel(errAbort) },
	}, a.logSink(job))
	res, execErr := a.exec.Run(ectx, spec, shipper)

	// Artifacts go up before the verdict is fixed and before complete,
	// while the attempt is still the running one; every upload carries the
	// attempt and is fenced like a log chunk. A failed upload fails the
	// attempt as infra (or as whatever ended the context meanwhile), and a
	// 409 means the attempt was superseded: it is aborted like a stale log
	// batch would abort it.
	if execErr == nil && res.ExitCode == 0 && spec.ArtifactDir != "" {
		if err := a.uploadArtifacts(ectx, job, spec.ArtifactDir, shipper); err != nil {
			if errors.Is(err, errFenced) {
				cancel(errAbort)
			}
			execErr = fmt.Errorf("upload artifacts: %w", err)
		}
	}

	// The verdict is fixed here, before the final flush: whatever the
	// shipper learns from now on (a 409 on the tail, say) belongs to a
	// finished execution and must not turn into an abort.
	cause := context.Cause(ectx)
	aborted := execErr != nil && errors.Is(cause, errAbort)

	// Flush every buffered chunk synchronously; complete never precedes
	// the last chunk. An aborted attempt has nothing the server would
	// accept, so its buffer is just discarded.
	flushCtx, stopFlush := context.WithCancel(deliveryCtx)
	if aborted {
		stopFlush()
	}
	if err := shipper.Close(flushCtx); err != nil && !aborted && !errors.Is(err, logship.ErrStale) {
		logger.Warn("final log flush failed", "err", err)
	}
	stopFlush()

	var req completeRequest
	switch {
	case errors.Is(cause, errTimeout):
		req = completeRequest{Status: statusFailed, FailureKind: kindTimeout, Error: errTimeout.Error()}
	case errors.Is(cause, errCancelled):
		req = completeRequest{Status: statusFailed, FailureKind: kindCancelled, Error: errCancelled.Error()}
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
	case aborted:
		logger.Info("aborted by server, result discarded")
		return
	case ctx.Err() != nil:
		req = completeRequest{Status: statusFailed, FailureKind: kindInfra, Error: errShutdown.Error()}
	default:
		// The executor could not run the job (or the upload failed): the
		// runner-side error, as opposed to a job that ran and failed.
		a.cfg.Metrics.ExecutorErrors.Inc()
		logger.Error("executor error", "err", execErr)
		req = completeRequest{Status: statusFailed, FailureKind: kindInfra, Error: execErr.Error()}
	}
	a.report(deliveryCtx, job, logger, req)
}

// report uses Run's delivery context so executor cancellation cannot lose
// a verdict, while runner shutdown still bounds the reconnect wait.
func (a *Agent) report(ctx context.Context, job *claimedJob, logger *slog.Logger, req completeRequest) {
	req.RunnerID = a.RunnerID()
	req.Attempt = job.Attempt
	err := a.client.completeWithRetry(ctx, job.ID, req, a.cfg.CompleteRetries, a.cfg.CompleteBackoff)
	switch {
	case err == nil:
	case errors.Is(err, errFenced):
		logger.Warn("result rejected, attempt superseded", "err", err)
	default:
		logger.Error("report failed", "status", req.Status, "err", err)
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
				a.logger().Warn("heartbeat failed", "err", err)
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

// uploadArtifacts posts every regular file under dir as an artifact of
// job's attempt, one request per file with Content-Length set, in lexical
// path order. Transport errors and 5xx replies are retried with the
// completion backoff; a 409 (errFenced) and other 4xx are final. One line
// per file goes to logs so the job's output records what was kept.
func (a *Agent) uploadArtifacts(ctx context.Context, job *claimedJob, dir string, logs io.Writer) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		path := filepath.ToSlash(rel)
		backoff := a.cfg.CompleteBackoff
		for i := 0; ; i++ {
			err = a.client.uploadArtifact(ctx, job.ID, job.Attempt, path, p)
			var se *statusError
			if err == nil || errors.Is(err, errFenced) || (errors.As(err, &se) && !se.Retryable()) || ctx.Err() != nil || i+1 >= a.cfg.CompleteRetries {
				break
			}
			if !a.wait(ctx, backoff) {
				break
			}
			if backoff *= 2; backoff > maxCompleteBackoff {
				backoff = maxCompleteBackoff
			}
		}
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if info, ierr := d.Info(); ierr == nil {
			fmt.Fprintf(logs, "[quarry] uploaded artifact %s (%d bytes)\n", path, info.Size())
		}
		return nil
	})
}

// logSink is the shipper's delivery function for one attempt: each batch
// is one POST /api/runner/jobs/{id}/logs carrying this runner's id.
func (a *Agent) logSink(job *claimedJob) logship.Sink {
	return func(ctx context.Context, chunks []logship.Chunk) error {
		req := logsRequest{RunnerID: a.RunnerID(), Attempt: job.Attempt, Chunks: make([]logChunk, 0, len(chunks))}
		for _, c := range chunks {
			req.Chunks = append(req.Chunks, logChunk{Seq: c.Seq, Data: c.Data})
		}
		return a.client.logs(ctx, job.ID, req)
	}
}
