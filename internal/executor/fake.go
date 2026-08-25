package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Outcome scripts what FakeExecutor does for a job. The zero value
// succeeds immediately with no output.
type Outcome struct {
	// ExitCode is returned when the job "runs". Non-zero means failed.
	ExitCode int
	// Err, when set, is returned instead of a Result: the platform could
	// not run the job (infra failure).
	Err error
	// Delay is how long the job runs before returning; the fake honours
	// ctx cancellation during the wait.
	Delay time.Duration
	// Hang, when true, makes Run block until ctx is cancelled or Release is
	// called for the job, whichever comes first.
	Hang bool
	// LogBytes is how many bytes of output the job writes before ending.
	LogBytes int
}

// Execution records one Run call.
type Execution struct {
	Spec JobSpec
	// Err is what Run returned: nil, the scripted Err, or ctx.Err().
	Err error
}

// FakeExecutor is a scripted Executor for tests and the in-process
// harness. Outcomes are keyed by job name; jobs with no script succeed.
// It is safe for concurrent use.
type FakeExecutor struct {
	mu       sync.Mutex
	outcomes map[string]Outcome
	execs    []Execution
	release  map[string]chan struct{} // job name -> closed by Release
}

// ErrInfra is a convenient scripted infra failure.
var ErrInfra = errors.New("fake executor: infra failure")

// NewFake returns an executor with no scripts: every job succeeds at once.
func NewFake() *FakeExecutor {
	return &FakeExecutor{outcomes: map[string]Outcome{}, release: map[string]chan struct{}{}}
}

// Script sets the outcome for every job named name.
func (f *FakeExecutor) Script(name string, o Outcome) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outcomes[name] = o
}

// Release unblocks a hanging job named name. Calling it before the job
// starts is fine: the job then returns as soon as it is run.
func (f *FakeExecutor) Release(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := f.releaseChan(name)
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// releaseChan must be called with mu held.
func (f *FakeExecutor) releaseChan(name string) chan struct{} {
	ch, ok := f.release[name]
	if !ok {
		ch = make(chan struct{})
		f.release[name] = ch
	}
	return ch
}

// Executions returns a copy of every Run call so far, in start order.
func (f *FakeExecutor) Executions() []Execution {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Execution, len(f.execs))
	copy(out, f.execs)
	return out
}

// Run implements Executor.
func (f *FakeExecutor) Run(ctx context.Context, spec JobSpec, logs io.Writer) (Result, error) {
	f.mu.Lock()
	o := f.outcomes[spec.Job.Name]
	idx := len(f.execs)
	f.execs = append(f.execs, Execution{Spec: spec})
	release := f.releaseChan(spec.Job.Name)
	f.mu.Unlock()

	start := time.Now()
	res, err := f.run(ctx, o, release, logs)
	res.Duration = time.Since(start)

	f.mu.Lock()
	f.execs[idx].Err = err
	f.mu.Unlock()
	return res, err
}

func (f *FakeExecutor) run(ctx context.Context, o Outcome, release <-chan struct{}, logs io.Writer) (Result, error) {
	if o.LogBytes > 0 {
		if err := writeBytes(logs, o.LogBytes); err != nil {
			return Result{}, err
		}
	}
	if o.Hang {
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-release:
		}
	}
	if o.Delay > 0 {
		t := time.NewTimer(o.Delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-t.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if o.Err != nil {
		return Result{}, o.Err
	}
	return Result{ExitCode: o.ExitCode}, nil
}

// writeBytes emits n bytes of line-oriented filler.
func writeBytes(w io.Writer, n int) error {
	const line = "fake executor output line\n"
	for n > 0 {
		chunk := line
		if n < len(chunk) {
			chunk = chunk[:n]
		}
		if _, err := fmt.Fprint(w, chunk); err != nil {
			return err
		}
		n -= len(chunk)
	}
	return nil
}
