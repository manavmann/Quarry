// Package executor defines how a runner turns a claimed job into a
// process. The interface is deliberately one method: the runner kills a
// job by cancelling the context, never through a second call, so every
// implementation has exactly one place to honour cancellation.
package executor

import (
	"context"
	"io"
	"time"

	"quarry/internal/pipeline"
)

// JobSpec is what an Executor is asked to run: the pipeline job plus the
// identity of the attempt, which real executors use to name containers.
type JobSpec struct {
	JobID   string
	RunID   string
	Attempt int
	Job     pipeline.Job
	// ArtifactDir, when set, is an empty directory owned by the runner
	// into which a successful Run copies the files of every declared
	// artifact, laid out as <ArtifactDir>/<artifact path>/<files>. The
	// runner uploads what it finds there and removes the directory; an
	// executor never uploads. Empty means the job declares no artifacts.
	ArtifactDir string
}

// Result is how a finished job ended. A non-zero ExitCode is the
// protocol's exit_code failure; timeout and cancelled come from the
// context, infra from a non-nil error, never from Result.
type Result struct {
	ExitCode int
	// OOMKilled is set when the platform killed the job for exceeding
	// its memory limit; ExitCode is then the kill's (137).
	OOMKilled bool
	// Duration is wall time from container start to exit.
	Duration time.Duration
}

// Executor runs one job attempt to completion and returns its result.
//
// Run writes the job's combined stdout/stderr to logs as it happens. It
// returns a nil error with the job's exit code when the job ran, or an
// error when the platform could not run it at all (image pull failed,
// daemon unreachable): the runner reports the former as exit_code and the
// latter as infra. When ctx is cancelled Run kills the job and returns
// ctx.Err(); the runner reports timeout or cancelled from the context's
// cause, never from Run.
type Executor interface {
	Run(ctx context.Context, spec JobSpec, logs io.Writer) (Result, error)
}
