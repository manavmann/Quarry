package store

// Run states.
const (
	RunPending   = "pending"
	RunRunning   = "running"
	RunSucceeded = "succeeded"
	RunFailed    = "failed"
	RunCancelled = "cancelled"
)

// Job states. A job is `pending` until every dependency succeeded, `queued`
// once claimable, `running` while a runner holds its lease, and terminal
// otherwise. `skipped` means a dependency failed or the run was cancelled
// before the job started.
const (
	JobPending   = "pending"
	JobQueued    = "queued"
	JobRunning   = "running"
	JobSucceeded = "succeeded"
	JobFailed    = "failed"
	JobCancelled = "cancelled"
	JobSkipped   = "skipped"
)

// Failure kinds, recorded in jobs.failure_kind when a job attempt fails.
const (
	FailureExitCode   = "exit_code"   // the container exited non-zero
	FailureTimeout    = "timeout"     // the job exceeded its timeout
	FailureInfra      = "infra"       // executor/image/setup error
	FailureLostRunner = "lost_runner" // the lease expired without completion
	FailureCancelled  = "cancelled"   // cancelled by a user
)

// Cancel reasons, recorded in jobs.cancel_reason when the server asks a
// runner to kill an attempt: who asked decides how the runner's
// `cancelled` report is classified.
const (
	CancelReasonUser    = "user"    // POST /api/runs/{id}/cancel → job cancelled
	CancelReasonTimeout = "timeout" // monitor backstop → job failed(timeout)
)

// Runner states.
const (
	RunnerOnline  = "online"
	RunnerOffline = "offline"
)

// Run is one execution of a pipeline. PipelineYAML is the raw .quarry.yml
// text the run was created from, so a run can always be re-inspected even
// if the source moves. CommitSHA and Ref are optional and stored as NULL
// when empty. Zero-valued optional timestamps are stored as NULL.
type Run struct {
	ID           string
	SourceKey    string
	PipelineYAML string
	CommitSHA    string
	Ref          string
	Trigger      string
	State        string
	CreatedAt    int64
	StartedAt    int64
	FinishedAt   int64
}

// Job is one node of a run's DAG. SpecJSON is the serialised job definition
// (image, steps, env, ...) as the caller chose to encode it; the store
// treats it as opaque bytes. Attempt increments on every claim so
// runner-side writes can be fenced on (state='running', attempt);
// MaxAttempts bounds retries. FailureKind is one of the Failure* constants
// once the job has failed, "" otherwise. CancelRequestedAt/CancelReason
// are set while a running attempt has a cancel directive waiting for it.
type Job struct {
	ID             string
	RunID          string
	Name           string
	SpecJSON       []byte
	State          string
	Attempt        int
	MaxAttempts    int
	RunnerID       string
	LeaseExpiresAt int64
	FailureKind    string
	ExitCode       *int
	Error          string
	QueuedAt       int64
	StartedAt      int64
	FinishedAt     int64

	CancelRequestedAt int64
	CancelReason      string
}

// JobDep is an edge: JobID cannot start until NeedsJobID succeeds.
type JobDep struct {
	JobID      string
	NeedsJobID string
}

// Runner is a registered agent. Labels drive job placement and are stored
// as JSON in labels_json.
type Runner struct {
	ID           string
	Name         string
	Labels       map[string]string
	Capacity     int
	State        string
	Version      string
	LastSeenAt   int64
	RegisteredAt int64
}

// Event is an append-only audit record for a run (and optionally one of its
// jobs). ID is assigned by the store and is the total order clients page
// through. DetailJSON is an opaque JSON object, "{}" when empty.
type Event struct {
	ID         int64
	RunID      string
	JobID      string
	Type       string
	DetailJSON []byte
	CreatedAt  int64
}
