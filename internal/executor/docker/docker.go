// Package docker is the real Executor: one container per job attempt on
// a Docker daemon the runner shares (Docker-out-of-Docker), a named
// volume per attempt mounted at /workspace, the job's source injected by
// tar copy, and a generated /quarry/run.sh that echoes each step before
// running it. Job containers get memory/cpu limits from the spec and
// nothing else: no socket, no privileges.
//
// Goroutines and their owners: Run owns one goroutine that demuxes the
// attach stream into the log writer; it is joined before Run returns.
package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"

	"quarry/internal/executor"
	"quarry/internal/pipeline"
)

// Labels put on every container and volume so a runner can find (and,
// from C16, reap) what it created. Values are the runner name and the
// attempt identity.
const (
	LabelRunner  = "quarry.runner"
	LabelJob     = "quarry.job"
	LabelRun     = "quarry.run"
	LabelAttempt = "quarry.attempt"
)

const (
	workspace  = "/workspace"
	scriptPath = "quarry/run.sh" // relative to /, where it is copied
	// cleanupTimeout bounds each remove call after the job is over; the
	// job's own context may already be cancelled by then.
	cleanupTimeout = 30 * time.Second
	// killGrace is how long Run waits for the daemon to report the
	// container gone after ContainerKill before giving up on the wait.
	killGrace = 10 * time.Second
)

// SourceFunc returns the run's source bundle as a tar stream to be
// unpacked into /workspace. A (nil, nil) return means the run has no
// bundle: the workspace starts empty.
type SourceFunc func(ctx context.Context, runID string) (io.ReadCloser, error)

// Config is what a Docker executor needs beyond the daemon.
type Config struct {
	// RunnerName labels everything created so it can be reaped.
	RunnerName string
	// Source fetches a run's bundle; nil means every workspace is empty.
	Source SourceFunc
	// KeepFailed leaves the container and volume of a failed attempt in
	// place for inspection (QUARRY_KEEP_FAILED=1).
	KeepFailed bool
}

// Executor runs jobs on a Docker daemon. It is safe for concurrent use.
type Executor struct {
	cfg Config
	cli *client.Client
}

// New connects to the daemon named by the DOCKER_* environment (or the
// platform default socket) and negotiates the API version.
func New(cfg Config) (*Executor, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker: %w", err)
	}
	if cfg.RunnerName == "" {
		cfg.RunnerName = "unnamed"
	}
	return &Executor{cfg: cfg, cli: cli}, nil
}

// Close releases the daemon connection.
func (e *Executor) Close() error { return e.cli.Close() }

// Run implements executor.Executor. Every daemon call up to start uses
// ctx; cleanup uses a detached context so a cancelled job is still torn
// down. Container and volume are removed in defers on every path, the
// container first.
func (e *Executor) Run(ctx context.Context, spec executor.JobSpec, logs io.Writer) (res executor.Result, err error) {
	name := fmt.Sprintf("quarry-%s-%d", spec.JobID, spec.Attempt)
	labels := map[string]string{
		LabelRunner: e.cfg.RunnerName, LabelJob: spec.JobID, LabelRun: spec.RunID,
		LabelAttempt: strconv.Itoa(spec.Attempt),
	}
	hostCfg, err := hostConfig(name, spec.Job.Resources)
	if err != nil {
		return res, orCtx(ctx, err)
	}

	if err := e.ensureImage(ctx, spec.Job.Image, logs); err != nil {
		return res, orCtx(ctx, err)
	}

	// keep reports, at defer time, whether the failed attempt's remains
	// should be left for debugging.
	keep := func() bool { return e.cfg.KeepFailed && (err != nil || res.ExitCode != 0) }

	if _, err = e.cli.VolumeCreate(ctx, volume.CreateOptions{Name: name, Labels: labels}); err != nil {
		return res, orCtx(ctx, fmt.Errorf("docker: create volume: %w", err))
	}
	defer func() {
		if keep() {
			return
		}
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		if rerr := e.cli.VolumeRemove(cctx, name, true); rerr != nil {
			fmt.Fprintf(logs, "[quarry] remove volume %s: %v\n", name, rerr)
		}
	}()

	created, err := e.cli.ContainerCreate(ctx,
		&container.Config{
			Image:      spec.Job.Image,
			Cmd:        []string{"/bin/sh", "/" + scriptPath},
			WorkingDir: workspace,
			Env:        env(spec),
			Labels:     labels,
			Tty:        false, // stdcopy demuxes only a non-tty stream
		}, hostCfg, nil, nil, name)
	if err != nil {
		return res, orCtx(ctx, fmt.Errorf("docker: create container: %w", err))
	}
	id := created.ID
	defer func() {
		if keep() {
			return
		}
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		if rerr := e.cli.ContainerRemove(cctx, id, container.RemoveOptions{Force: true}); rerr != nil {
			fmt.Fprintf(logs, "[quarry] remove container %s: %v\n", name, rerr)
		}
	}()

	if err = e.injectSource(ctx, id, spec.RunID); err != nil {
		return res, orCtx(ctx, err)
	}
	if err = e.cli.CopyToContainer(ctx, id, "/", tarOf(scriptPath, 0o755, runScript(spec.Job.Steps)), container.CopyToContainerOptions{}); err != nil {
		return res, orCtx(ctx, fmt.Errorf("docker: copy run.sh: %w", err))
	}

	// Attach before start or the first lines of output are lost.
	att, err := e.cli.ContainerAttach(ctx, id, container.AttachOptions{Stream: true, Stdout: true, Stderr: true})
	if err != nil {
		return res, orCtx(ctx, fmt.Errorf("docker: attach: %w", err))
	}
	demuxDone := make(chan struct{})
	go func() {
		defer close(demuxDone)
		_, _ = stdcopy.StdCopy(logs, logs, att.Reader)
	}()
	defer func() { att.Close(); <-demuxDone }()

	// Registered before start so no exit can be missed; NextExit is the
	// condition that does not fire at once on a created container. The
	// wait outlives ctx: on cancel we kill explicitly and still want
	// the daemon's exit report.
	wctx, wcancel := context.WithCancel(context.WithoutCancel(ctx))
	defer wcancel()
	waitCh, errCh := e.cli.ContainerWait(wctx, id, container.WaitConditionNextExit)

	start := time.Now()
	if err = e.cli.ContainerStart(ctx, id, container.StartOptions{}); err != nil {
		return res, orCtx(ctx, fmt.Errorf("docker: start: %w", err))
	}

	var w container.WaitResponse
	select {
	case w = <-waitCh:
	case werr := <-errCh:
		return res, fmt.Errorf("docker: wait: %w", werr)
	case <-ctx.Done():
		kctx, kcancel := context.WithTimeout(context.WithoutCancel(ctx), killGrace)
		defer kcancel()
		_ = e.cli.ContainerKill(kctx, id, "KILL") // already-exited is fine
		select {
		case <-waitCh:
		case <-errCh:
		case <-kctx.Done():
		}
		return res, ctx.Err()
	}
	res.Duration = time.Since(start)
	res.ExitCode = int(w.StatusCode)
	if w.Error != nil {
		return res, fmt.Errorf("docker: wait: %s", w.Error.Message)
	}
	if info, ierr := e.cli.ContainerInspect(wctx, id); ierr == nil && info.State != nil {
		res.OOMKilled = info.State.OOMKilled
	}
	// Artifacts come out of the exited container before the deferred
	// removal; a collection failure is an infra error (and keeps the
	// container when KeepFailed).
	if res.ExitCode == 0 && spec.ArtifactDir != "" && len(spec.Job.Artifacts) > 0 {
		if err = e.collectArtifacts(ctx, id, spec, logs); err != nil {
			return res, orCtx(ctx, err)
		}
	}
	return res, nil
}

// orCtx keeps the Executor contract: once ctx is done, Run returns
// ctx.Err() whatever the daemon call said.
func orCtx(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	return err
}

// ensureImage pulls ref when the daemon does not have it, draining the
// pull stream completely and writing only milestone lines (no progress
// bars) to logs. A stream error is a pull failure.
func (e *Executor) ensureImage(ctx context.Context, ref string, logs io.Writer) error {
	if _, err := e.cli.ImageInspect(ctx, ref); err == nil {
		return nil
	}
	fmt.Fprintf(logs, "[quarry] pulling image %s\n", ref)
	rc, err := e.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("docker: pull %s: %w", ref, err)
	}
	defer rc.Close()
	dec := json.NewDecoder(rc)
	for {
		var msg struct {
			Status   string `json:"status"`
			ID       string `json:"id"`
			Progress string `json:"progress"`
			Error    string `json:"error"`
		}
		if err := dec.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("docker: pull %s: %w", ref, err)
		}
		if msg.Error != "" {
			return fmt.Errorf("docker: pull %s: %s", ref, msg.Error)
		}
		if msg.Progress != "" || msg.Status == "" {
			continue
		}
		if msg.ID != "" {
			fmt.Fprintf(logs, "[quarry] %s: %s\n", msg.ID, msg.Status)
		} else {
			fmt.Fprintf(logs, "[quarry] %s\n", msg.Status)
		}
	}
}

// injectSource unpacks the run's bundle into /workspace of the created
// (not yet started) container. No bundle means an empty workspace.
func (e *Executor) injectSource(ctx context.Context, id, runID string) error {
	if e.cfg.Source == nil {
		return nil
	}
	src, err := e.cfg.Source(ctx, runID)
	if err != nil {
		return fmt.Errorf("docker: fetch source: %w", err)
	}
	if src == nil {
		return nil
	}
	defer src.Close()
	if err := e.cli.CopyToContainer(ctx, id, workspace, src, container.CopyToContainerOptions{}); err != nil {
		return fmt.Errorf("docker: copy source: %w", err)
	}
	return nil
}

// hostConfig is the sandbox: the attempt's volume at /workspace and the
// spec's limits. Nothing here grants the socket or privileges.
func hostConfig(vol string, r pipeline.Resources) (*container.HostConfig, error) {
	hc := &container.HostConfig{
		Mounts: []mount.Mount{{Type: mount.TypeVolume, Source: vol, Target: workspace}},
	}
	if r.Memory != "" {
		b, err := parseMemory(r.Memory)
		if err != nil {
			return nil, err
		}
		hc.Resources.Memory = b
	}
	if r.CPU > 0 {
		hc.Resources.NanoCPUs = int64(r.CPU * 1e9)
	}
	return hc, nil
}

// platformEnv are the variables every job gets; they win over job env.
var platformEnv = []string{"CI", "QUARRY_JOB", "QUARRY_RUN", "QUARRY_ATTEMPT"}

// env is the job's env plus the platform variables.
func env(spec executor.JobSpec) []string {
	sys := map[string]string{
		"CI":             "true",
		"QUARRY_JOB":     spec.JobID,
		"QUARRY_RUN":     spec.RunID,
		"QUARRY_ATTEMPT": strconv.Itoa(spec.Attempt),
	}
	out := make([]string, 0, len(spec.Job.Env)+len(sys))
	for k, v := range spec.Job.Env {
		if _, reserved := sys[k]; !reserved {
			out = append(out, k+"="+v)
		}
	}
	for _, k := range platformEnv {
		out = append(out, k+"="+sys[k])
	}
	return out
}

// runScript renders the steps as a sh script: set -e, each step echoed
// with a "+ " prefix then run verbatim. Steps are never interpolated
// into a shell string, so quoting inside them cannot break the script.
func runScript(steps []string) []byte {
	var b strings.Builder
	b.WriteString("#!/bin/sh\nset -e\n")
	for _, s := range steps {
		b.WriteString("echo '+ " + strings.ReplaceAll(s, "'", `'\''`) + "'\n")
		b.WriteString(s + "\n")
	}
	return []byte(b.String())
}

// tarOf is a one-file tar stream.
func tarOf(name string, mode int64, body []byte) io.Reader {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	_ = tw.WriteHeader(&tar.Header{Name: name, Mode: mode, Size: int64(len(body)), ModTime: time.Now()})
	_, _ = tw.Write(body)
	_ = tw.Close()
	return &buf
}

// parseMemory reads a byte count with an optional k/m/g suffix and
// optional trailing "i"/"b" ("512m", "1Gi", "256MiB"): Docker's own
// --memory units, all binary multiples.
func parseMemory(s string) (int64, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	t = strings.TrimSuffix(t, "b")
	t = strings.TrimSuffix(t, "i")
	mult := int64(1)
	if n := len(t); n > 0 {
		switch t[n-1] {
		case 'k':
			mult, t = 1<<10, t[:n-1]
		case 'm':
			mult, t = 1<<20, t[:n-1]
		case 'g':
			mult, t = 1<<30, t[:n-1]
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("docker: resources.memory %q is not a size like 512m or 1Gi", s)
	}
	return n * mult, nil
}
