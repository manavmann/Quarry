package docker

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/volume"
)

// Reap removes every container and volume labelled with this runner's
// name: the remains of attempts a previous process of the same runner
// did not get to clean up (SIGKILL, host reboot). Containers go first,
// forced, then volumes, so a volume is never still mounted when its
// removal is tried. Each item removed is logged with its job, run and
// attempt. A failed list is returned; a failed remove is logged and the
// rest continues. With KeepFailed set nothing is reaped, since that knob
// promises failed attempts stay in place for inspection.
//
// Only this runner's label is matched: several runners share one daemon
// in the compose cluster and must not touch each other's attempts.
func (e *Executor) Reap(ctx context.Context, logger *log.Logger) error {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	if e.cfg.KeepFailed {
		logger.Printf("reap: keep_failed set, leaving containers and volumes of runner %s", e.cfg.RunnerName)
		return nil
	}
	f := filters.NewArgs(filters.Arg("label", LabelRunner+"="+e.cfg.RunnerName))

	cs, err := e.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return fmt.Errorf("docker: reap: list containers: %w", err)
	}
	for _, c := range cs {
		if rerr := e.cli.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); rerr != nil {
			logger.Printf("reap: remove container %s: %v", describe(c.Labels), rerr)
			continue
		}
		logger.Printf("reap: removed orphan container %s (state %s)", describe(c.Labels), c.State)
	}

	vs, err := e.cli.VolumeList(ctx, volume.ListOptions{Filters: f})
	if err != nil {
		return fmt.Errorf("docker: reap: list volumes: %w", err)
	}
	for _, v := range vs.Volumes {
		if rerr := e.cli.VolumeRemove(ctx, v.Name, true); rerr != nil {
			logger.Printf("reap: remove volume %s: %v", describe(v.Labels), rerr)
			continue
		}
		logger.Printf("reap: removed orphan volume %s", describe(v.Labels))
	}
	logger.Printf("reap: runner %s: %d containers, %d volumes found", e.cfg.RunnerName, len(cs), len(vs.Volumes))
	return nil
}

// describe renders the attempt identity from the labels Run stamps.
func describe(labels map[string]string) string {
	return fmt.Sprintf("quarry-%s-%s (run %s)", labels[LabelJob], labels[LabelAttempt], labels[LabelRun])
}
