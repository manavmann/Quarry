package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"quarry/internal/pipeline"
	"quarry/internal/store"
)

// Trigger values recorded on runs.trigger.
const TriggerAPI = "api"

// submitRun turns a validated pipeline into store rows and commits them in
// one transaction: the run (pending), one job per pipeline job, the
// dependency edges, and a run.created event. Root jobs are inserted as
// ready so a runner can claim them immediately; every other job waits as
// pending until the scheduler (C05) advances the DAG.
//
// This is deliberately not an HTTP handler: it is the only piece of
// submission logic in the package and the scheduler may later own it.
func submitRun(ctx context.Context, st *store.Store, p *pipeline.Pipeline, src string) (*store.Run, []store.Job, error) {
	run := &store.Run{
		ID:           newID(8),
		PipelineYAML: src,
		Trigger:      TriggerAPI,
		State:        store.RunPending,
	}
	roots := make(map[string]bool, len(p.Jobs))
	for _, name := range p.Roots() {
		roots[name] = true
	}
	ids := make(map[string]string, len(p.Jobs))
	jobs := make([]store.Job, 0, len(p.Jobs))
	var deps []store.JobDep
	for i := range p.Jobs {
		pj := &p.Jobs[i]
		spec, err := json.Marshal(pj)
		if err != nil {
			return nil, nil, fmt.Errorf("api: encode job %q: %w", pj.Name, err)
		}
		id := newID(8)
		ids[pj.Name] = id
		state := store.JobPending
		if roots[pj.Name] {
			state = store.JobReady
		}
		jobs = append(jobs, store.Job{ID: id, Name: pj.Name, SpecJSON: spec, State: state, MaxAttempts: 1})
	}
	for i := range p.Jobs {
		pj := &p.Jobs[i]
		for _, need := range pj.Needs {
			deps = append(deps, store.JobDep{JobID: ids[pj.Name], NeedsJobID: ids[need]})
		}
	}

	err := st.Tx(ctx, func(tx *sql.Tx) error {
		if err := st.CreateRun(ctx, tx, run, jobs, deps); err != nil {
			return err
		}
		detail, _ := json.Marshal(map[string]any{"jobs": len(jobs), "roots": p.Roots()})
		return st.AppendEvent(ctx, tx, &store.Event{RunID: run.ID, Type: "run.created", DetailJSON: detail})
	})
	if err != nil {
		return nil, nil, err
	}
	return run, jobs, nil
}
