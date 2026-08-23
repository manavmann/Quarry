// Package pipeline parses and validates .quarry.yml files and exposes the
// resulting job DAG. It performs no I/O: callers hand it bytes.
package pipeline

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultTimeout applies to any job whose spec omits `timeout`.
const DefaultTimeout = 30 * time.Minute

// Pipeline is a validated .quarry.yml: a set of jobs forming a DAG.
type Pipeline struct {
	Name string `yaml:"name"`
	Jobs []Job  `yaml:"jobs"`

	// byName indexes Jobs; deps holds the reverse edges (job -> jobs that
	// need it). Both are built once by Parse.
	byName map[string]*Job
	deps   map[string][]string
}

// Job is one unit of work executed in a single container.
type Job struct {
	Name      string            `yaml:"name"`
	Image     string            `yaml:"image"`
	Steps     []string          `yaml:"steps"`
	Needs     []string          `yaml:"needs"`
	Env       map[string]string `yaml:"env"`
	Labels    map[string]string `yaml:"labels"`
	Artifacts []string          `yaml:"artifacts"`
	Timeout   time.Duration     `yaml:"timeout"`
	Resources Resources         `yaml:"resources"`
}

// Resources are the scheduling hints a job may declare.
type Resources struct {
	CPU    float64 `yaml:"cpu"`
	Memory string  `yaml:"memory"`
}

// ValidationError collects every problem found in a spec so the user sees
// them all at once. Each message names the offending job.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid pipeline:\n  " + strings.Join(e.Problems, "\n  ")
}

// Parse decodes a .quarry.yml document, applies defaults and validates it.
// The returned *Pipeline is safe to query only when err is nil.
func Parse(src []byte) (*Pipeline, error) {
	var p Pipeline
	dec := yaml.NewDecoder(strings.NewReader(string(src)))
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, &ValidationError{Problems: []string{"document is empty"}}
		}
		return nil, fmt.Errorf("parse pipeline: %w", err)
	}
	if err := p.finalize(); err != nil {
		return nil, err
	}
	return &p, nil
}

// finalize applies defaults, validates and builds the indexes.
func (p *Pipeline) finalize() error {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	if len(p.Jobs) == 0 {
		add("pipeline has no jobs")
	}

	p.byName = make(map[string]*Job, len(p.Jobs))
	for i := range p.Jobs {
		j := &p.Jobs[i]
		if j.Name == "" {
			add("job #%d: name is required", i+1)
			continue
		}
		if _, dup := p.byName[j.Name]; dup {
			add("job %q: duplicate job name", j.Name)
			continue
		}
		p.byName[j.Name] = j
		if j.Image == "" {
			add("job %q: image is required", j.Name)
		}
		if len(j.Steps) == 0 {
			add("job %q: at least one step is required", j.Name)
		}
		for k, s := range j.Steps {
			if strings.TrimSpace(s) == "" {
				add("job %q: step #%d is empty", j.Name, k+1)
			}
		}
		if j.Timeout < 0 {
			add("job %q: timeout must not be negative", j.Name)
		} else if j.Timeout == 0 {
			j.Timeout = DefaultTimeout
		}
		if j.Resources.CPU < 0 {
			add("job %q: resources.cpu must not be negative", j.Name)
		}
	}

	// Edges are only checked once names are settled so "unknown" is accurate.
	p.deps = make(map[string][]string, len(p.Jobs))
	for i := range p.Jobs {
		j := &p.Jobs[i]
		if p.byName[j.Name] != j {
			continue // unnamed or duplicate; already reported
		}
		seen := make(map[string]bool, len(j.Needs))
		for _, need := range j.Needs {
			switch {
			case need == j.Name:
				add("job %q: needs itself", j.Name)
			case seen[need]:
				add("job %q: needs %q more than once", j.Name, need)
			case p.byName[need] == nil:
				add("job %q: needs unknown job %q", j.Name, need)
			default:
				p.deps[need] = append(p.deps[need], j.Name)
			}
			seen[need] = true
		}
	}

	if len(problems) == 0 {
		if _, err := p.topo(); err != nil {
			problems = append(problems, err.Error())
		}
	}
	if len(problems) > 0 {
		return &ValidationError{Problems: problems}
	}
	return nil
}

// topo runs Kahn's algorithm. Ties are broken by declaration order so the
// result is deterministic. On a cycle it returns the names of every job
// caught in one.
func (p *Pipeline) topo() ([]string, error) {
	indeg := make(map[string]int, len(p.Jobs))
	for _, j := range p.Jobs {
		indeg[j.Name] = len(j.Needs)
	}
	// Declaration index gives the deterministic tie-break.
	order := make(map[string]int, len(p.Jobs))
	for i, j := range p.Jobs {
		order[j.Name] = i
	}

	var ready []string
	for _, j := range p.Jobs {
		if indeg[j.Name] == 0 {
			ready = append(ready, j.Name)
		}
	}
	out := make([]string, 0, len(p.Jobs))
	for len(ready) > 0 {
		sort.Slice(ready, func(a, b int) bool { return order[ready[a]] < order[ready[b]] })
		n := ready[0]
		ready = ready[1:]
		out = append(out, n)
		for _, d := range p.deps[n] {
			indeg[d]--
			if indeg[d] == 0 {
				ready = append(ready, d)
			}
		}
	}
	if len(out) != len(p.Jobs) {
		var stuck []string
		for _, j := range p.Jobs {
			if indeg[j.Name] > 0 {
				stuck = append(stuck, j.Name)
			}
		}
		return nil, fmt.Errorf("dependency cycle among jobs: %s", strings.Join(stuck, ", "))
	}
	return out, nil
}

// Job returns the job with the given name, or nil.
func (p *Pipeline) Job(name string) *Job {
	return p.byName[name]
}

// TopoOrder returns job names such that every job appears after all of its
// needs. Independent jobs keep their declaration order.
func (p *Pipeline) TopoOrder() []string {
	out, err := p.topo()
	if err != nil {
		// Parse rejects cyclic pipelines, so this cannot happen for a
		// Pipeline obtained from Parse.
		panic("pipeline: TopoOrder on unvalidated pipeline: " + err.Error())
	}
	return out
}

// Roots returns the names of jobs with no needs, in declaration order.
func (p *Pipeline) Roots() []string {
	var roots []string
	for _, j := range p.Jobs {
		if len(j.Needs) == 0 {
			roots = append(roots, j.Name)
		}
	}
	return roots
}

// Dependents returns the names of jobs that list name in their needs, in
// declaration order. Unknown names yield nil.
func (p *Pipeline) Dependents(name string) []string {
	if len(p.deps[name]) == 0 {
		return nil
	}
	out := make([]string, len(p.deps[name]))
	copy(out, p.deps[name])
	return out
}
