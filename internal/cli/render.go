package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// dagOrder returns jobs in dependency order: every job after all of its
// needs, ties broken by the server's order (declaration order). It is the
// same Kahn walk pipeline.TopoOrder does, on the job specs the API
// returns. Jobs whose needs never resolve (impossible for a run the
// server accepted) are appended at the end so nothing is hidden.
func dagOrder(jobs []Job) []Job {
	type spec struct {
		Needs []string `json:"needs"`
	}
	index := make(map[string]int, len(jobs))
	needs := make([][]string, len(jobs))
	for i := range jobs {
		index[jobs[i].Name] = i
		var s spec
		_ = json.Unmarshal(jobs[i].Spec, &s)
		needs[i] = s.Needs
	}
	indeg := make([]int, len(jobs))
	dependents := make([][]int, len(jobs))
	for i, ns := range needs {
		for _, n := range ns {
			if j, ok := index[n]; ok {
				indeg[i]++
				dependents[j] = append(dependents[j], i)
			}
		}
	}
	var ready []int
	for i := range jobs {
		if indeg[i] == 0 {
			ready = append(ready, i)
		}
	}
	done := make([]bool, len(jobs))
	out := make([]Job, 0, len(jobs))
	for len(ready) > 0 {
		sort.Ints(ready)
		i := ready[0]
		ready = ready[1:]
		done[i] = true
		out = append(out, jobs[i])
		for _, d := range dependents[i] {
			indeg[d]--
			if indeg[d] == 0 {
				ready = append(ready, d)
			}
		}
	}
	for i := range jobs {
		if !done[i] {
			out = append(out, jobs[i])
		}
	}
	return out
}

// renderJobs writes the watch/status table: one row per job in DAG order
// with its state, attempt, runner and duration. now (Unix ms) is the
// clock for still-running jobs so the output is reproducible.
func renderJobs(w io.Writer, jobs []Job, now int64) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB\tSTATE\tATTEMPT\tRUNNER\tDURATION")
	for _, j := range dagOrder(jobs) {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", j.Name, jobState(j), attempt(j), dash(j.RunnerID), duration(j.StartedAt, j.FinishedAt, now))
	}
	tw.Flush()
}

// jobState is the state plus the failure reason when there is one, e.g.
// "failed (exit 2)" or "failed (timeout)".
func jobState(j Job) string {
	if j.State != "failed" {
		return j.State
	}
	switch {
	case j.ExitCode != nil:
		return fmt.Sprintf("failed (exit %d)", *j.ExitCode)
	case j.FailureKind != "":
		return "failed (" + j.FailureKind + ")"
	}
	return j.State
}

func attempt(j Job) string {
	if j.Attempt == 0 {
		return "-"
	}
	if j.MaxAttempts > 1 {
		return fmt.Sprintf("%d/%d", j.Attempt, j.MaxAttempts)
	}
	return fmt.Sprint(j.Attempt)
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// duration renders finished-started, or now-started while running, or "-".
func duration(started, finished, now int64) string {
	if started == 0 {
		return "-"
	}
	end := finished
	if end == 0 {
		end = now
	}
	d := time.Duration(end-started) * time.Millisecond
	if d < 0 {
		d = 0
	}
	if d < time.Minute {
		return d.Round(100 * time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}

func renderRuns(w io.Writer, runs []Run, now int64) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "RUN\tSTATE\tTRIGGER\tCREATED\tDURATION")
	for _, r := range runs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", r.ID, r.State, r.Trigger, stamp(r.CreatedAt), duration(r.StartedAt, r.FinishedAt, now))
	}
	tw.Flush()
}

func renderRunners(w io.Writer, runners []Runner, now int64) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "RUNNER\tID\tSTATE\tCAPACITY\tLABELS\tLAST SEEN\tVERSION")
	for _, r := range runners {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n", r.Name, r.ID, r.State, r.Capacity, labels(r.Labels), ago(r.LastSeenAt, now), dash(r.Version))
	}
	tw.Flush()
}

func renderEvents(w io.Writer, events []Event) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, e := range events {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", stamp(e.CreatedAt), e.Type, dash(e.JobID), compactJSON(e.Detail))
	}
	tw.Flush()
}

func labels(m map[string]string) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ",")
}

func ago(at, now int64) string {
	if at == 0 {
		return "-"
	}
	return duration(at, now, now) + " ago"
}

// stamp renders a Unix-ms timestamp in UTC, second precision.
func stamp(ms int64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05Z")
}

func compactJSON(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" || s == "{}" {
		return ""
	}
	return s
}
