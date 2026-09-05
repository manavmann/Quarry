// Command bench drives the four Quarry benchmarks of docs/benchmarks.md
// against a running control plane over its HTTP API, and for the
// scheduling-overhead one starts a local server and fake runners itself.
// The shell scripts next to it are the entry points; this file only
// knows how to submit, wait, and compute. Standard library only.
//
//	go run ./scripts/bench sched      -agents N   (local binaries, fake executor)
//	go run ./scripts/bench throughput            (compose cluster)
//	go run ./scripts/bench logs                  (compose cluster)
//	go run ./scripts/bench recovery  -kill CMD   (compose cluster)
//
// Every mode prints one "rep N: ..." line per repetition and a final
// "median: ..." line. Timestamps come from the server (Unix ms) except in
// recovery, where kill and replacement are both observed from this
// process so the two clocks cancel out.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: bench sched|throughput|logs|recovery [flags]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	server := fs.String("server", envOr("QUARRY_SERVER", "http://127.0.0.1:8080"), "control plane URL")
	token := fs.String("token", envOr("QUARRY_TOKEN", "dev-token"), "bearer token")
	reps := fs.Int("reps", 3, "repetitions")
	var err error
	switch os.Args[1] {
	case "sched":
		agents := fs.Int("agents", 1, "fake runners to start")
		jobs := fs.Int("jobs", 100, "independent jobs per submission")
		poll := fs.Duration("poll", time.Second, "runner poll interval (QUARRY_POLL_INTERVAL)")
		bin := fs.String("bin", "bin", "directory holding the server and runner binaries")
		fs.Parse(os.Args[2:])
		err = sched(*bin, *agents, *jobs, *reps, *poll)
	case "throughput":
		jobs := fs.Int("jobs", 30, "trivial jobs per submission")
		image := fs.String("image", "alpine:3.20", "job image (pre-pull it)")
		fs.Parse(os.Args[2:])
		err = throughput(&client{*server, *token}, *jobs, *reps, *image)
	case "logs":
		size := fs.Int64("bytes", 50<<20, "bytes of output the job emits")
		image := fs.String("image", "alpine:3.20", "job image (pre-pull it)")
		fs.Parse(os.Args[2:])
		err = logIngest(&client{*server, *token}, *size, *reps, *image)
	case "recovery":
		kill := fs.String("kill", "", "command that kills the runner; {runner} is replaced by its name")
		restore := fs.String("restore", "", "command that brings the runner back; {runner} as above")
		ttl := fs.Duration("ttl", 30*time.Second, "the server's lease TTL, for the grace wait and the report")
		image := fs.String("image", "alpine:3.20", "job image (pre-pull it)")
		fs.Parse(os.Args[2:])
		if *kill == "" {
			err = errors.New("-kill is required")
		} else {
			err = recovery(&client{*server, *token}, *kill, *restore, *ttl, *reps, *image)
		}
	default:
		err = fmt.Errorf("unknown mode %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ---- API client (own wire types, like the CLI) --------------------------

type client struct{ url, token string }

type run struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	CreatedAt  int64  `json:"created_at"`
	FinishedAt int64  `json:"finished_at"`
}

type job struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	State      string `json:"state"`
	Attempt    int    `json:"attempt"`
	RunnerID   string `json:"runner_id"`
	QueuedAt   int64  `json:"queued_at"`
	StartedAt  int64  `json:"started_at"`
	FinishedAt int64  `json:"finished_at"`
}

type runDetail struct {
	Run  run   `json:"run"`
	Jobs []job `json:"jobs"`
}

type runnerRow struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	State      string `json:"state"`
	LastSeenAt int64  `json:"last_seen_at"`
}

func (c *client) do(method, path string, body []byte, out any) error {
	req, err := http.NewRequest(method, c.url+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func (c *client) submit(yaml string) (*runDetail, error) {
	var d runDetail
	return &d, c.do(http.MethodPost, "/api/runs", []byte(yaml), &d)
}

func (c *client) getRun(id string) (*runDetail, error) {
	var d runDetail
	return &d, c.do(http.MethodGet, "/api/runs/"+id, nil, &d)
}

func (c *client) cancel(id string) error {
	return c.do(http.MethodPost, "/api/runs/"+id+"/cancel", nil, nil)
}

func (c *client) runners() ([]runnerRow, error) {
	var reply struct {
		Runners []runnerRow `json:"runners"`
	}
	return reply.Runners, c.do(http.MethodGet, "/api/runners", nil, &reply)
}

// logStats walks a job's log through the cursor API and returns the
// number of chunks and bytes stored.
func (c *client) logStats(jobID string) (chunks int, size int64, err error) {
	var after int64
	for {
		var l struct {
			Chunks []struct {
				Seq  int64  `json:"seq"`
				Data []byte `json:"data"`
			} `json:"chunks"`
			Next int64 `json:"next"`
		}
		if err := c.do(http.MethodGet, "/api/jobs/"+jobID+"/logs?after="+strconv.FormatInt(after, 10), nil, &l); err != nil {
			return 0, 0, err
		}
		if len(l.Chunks) == 0 {
			return chunks, size, nil
		}
		for _, ch := range l.Chunks {
			chunks++
			size += int64(len(ch.Data))
		}
		after = l.Next
	}
}

// waitRun polls until the run is terminal or the deadline passes.
func (c *client) waitRun(id string, timeout time.Duration) (*runDetail, error) {
	deadline := time.Now().Add(timeout)
	for {
		d, err := c.getRun(id)
		if err != nil {
			return nil, err
		}
		switch d.Run.State {
		case "succeeded", "failed", "cancelled":
			return d, nil
		}
		if time.Now().After(deadline) {
			return d, fmt.Errorf("run %s still %s after %s", id, d.Run.State, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitOnline polls until n runners are online.
func (c *client) waitOnline(n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		rs, err := c.runners()
		if err == nil {
			online := 0
			for _, r := range rs {
				if r.State == "online" {
					online++
				}
			}
			if online >= n {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d runners not online after %s (%v)", n, timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ---- pipelines --------------------------------------------------------

// fanOut is n independent jobs running the given steps.
func fanOut(name, image string, n int, steps string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "name: %s\njobs:\n", name)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  - name: j%03d\n    image: %s\n    steps: [%s]\n", i, image, steps)
	}
	return b.String()
}

// ---- statistics -------------------------------------------------------

func percentile(vs []float64, p float64) float64 {
	if len(vs) == 0 {
		return 0
	}
	s := append([]float64(nil), vs...)
	sort.Float64s(s)
	i := int(float64(len(s))*p+0.999999) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(s) {
		i = len(s) - 1
	}
	return s[i]
}

func median(vs []float64) float64 { return percentile(vs, 0.5) }

// ---- 1. scheduling overhead ------------------------------------------

// sched starts a server and n fake runners from bin, submits reps
// fan-outs of jobs independent jobs and reports queued→running latency
// (started_at − queued_at, both server-stamped) p50/p95 per rep and the
// claim rate. With the fake executor a job takes no time, so the number
// is the scheduler + one HTTP round trip + the runner's next claim.
func sched(bin string, n, jobs, reps int, poll time.Duration) error {
	exe := ""
	if runtime.GOOS == "windows" {
		exe = ".exe"
	}
	dir, err := os.MkdirTemp("", "quarry-bench-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	logs, err := os.Create(filepath.Join(dir, "processes.log"))
	if err != nil {
		return err
	}
	defer logs.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const addr = "127.0.0.1:18080"
	c := &client{url: "http://" + addr, token: "bench"}
	start := func(name string, env ...string) (*exec.Cmd, error) {
		cmd := exec.CommandContext(ctx, filepath.Join(bin, name+exe))
		cmd.Env = append(os.Environ(), env...)
		cmd.Stdout, cmd.Stderr = logs, logs
		return cmd, cmd.Start()
	}
	var procs []*exec.Cmd
	defer func() {
		cancel()
		for _, p := range procs {
			_ = p.Wait()
		}
	}()
	srv, err := start("server",
		"QUARRY_LISTEN="+addr, "QUARRY_DB="+filepath.Join(dir, "quarry.db"),
		"QUARRY_API_TOKEN=bench", "QUARRY_ARTIFACT_DIR="+filepath.Join(dir, "artifacts"))
	if err != nil {
		return fmt.Errorf("start server: %w", err)
	}
	procs = append(procs, srv)
	if err := waitHealthy(c.url, 10*time.Second); err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		r, err := start("runner",
			"QUARRY_SERVER="+c.url, "QUARRY_API_TOKEN=bench", "QUARRY_RUNNER_NAME=bench-"+strconv.Itoa(i),
			"QUARRY_EXECUTOR=fake", "QUARRY_CAPACITY=2", "QUARRY_POLL_INTERVAL="+poll.String())
		if err != nil {
			return fmt.Errorf("start runner %d: %w", i, err)
		}
		procs = append(procs, r)
	}
	if err := c.waitOnline(n, 10*time.Second); err != nil {
		return err
	}
	fmt.Printf("sched: %d fake runners (capacity 2, poll %s), %d jobs per submission, %d reps\n", n, poll, jobs, reps)

	var p50s, p95s, pickups, rates []float64
	for rep := 1; rep <= reps; rep++ {
		d, err := c.submit(fanOut("sched", "alpine", jobs, `"true"`))
		if err != nil {
			return err
		}
		done, err := c.waitRun(d.Run.ID, 2*time.Minute)
		if err != nil {
			return err
		}
		if done.Run.State != "succeeded" {
			return fmt.Errorf("rep %d: run %s", rep, done.Run.State)
		}
		// queued→running includes the idle runners' wait for their next
		// poll before the first claim (pickup); the claim rate is taken
		// between the first and last start, where every claim follows
		// the previous one at once.
		var lat []float64
		queued, first, last := int64(1<<62), int64(1<<62), int64(0)
		for _, j := range done.Jobs {
			lat = append(lat, float64(j.StartedAt-j.QueuedAt))
			queued = min(queued, j.QueuedAt)
			first = min(first, j.StartedAt)
			last = max(last, j.StartedAt)
		}
		p50, p95 := percentile(lat, 0.5), percentile(lat, 0.95)
		pickup := first - queued
		rate := float64(jobs-1) / (float64(last-first) / 1000)
		p50s, p95s, pickups, rates = append(p50s, p50), append(p95s, p95), append(pickups, float64(pickup)), append(rates, rate)
		fmt.Printf("rep %d: queued→running p50 %.0f ms  p95 %.0f ms  (pickup %d ms, then %d claims in %d ms = %.0f claims/s)\n",
			rep, p50, p95, pickup, jobs-1, last-first, rate)
	}
	fmt.Printf("median: agents=%d p50 %.0f ms  p95 %.0f ms  pickup %.0f ms  %.0f claims/s\n",
		n, median(p50s), median(p95s), median(pickups), median(rates))
	return nil
}

func waitHealthy(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		resp, err := http.Get(url + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("server not healthy after %s: %v", timeout, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ---- 2. throughput ------------------------------------------------------

// throughput submits reps fan-outs of jobs trivial container jobs and
// reports jobs per minute from the first queued_at to the last
// finished_at of each run. On the compose cluster this is the cost of
// starting and stopping a container per job, not of scheduling.
func throughput(c *client, jobs, reps int, image string) error {
	fmt.Printf("throughput: %d jobs of `true` on %s per submission, %d reps\n", jobs, image, reps)
	var jpm []float64
	for rep := 1; rep <= reps; rep++ {
		d, err := c.submit(fanOut("throughput", image, jobs, `"true"`))
		if err != nil {
			return err
		}
		done, err := c.waitRun(d.Run.ID, 10*time.Minute)
		if err != nil {
			return err
		}
		if done.Run.State != "succeeded" {
			return fmt.Errorf("rep %d: run %s", rep, done.Run.State)
		}
		first, last := int64(1<<62), int64(0)
		var durs []float64
		for _, j := range done.Jobs {
			first = min(first, j.QueuedAt)
			last = max(last, j.FinishedAt)
			durs = append(durs, float64(j.FinishedAt-j.StartedAt))
		}
		v := float64(jobs) * 60000 / float64(last-first)
		jpm = append(jpm, v)
		fmt.Printf("rep %d: %d jobs in %.1f s = %.1f jobs/min (per-job running p50 %.0f ms)\n",
			rep, jobs, float64(last-first)/1000, v, median(durs))
	}
	fmt.Printf("median: %.1f jobs/min\n", median(jpm))
	return nil
}

// ---- 3. log ingestion ----------------------------------------------------

// logIngest runs one job that writes size bytes of 100-byte lines and
// reports MB/s and chunks/s over the attempt's running time. The server
// must accept that many bytes per attempt (QUARRY_LOG_CAP).
func logIngest(c *client, size int64, reps int, image string) error {
	line := strings.Repeat("0123456789", 10)
	steps := fmt.Sprintf(`"yes %s | head -c %d"`, line, size)
	fmt.Printf("logs: one job emitting %d bytes on %s, %d reps\n", size, image, reps)
	var mbps, cps []float64
	for rep := 1; rep <= reps; rep++ {
		d, err := c.submit(fanOut("logs", image, 1, steps))
		if err != nil {
			return err
		}
		done, err := c.waitRun(d.Run.ID, 10*time.Minute)
		if err != nil {
			return err
		}
		if done.Run.State != "succeeded" {
			return fmt.Errorf("rep %d: run %s", rep, done.Run.State)
		}
		j := done.Jobs[0]
		chunks, stored, err := c.logStats(j.ID)
		if err != nil {
			return err
		}
		if stored < size {
			return fmt.Errorf("rep %d: %d bytes stored of %d emitted — raise QUARRY_LOG_CAP", rep, stored, size)
		}
		secs := float64(j.FinishedAt-j.StartedAt) / 1000
		mb, ch := float64(stored)/1e6/secs, float64(chunks)/secs
		mbps, cps = append(mbps, mb), append(cps, ch)
		fmt.Printf("rep %d: %d bytes in %d chunks over %.2f s = %.1f MB/s, %.0f chunks/s\n", rep, stored, chunks, secs, mb, ch)
	}
	fmt.Printf("median: %.1f MB/s  %.0f chunks/s\n", median(mbps), median(cps))
	return nil
}

// ---- 4. runner-loss recovery ---------------------------------------------

// recovery submits a long job, kills the runner that claimed it and
// measures the time until attempt 2 is observed running. Both instants
// are taken by this process, so the number includes the kill command,
// lease expiry (TTL), the monitor tick and the replacement's poll. The
// run is cancelled afterwards and restore, if given, brings the runner
// back before the next rep. It first waits one TTL so a server that was
// just (re)started is past its lease-expiry grace.
func recovery(c *client, kill, restore string, ttl time.Duration, reps int, image string) error {
	fmt.Printf("recovery: lease TTL %s, %d reps; waiting %s for the server's expiry grace\n", ttl, reps, ttl)
	time.Sleep(ttl)
	var secs []float64
	for rep := 1; rep <= reps; rep++ {
		d, err := c.submit(fanOut("recovery", image, 1, `"sleep 600"`))
		if err != nil {
			return err
		}
		jobID := d.Jobs[0].ID
		var runnerID string
		if err := pollUntil(5*time.Minute, func() (bool, error) {
			d, err := c.getRun(d.Run.ID)
			if err != nil {
				return false, err
			}
			runnerID = d.Jobs[0].RunnerID
			return d.Jobs[0].State == "running" && d.Jobs[0].Attempt == 1, nil
		}); err != nil {
			return fmt.Errorf("rep %d: attempt 1: %w", rep, err)
		}
		rs, err := c.runners()
		if err != nil {
			return err
		}
		var name string
		for _, r := range rs {
			if r.ID == runnerID {
				name = r.Name
			}
		}
		if name == "" {
			return fmt.Errorf("rep %d: runner %s not listed", rep, runnerID)
		}
		t0 := time.Now()
		if err := shell(strings.ReplaceAll(kill, "{runner}", name)); err != nil {
			return fmt.Errorf("rep %d: kill: %w", rep, err)
		}
		var replacement string
		if err := pollUntil(ttl+2*time.Minute, func() (bool, error) {
			d, err := c.getRun(d.Run.ID)
			if err != nil {
				return false, err
			}
			replacement = d.Jobs[0].RunnerID
			return d.Jobs[0].State == "running" && d.Jobs[0].Attempt == 2, nil
		}); err != nil {
			return fmt.Errorf("rep %d: attempt 2: %w", rep, err)
		}
		s := time.Since(t0).Seconds()
		secs = append(secs, s)
		fmt.Printf("rep %d: killed %s, attempt 2 running on %s after %.1f s (job %s)\n", rep, name, replacement, s, jobID)
		if err := c.cancel(d.Run.ID); err != nil {
			return err
		}
		if _, err := c.waitRun(d.Run.ID, time.Minute); err != nil {
			return err
		}
		if restore != "" {
			if err := shell(strings.ReplaceAll(restore, "{runner}", name)); err != nil {
				return fmt.Errorf("rep %d: restore: %w", rep, err)
			}
			if err := c.waitOnline(len(rs), 2*time.Minute); err != nil {
				return err
			}
		}
	}
	fmt.Printf("median: ttl=%s kill→replacement %.1f s\n", ttl, median(secs))
	return nil
}

func pollUntil(timeout time.Duration, cond func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// shell runs one command line through sh so the scripts can pass
// `docker compose ... {runner}` as a single flag.
func shell(line string) error {
	cmd := exec.Command("sh", "-c", line)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	return cmd.Run()
}
