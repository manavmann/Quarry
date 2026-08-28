// Package cli implements the quarry command: a thin client of the control
// plane's HTTP API. It bundles a workspace, submits it, and renders what
// the server reports; every decision about state lives on the server.
//
// Configuration: --server / QUARRY_SERVER (default http://127.0.0.1:8080)
// and --token / QUARRY_TOKEN (required).
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"quarry/internal/version"
)

// DefaultServer is used when neither --server nor QUARRY_SERVER is set.
const DefaultServer = "http://127.0.0.1:8080"

// PipelineFile is the pipeline document read from the workspace root.
const PipelineFile = ".quarry.yml"

// ExitError carries a process exit code out of a command: 1 for a failed
// or cancelled run, 130 on interrupt.
type ExitError struct{ Code int }

func (e *ExitError) Error() string { return fmt.Sprintf("exit %d", e.Code) }

// App holds the state the subcommands share.
type App struct {
	out, err io.Writer
	server   string
	token    string
	interval time.Duration
	client   *Client
	// now is the clock rendering uses for durations; tests pin it.
	now func() int64
}

// New builds the root command writing to out and err.
func New(out, err io.Writer) *cobra.Command {
	a := &App{out: out, err: err, now: func() int64 { return time.Now().UnixMilli() }}
	root := &cobra.Command{
		Use:           "quarry",
		Short:         "Quarry CI/CD client",
		Version:       version.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Name() == "help" || cmd.Name() == "completion" {
				return nil
			}
			if a.token == "" {
				return errors.New("no API token: set QUARRY_TOKEN or --token")
			}
			a.server = strings.TrimRight(a.server, "/")
			a.client = NewClient(a.server, a.token)
			return nil
		},
	}
	root.SetOut(out)
	root.SetErr(err)
	pf := root.PersistentFlags()
	pf.StringVar(&a.server, "server", envOr("QUARRY_SERVER", DefaultServer), "control plane URL (QUARRY_SERVER)")
	pf.StringVar(&a.token, "token", os.Getenv("QUARRY_TOKEN"), "API bearer token (QUARRY_TOKEN)")
	pf.DurationVar(&a.interval, "interval", time.Second, "poll interval for watch, logs -f and --wait")

	root.AddCommand(a.runCmd(), a.runsCmd(), a.statusCmd(), a.watchCmd(), a.logsCmd(),
		a.artifactsCmd(), a.cancelCmd(), a.runnersCmd(), a.eventsCmd())
	return root
}

// Main runs the CLI with os args and returns the process exit code.
func Main() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	root := New(os.Stdout, os.Stderr)
	err := root.ExecuteContext(ctx)
	var exit *ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &exit):
		return exit.Code
	case errors.Is(err, context.Canceled):
		fmt.Fprintln(os.Stderr, "interrupted")
		return 130
	default:
		fmt.Fprintln(os.Stderr, "quarry:", err)
		return 1
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ---- run -------------------------------------------------------------

func (a *App) runCmd() *cobra.Command {
	var dir, file string
	var wait bool
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Bundle the workspace and submit its .quarry.yml as a run",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if file == "" {
				file = filepath.Join(dir, PipelineFile)
			}
			pipeline, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			// The bundle is written once to a temp file so a retried submit
			// can stream an identical body.
			bundle, err := os.CreateTemp("", "quarry-bundle-*.tar")
			if err != nil {
				return err
			}
			defer os.Remove(bundle.Name())
			err = Bundle(bundle, dir)
			if cerr := bundle.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return fmt.Errorf("bundle %s: %w", dir, err)
			}
			d, err := a.client.Submit(ctx, multipartBody(pipeline, bundle.Name()))
			if err != nil {
				return err
			}
			fmt.Fprintln(a.out, d.Run.ID)
			if !wait {
				return nil
			}
			return a.watch(ctx, d.Run.ID)
		},
	}
	cmd.Flags().StringVarP(&dir, "dir", "C", ".", "workspace directory to bundle")
	cmd.Flags().StringVarP(&file, "file", "f", "", "pipeline file (default <dir>/.quarry.yml)")
	cmd.Flags().BoolVar(&wait, "wait", false, "watch the run until it finishes; exit 1 unless it succeeds")
	return cmd
}

// multipartBody returns a per-attempt body factory: a multipart stream
// with the pipeline as "pipeline" and the bundle file as "source",
// produced through a pipe so the bundle is never held in memory.
func multipartBody(pipeline []byte, bundlePath string) func() (io.ReadCloser, string, error) {
	return func() (io.ReadCloser, string, error) {
		f, err := os.Open(bundlePath)
		if err != nil {
			return nil, "", err
		}
		pr, pw := io.Pipe()
		mw := multipart.NewWriter(pw)
		go func() {
			defer f.Close()
			err := func() error {
				p, err := mw.CreateFormFile("pipeline", PipelineFile)
				if err != nil {
					return err
				}
				if _, err := p.Write(pipeline); err != nil {
					return err
				}
				s, err := mw.CreateFormFile("source", "source.tar")
				if err != nil {
					return err
				}
				if _, err := io.Copy(s, f); err != nil {
					return err
				}
				return mw.Close()
			}()
			pw.CloseWithError(err)
		}()
		return pr, mw.FormDataContentType(), nil
	}
}

// ---- runs / status / watch ---------------------------------------------

func (a *App) runsCmd() *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:   "runs",
		Short: "List recent runs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			runs, err := a.client.ListRuns(cmd.Context(), limit)
			if err != nil {
				return err
			}
			renderRuns(a.out, runs, a.now())
			return nil
		},
	}
	cmd.Flags().IntVarP(&limit, "limit", "n", 20, "number of runs to show")
	return cmd
}

func (a *App) statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <run>",
		Short: "Show a run and its jobs once",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := a.client.GetRun(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			a.printRun(d)
			return nil
		},
	}
}

func (a *App) watchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "watch <run>",
		Short: "Poll a run and redraw its job table until it finishes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.watch(cmd.Context(), args[0])
		},
	}
}

// watch prints the job table whenever it changes and returns once the run
// is terminal: nil for succeeded, ExitError{1} otherwise.
func (a *App) watch(ctx context.Context, runID string) error {
	var last string
	for {
		d, err := a.client.GetRun(ctx, runID)
		if err != nil {
			return err
		}
		var b strings.Builder
		fmt.Fprintf(&b, "run %s: %s\n", d.Run.ID, d.Run.State)
		renderJobs(&b, d.Jobs, a.now())
		if cur := b.String(); cur != last {
			if last != "" {
				fmt.Fprintln(a.out)
			}
			fmt.Fprint(a.out, cur)
			last = cur
		}
		if runTerminal(d.Run.State) {
			if d.Run.State != "succeeded" {
				return &ExitError{Code: 1}
			}
			return nil
		}
		if err := sleep(ctx, a.interval); err != nil {
			return err
		}
	}
}

func (a *App) printRun(d *RunDetail) {
	fmt.Fprintf(a.out, "run %s: %s\n", d.Run.ID, d.Run.State)
	renderJobs(a.out, d.Jobs, a.now())
}

// ---- logs ------------------------------------------------------------

func (a *App) logsCmd() *cobra.Command {
	var follow bool
	var attempt int
	cmd := &cobra.Command{
		Use:   "logs <job>",
		Short: "Print a job's log; -f follows until the job is finished and drained",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			var after int64
			for {
				// Read the job state before the chunks: a terminal state
				// observed first guarantees the following read sees every
				// chunk the runner flushed before completing.
				var terminal bool
				if follow {
					j, err := a.client.GetJob(ctx, args[0])
					if err != nil {
						return err
					}
					terminal = jobTerminal(j.State)
				}
				l, err := a.client.GetLogs(ctx, args[0], attempt, after)
				if err != nil {
					return err
				}
				for _, c := range l.Chunks {
					if _, err := a.out.Write(c.Data); err != nil {
						return err
					}
				}
				after = l.Next
				if !follow || (terminal && len(l.Chunks) == 0) {
					return nil
				}
				if err := sleep(ctx, a.interval); err != nil {
					return err
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "keep reading until the job is terminal and drained")
	cmd.Flags().IntVar(&attempt, "attempt", 0, "attempt to read (default: the job's current attempt)")
	return cmd
}

// ---- cancel / runners / events -----------------------------------------

func (a *App) cancelCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <run>",
		Short: "Cancel a run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := a.client.Cancel(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Fprintf(a.out, "run %s: cancel requested\n", args[0])
			return nil
		},
	}
}

func (a *App) runnersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "runners",
		Short: "List registered runners",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			rs, err := a.client.ListRunners(cmd.Context())
			if err != nil {
				return err
			}
			renderRunners(a.out, rs, a.now())
			return nil
		},
	}
}

func (a *App) eventsCmd() *cobra.Command {
	var follow bool
	cmd := &cobra.Command{
		Use:   "events <run>",
		Short: "Print a run's events; -f keeps printing until the run finishes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			var after int64
			for {
				var terminal bool
				if follow {
					d, err := a.client.GetRun(ctx, args[0])
					if err != nil {
						return err
					}
					terminal = runTerminal(d.Run.State)
				}
				evs, err := a.client.ListEvents(ctx, args[0], after)
				if err != nil {
					return err
				}
				renderEvents(a.out, evs)
				if len(evs) > 0 {
					after = evs[len(evs)-1].ID
				}
				if !follow || (terminal && len(evs) == 0) {
					return nil
				}
				if err := sleep(ctx, a.interval); err != nil {
					return err
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "keep reading until the run is terminal and drained")
	return cmd
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// ---- artifacts ---------------------------------------------------------

func (a *App) artifactsCmd() *cobra.Command {
	var download string
	var attempt int
	cmd := &cobra.Command{
		Use:   "artifacts <job>",
		Short: "List a job's artifacts; --download writes them under a directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			arts, err := a.client.ListArtifacts(ctx, args[0], attempt)
			if err != nil {
				return err
			}
			if download == "" {
				renderArtifacts(a.out, arts)
				return nil
			}
			for _, art := range arts {
				if err := a.downloadArtifact(ctx, args[0], attempt, art, download); err != nil {
					return err
				}
				fmt.Fprintf(a.out, "%s\t%d\n", filepath.Join(download, filepath.FromSlash(art.Path)), art.SizeBytes)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&download, "download", "", "directory to write the artifacts into (created if needed)")
	cmd.Flags().IntVar(&attempt, "attempt", 0, "attempt to read (default: the job's current attempt)")
	return cmd
}

// downloadArtifact streams one artifact to <dir>/<path>, refusing a path
// that would leave dir and rejecting a body whose sha256 differs from the
// listing. The file is written to a temp name and renamed into place.
func (a *App) downloadArtifact(ctx context.Context, jobID string, attempt int, art Artifact, dir string) error {
	if !safeRelPath(art.Path) {
		return fmt.Errorf("artifact %q: refusing unsafe path", art.Path)
	}
	dst := filepath.Join(dir, filepath.FromSlash(art.Path))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".quarry-dl-*")
	if err != nil {
		return err
	}
	sum, err := a.client.DownloadArtifact(ctx, jobID, attempt, art.Path, tmp)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil && sum != art.SHA256 {
		err = fmt.Errorf("artifact %s: sha256 %s, want %s", art.Path, sum, art.SHA256)
	}
	if err == nil {
		err = os.Rename(tmp.Name(), dst)
	}
	if err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return nil
}

// safeRelPath reports whether p is a slash-relative path with no empty,
// "." or ".." segments, so joining it under a directory stays inside it.
func safeRelPath(p string) bool {
	if p == "" || strings.HasPrefix(p, "/") || strings.ContainsRune(p, '\\') || strings.ContainsRune(p, 0) {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}
