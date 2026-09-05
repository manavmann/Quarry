// Command record runs a command with stdout and stderr merged and writes
// an asciicast v2 file of everything it printed, each line stamped with
// the time it arrived. It is the recorder behind scripts/record-demo.sh:
// asciinema's agg turns the cast into docs/demo.gif. Standard library only.
//
//	go run ./scripts/record -o docs/demo.cast -cols 100 -rows 30 -from 'Runners registered' -- sh scripts/demo.sh kill-runner
//
// The command runs on a pipe, not a terminal, so tools print their
// non-interactive output (no progress bars). The first event is the
// command itself behind a prompt, as if typed; with -from, output before
// the first line matching the pattern is left out and the clock starts
// there, so a slow setup does not pad the recording. The exit code is the
// command's.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

func main() {
	out := flag.String("o", "demo.cast", "asciicast file to write")
	cols := flag.Int("cols", 100, "terminal width recorded in the header")
	rows := flag.Int("rows", 30, "terminal height recorded in the header")
	prompt := flag.String("prompt", "$ ", "prompt shown before the command")
	from := flag.String("from", "", "drop output before the first line matching this regexp")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: record [-o file] [-cols N] [-rows N] [-from regexp] -- command [args...]")
		os.Exit(2)
	}
	code, err := run(*out, *cols, *rows, *prompt, *from, flag.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, "record:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(out string, cols, rows int, prompt, from string, args []string) (int, error) {
	var skip *regexp.Regexp
	if from != "" {
		re, err := regexp.Compile(from)
		if err != nil {
			return 0, err
		}
		skip = re
	}
	f, err := os.Create(out)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	start := time.Now()
	header := map[string]any{
		"version":   2,
		"width":     cols,
		"height":    rows,
		"timestamp": start.Unix(),
		"env":       map[string]string{"TERM": "xterm-256color", "SHELL": "/bin/sh"},
	}
	if err := json.NewEncoder(w).Encode(header); err != nil {
		return 0, err
	}
	// Timestamps never go backwards, even when -from resets the clock.
	var last float64
	event := func(text string) error {
		data, err := json.Marshal(strings.ReplaceAll(text, "\n", "\r\n"))
		if err != nil {
			return err
		}
		t := time.Since(start).Seconds()
		if t < last {
			t = last
		}
		last = t
		_, err = fmt.Fprintf(w, "[%.3f, \"o\", %s]\n", t, data)
		return err
	}
	if err := event(prompt + strings.Join(args, " ") + "\n"); err != nil {
		return 0, err
	}

	pr, pw := io.Pipe()
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout, cmd.Stderr = pw, pw
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	waited := make(chan error, 1)
	go func() {
		waited <- cmd.Wait()
		pw.Close()
	}()
	r := bufio.NewReader(pr)
	for {
		line, err := r.ReadString('\n')
		if skip != nil && line != "" {
			if !skip.MatchString(line) {
				line = ""
			} else {
				skip, start = nil, time.Now()
			}
		}
		if line != "" {
			if err := event(line); err != nil {
				return 0, err
			}
		}
		if err != nil {
			break
		}
	}
	err = <-waited
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode(), nil
	}
	return 0, err
}
