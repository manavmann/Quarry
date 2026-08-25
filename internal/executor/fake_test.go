package executor

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"quarry/internal/pipeline"
)

func spec(name string) JobSpec {
	return JobSpec{JobID: "j-" + name, RunID: "r", Attempt: 1, Job: pipeline.Job{Name: name}}
}

func TestFakeDefaultsToSuccess(t *testing.T) {
	f := NewFake()
	var buf bytes.Buffer
	res, err := f.Run(context.Background(), spec("build"), &buf)
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("got %+v, %v; want exit 0", res, err)
	}
	if got := f.Executions(); len(got) != 1 || got[0].Spec.Job.Name != "build" {
		t.Fatalf("executions = %+v", got)
	}
}

func TestFakeScriptedOutcomes(t *testing.T) {
	f := NewFake()
	f.Script("fail", Outcome{ExitCode: 2, LogBytes: 100})
	f.Script("infra", Outcome{Err: ErrInfra})

	var buf bytes.Buffer
	res, err := f.Run(context.Background(), spec("fail"), &buf)
	if err != nil || res.ExitCode != 2 {
		t.Fatalf("fail: got %+v, %v", res, err)
	}
	if buf.Len() != 100 {
		t.Fatalf("log bytes = %d, want 100", buf.Len())
	}
	if _, err := f.Run(context.Background(), spec("infra"), &buf); !errors.Is(err, ErrInfra) {
		t.Fatalf("infra: err = %v", err)
	}
}

func TestFakeHangHonoursContextAndRelease(t *testing.T) {
	f := NewFake()
	f.Script("hang", Outcome{Hang: true})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := f.Run(ctx, spec("hang"), &bytes.Buffer{}); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	go func() { _, err := f.Run(context.Background(), spec("hang"), &bytes.Buffer{}); done <- err }()
	f.Release("hang")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("err = %v, want nil after Release", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Release")
	}
}
