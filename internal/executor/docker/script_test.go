package docker

import (
	"strings"
	"testing"

	"quarry/internal/executor"
	"quarry/internal/pipeline"
)

func TestRunScriptEchoesEachStepVerbatim(t *testing.T) {
	got := string(runScript([]string{"go build ./...", `echo "it's" 'q'`}))
	want := "#!/bin/sh\nset -e\n" +
		"echo '+ go build ./...'\ngo build ./...\n" +
		`echo '+ echo "it'\''s" '\''q'\'''` + "\n" + `echo "it's" 'q'` + "\n"
	if got != want {
		t.Fatalf("script:\n%s\nwant:\n%s", got, want)
	}
}

func TestEnvPlatformVariablesWin(t *testing.T) {
	s := executor.JobSpec{JobID: "j", RunID: "r", Attempt: 2, Job: pipeline.Job{Env: map[string]string{"CI": "no", "FOO": "bar"}}}
	got := strings.Join(env(s), " ")
	for _, w := range []string{"FOO=bar", "CI=true", "QUARRY_JOB=j", "QUARRY_RUN=r", "QUARRY_ATTEMPT=2"} {
		if !strings.Contains(got, w) {
			t.Errorf("env %q lacks %q", got, w)
		}
	}
	if strings.Contains(got, "CI=no") {
		t.Errorf("job env overrode CI: %q", got)
	}
}

func TestParseMemory(t *testing.T) {
	for in, want := range map[string]int64{"512m": 512 << 20, "1Gi": 1 << 30, "256MiB": 256 << 20, "4096": 4096, "8k": 8 << 10} {
		if got, err := parseMemory(in); err != nil || got != want {
			t.Errorf("parseMemory(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "abc", "-1m", "0", "1.5g"} {
		if _, err := parseMemory(in); err == nil {
			t.Errorf("parseMemory(%q) accepted", in)
		}
	}
}

func TestHostConfigGrantsNothing(t *testing.T) {
	hc, err := hostConfig("vol", pipeline.Resources{CPU: 1.5, Memory: "1g"})
	if err != nil {
		t.Fatal(err)
	}
	if hc.Privileged || len(hc.Binds) != 0 || len(hc.CapAdd) != 0 {
		t.Fatalf("host config grants privileges: %+v", hc)
	}
	if hc.Resources.Memory != 1<<30 || hc.Resources.NanoCPUs != 1_500_000_000 {
		t.Fatalf("limits = %+v", hc.Resources)
	}
	if len(hc.Mounts) != 1 || hc.Mounts[0].Source != "vol" || hc.Mounts[0].Target != "/workspace" {
		t.Fatalf("mounts = %+v", hc.Mounts)
	}
}
