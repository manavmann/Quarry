package pipeline

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

const diamond = `
name: diamond
jobs:
  - name: build
    image: golang:1.27
    steps: ["go build ./..."]
  - name: lint
    image: golang:1.27
    steps: ["make lint"]
    needs: [build]
  - name: test
    image: golang:1.27
    steps: ["go test ./..."]
    needs: [build]
  - name: release
    image: alpine
    steps: ["./release.sh"]
    needs: [lint, test]
`

func mustParse(t *testing.T, src string) *Pipeline {
	t.Helper()
	p, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return p
}

func TestParseValid(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want func(t *testing.T, p *Pipeline)
	}{
		{
			name: "single job with all fields",
			src: `
name: full
jobs:
  - name: build
    image: golang:1.27
    steps:
      - go build ./...
      - go vet ./...
    env: {CGO_ENABLED: "0"}
    labels: {os: linux}
    artifacts: ["bin/**"]
    timeout: 5m
    resources: {cpu: 2, memory: 1Gi}
`,
			want: func(t *testing.T, p *Pipeline) {
				j := p.Job("build")
				if j == nil {
					t.Fatal("job build missing")
				}
				if j.Image != "golang:1.27" || len(j.Steps) != 2 {
					t.Errorf("image/steps = %q/%v", j.Image, j.Steps)
				}
				if j.Env["CGO_ENABLED"] != "0" || j.Labels["os"] != "linux" {
					t.Errorf("env/labels = %v/%v", j.Env, j.Labels)
				}
				if !reflect.DeepEqual(j.Artifacts, []string{"bin/**"}) {
					t.Errorf("artifacts = %v", j.Artifacts)
				}
				if j.Timeout != 5*time.Minute {
					t.Errorf("timeout = %v, want 5m", j.Timeout)
				}
				if j.Resources.CPU != 2 || j.Resources.Memory != "1Gi" {
					t.Errorf("resources = %+v", j.Resources)
				}
			},
		},
		{
			name: "timeout defaults to 30m",
			src: `
jobs:
  - name: a
    image: alpine
    steps: ["true"]
`,
			want: func(t *testing.T, p *Pipeline) {
				if got := p.Job("a").Timeout; got != DefaultTimeout {
					t.Errorf("timeout = %v, want %v", got, DefaultTimeout)
				}
			},
		},
		{
			name: "diamond",
			src:  diamond,
			want: func(t *testing.T, p *Pipeline) {
				if len(p.Jobs) != 4 || p.Name != "diamond" {
					t.Errorf("got %d jobs, name %q", len(p.Jobs), p.Name)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.want(t, mustParse(t, tt.src))
		})
	}
}

func TestParseInvalid(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []string // substrings that must appear in the error
	}{
		{
			name: "empty document",
			src:  "",
			want: []string{"document is empty"},
		},
		{
			name: "no jobs",
			src:  "name: x\n",
			want: []string{"pipeline has no jobs"},
		},
		{
			name: "unknown field",
			src: `
jobs:
  - name: a
    image: alpine
    steps: ["true"]
    bogus: 1
`,
			want: []string{"bogus"},
		},
		{
			name: "missing name",
			src: `
jobs:
  - image: alpine
    steps: ["true"]
`,
			want: []string{"job #1: name is required"},
		},
		{
			name: "duplicate name",
			src: `
jobs:
  - name: a
    image: alpine
    steps: ["true"]
  - name: a
    image: alpine
    steps: ["true"]
`,
			want: []string{`job "a": duplicate job name`},
		},
		{
			name: "missing image",
			src: `
jobs:
  - name: a
    steps: ["true"]
`,
			want: []string{`job "a": image is required`},
		},
		{
			name: "no steps",
			src: `
jobs:
  - name: a
    image: alpine
`,
			want: []string{`job "a": at least one step is required`},
		},
		{
			name: "blank step",
			src: `
jobs:
  - name: a
    image: alpine
    steps: ["true", "  "]
`,
			want: []string{`job "a": step #2 is empty`},
		},
		{
			name: "unknown need",
			src: `
jobs:
  - name: a
    image: alpine
    steps: ["true"]
    needs: [nope]
`,
			want: []string{`job "a": needs unknown job "nope"`},
		},
		{
			name: "self need",
			src: `
jobs:
  - name: a
    image: alpine
    steps: ["true"]
    needs: [a]
`,
			want: []string{`job "a": needs itself`},
		},
		{
			name: "duplicate need",
			src: `
jobs:
  - name: a
    image: alpine
    steps: ["true"]
  - name: b
    image: alpine
    steps: ["true"]
    needs: [a, a]
`,
			want: []string{`job "b": needs "a" more than once`},
		},
		{
			name: "negative timeout",
			src: `
jobs:
  - name: a
    image: alpine
    steps: ["true"]
    timeout: -1s
`,
			want: []string{`job "a": timeout must not be negative`},
		},
		{
			name: "multiple problems reported together",
			src: `
jobs:
  - name: a
    steps: ["true"]
  - name: b
    image: alpine
`,
			want: []string{`job "a": image is required`, `job "b": at least one step is required`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Parse([]byte(tt.src))
			if err == nil {
				t.Fatalf("Parse succeeded, want error; got %+v", p)
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err.Error(), w)
				}
			}
		})
	}
}

func TestCycleDetection(t *testing.T) {
	tests := []struct {
		name  string
		src   string
		stuck []string // every job named in the cycle error
	}{
		{
			name: "two-node cycle",
			src: `
jobs:
  - name: a
    image: alpine
    steps: ["true"]
    needs: [b]
  - name: b
    image: alpine
    steps: ["true"]
    needs: [a]
`,
			stuck: []string{"a", "b"},
		},
		{
			name: "three-node cycle behind a root",
			src: `
jobs:
  - name: root
    image: alpine
    steps: ["true"]
  - name: a
    image: alpine
    steps: ["true"]
    needs: [root, c]
  - name: b
    image: alpine
    steps: ["true"]
    needs: [a]
  - name: c
    image: alpine
    steps: ["true"]
    needs: [b]
`,
			stuck: []string{"a", "b", "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.src))
			var verr *ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("err = %v, want *ValidationError", err)
			}
			msg := err.Error()
			if !strings.Contains(msg, "dependency cycle") {
				t.Fatalf("error %q does not mention a cycle", msg)
			}
			for _, name := range tt.stuck {
				if !strings.Contains(msg, name) {
					t.Errorf("error %q does not name job %q", msg, name)
				}
			}
			if strings.Contains(msg, "root") {
				t.Errorf("error %q blames a job outside the cycle", msg)
			}
		})
	}
}

func TestDiamondOrdering(t *testing.T) {
	p := mustParse(t, diamond)

	order := p.TopoOrder()
	if want := []string{"build", "lint", "test", "release"}; !reflect.DeepEqual(order, want) {
		t.Errorf("TopoOrder = %v, want %v", order, want)
	}
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	for _, j := range p.Jobs {
		for _, need := range j.Needs {
			if pos[need] >= pos[j.Name] {
				t.Errorf("%s scheduled before its need %s", j.Name, need)
			}
		}
	}

	if got := p.Roots(); !reflect.DeepEqual(got, []string{"build"}) {
		t.Errorf("Roots = %v, want [build]", got)
	}
	cases := map[string][]string{
		"build":   {"lint", "test"},
		"lint":    {"release"},
		"test":    {"release"},
		"release": nil,
		"nope":    nil,
	}
	for name, want := range cases {
		if got := p.Dependents(name); !reflect.DeepEqual(got, want) {
			t.Errorf("Dependents(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestTopoOrderKeepsDeclarationOrderForIndependentJobs(t *testing.T) {
	p := mustParse(t, `
jobs:
  - name: z
    image: alpine
    steps: ["true"]
  - name: y
    image: alpine
    steps: ["true"]
  - name: x
    image: alpine
    steps: ["true"]
    needs: [z]
`)
	if got, want := p.TopoOrder(), []string{"z", "y", "x"}; !reflect.DeepEqual(got, want) {
		t.Errorf("TopoOrder = %v, want %v", got, want)
	}
	if got, want := p.Roots(), []string{"z", "y"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Roots = %v, want %v", got, want)
	}
}
