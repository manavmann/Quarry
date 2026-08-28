package artifact

import "testing"

func TestValidatePath(t *testing.T) {
	ok := []string{"a", "dist/app.bin", "a/b/c.txt", "with space/x", "unicode/ünï.txt", "dot.file", "a..b/c"}
	for _, p := range ok {
		if err := ValidatePath(p); err != nil {
			t.Errorf("ValidatePath(%q) = %v, want nil", p, err)
		}
	}
	bad := []string{
		"", "/abs", "../x", "a/../b", "a/..", "./a", "a/./b", "a//b", "a/", "..",
		"a\\b", "a\x00b", "a\nb", "\x7f",
	}
	for _, p := range bad {
		if err := ValidatePath(p); err == nil {
			t.Errorf("ValidatePath(%q) = nil, want error", p)
		}
	}
}

func TestKeys(t *testing.T) {
	if got, want := SourceKey("r1"), "sources/r1.tar"; got != want {
		t.Errorf("SourceKey = %q, want %q", got, want)
	}
	if got, want := JobKey("r1", "j1", 2, "dist/app.bin"), "runs/r1/jobs/j1/2/dist/app.bin"; got != want {
		t.Errorf("JobKey = %q, want %q", got, want)
	}
	for _, k := range []string{SourceKey("r1"), JobKey("r1", "j1", 1, "a/b")} {
		if err := ValidateKey(k); err != nil {
			t.Errorf("ValidateKey(%q) = %v", k, err)
		}
	}
	if err := ValidateKey("runs/../etc/passwd"); err == nil {
		t.Error("traversal key accepted")
	}
}
