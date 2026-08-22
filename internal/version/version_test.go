package version

import "testing"

func TestDefaultVersionIsDev(t *testing.T) {
	if Version != "dev" {
		t.Fatalf("Version = %q, want %q", Version, "dev")
	}
}

func TestStringPrefixesBinaryName(t *testing.T) {
	for _, bin := range []string{"server", "runner", "quarry"} {
		got := String(bin)
		want := bin + " " + Version
		if got != want {
			t.Errorf("String(%q) = %q, want %q", bin, got, want)
		}
	}
}
