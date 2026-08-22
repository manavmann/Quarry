// Package version holds the build version stamped into every Quarry binary.
package version

// Version is the build identifier. It defaults to "dev" and is overridden at
// link time by the Makefile:
//
//	-ldflags "-X quarry/internal/version.Version=<git describe>"
var Version = "dev"

// String returns the "<binary> <version>" line each cmd prints.
func String(binary string) string {
	return binary + " " + Version
}
