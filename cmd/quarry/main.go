// Command quarry is the Quarry CLI: submit, watch and inspect runs against
// a control plane. See internal/cli for the commands and configuration.
package main

import (
	"os"

	"quarry/internal/cli"
)

func main() {
	os.Exit(cli.Main())
}
