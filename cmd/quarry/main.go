// Command quarry is a Quarry binary stub; it prints its version and exits.
package main

import (
	"fmt"

	"quarry/internal/version"
)

func main() {
	fmt.Println(version.String("quarry"))
}
