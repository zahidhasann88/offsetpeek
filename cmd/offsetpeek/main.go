// Command offsetpeek previews a Kafka consumer-group offset reset without
// changing anything.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/example/offsetpeek/internal/cmd"
)

// version is set at release time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	root := cmd.RootCmd()
	root.Version = version
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		var exit *cmd.ExitError
		if errors.As(err, &exit) {
			os.Exit(exit.Code)
		}
		os.Exit(1)
	}
}
