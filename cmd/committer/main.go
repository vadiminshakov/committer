// Command committer runs a key/value cluster on the committer library: a
// coordinator, cohorts that keep the data in Badger, and put/get commands.
//
// Usage:
//
//	# Start a cohort and a coordinator; -cli serves put and get on -nodeaddr.
//	./committer cohort -nodeaddr=localhost:3001 -cli -coordinator=localhost:3000
//	./committer coordinator -nodeaddr=localhost:3000 -cli -cohorts=localhost:3001
//
//	# Write through the coordinator, read from the cohort.
//	./committer put greeting hello
//	./committer get greeting
package main

import (
	"fmt"
	"io"
	"os"
	"strings"
)

const usage = `Usage: committer <command> [flags] [arguments]

Node commands:
  coordinator -nodeaddr localhost:3000 -cohorts localhost:3001 -cli
  cohort -nodeaddr localhost:3001 -coordinator localhost:3000 -cli

CLI commands (flags must precede arguments):
  put    --addr localhost:3000 KEY VALUE    (a coordinator started with -cli)
  get    --addr localhost:3001 KEY          (a cohort started with -cli)

Use 'committer <command> -h' for command options.
The original flag-only node syntax is also supported.
`

func main() {
	if err := execute(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func execute(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(stdout, usage)

		return nil
	}

	switch args[0] {
	case cmdPut, cmdGet:
		return runCLICommand(args[0], args[1:], stdout, stderr)
	case roleCoordinator, roleCohort:
		return runNode(args, stderr)
	}

	// The original flag-only syntax starts a node too.
	if strings.HasPrefix(args[0], "-") {
		return runNode(args, stderr)
	}

	return fmt.Errorf("unknown command %q; run 'committer --help'", args[0])
}
