// Command committer runs a key/value cluster on the committer library: a
// coordinator, cohorts that keep the data in Badger, and put/get commands.
//
// Usage:
//
//	# Start a cohort and a coordinator; -cli lets them serve put and get.
//	./committer cohort -addr=localhost:3001 -coordinator=localhost:3000 -cli
//	./committer coordinator -addr=localhost:3000 -cohorts=localhost:3001 -cli
//
//	# Write through the coordinator, read from the cohort.
//	./committer put greeting hello
//	./committer get greeting
package main

import (
	"fmt"
	"io"
	"os"
)

const usage = `Usage: committer <command> [flags] [arguments]

Node commands:
  coordinator -addr localhost:3000 -cohorts localhost:3001 -cli
  cohort -addr localhost:3001 -coordinator localhost:3000 -cli

CLI commands, for nodes started with -cli (flags must precede arguments):
  put    --addr localhost:3000 KEY VALUE    (a coordinator's -addr)
  get    --addr localhost:3001 KEY          (a cohort's -addr)

The CLI connects to the node's -addr port + 1000.

Use 'committer <command> -h' for command options.
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
		return runNode(args[0], args[1:], stderr)
	}

	return fmt.Errorf("unknown command %q; run 'committer --help'", args[0])
}
