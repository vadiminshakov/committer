package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/vadiminshakov/committer/v2/cmd/committer/internal/cliapi"
	"github.com/vadiminshakov/committer/v2/core/coordinator"
	"github.com/vadiminshakov/committer/v2/core/dto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// CLI subcommands.
	cmdPut = "put"
	cmdGet = "get"
)

// Positional argument counts for CLI subcommands.
const (
	putArgCount = 2
	getArgCount = 1
)

// defaultCLITimeout bounds every CLI request unless -timeout overrides it.
const defaultCLITimeout = 5 * time.Second

// defaultClientAddrs match the README example: put goes to the coordinator,
// get to the cohort.
var defaultClientAddrs = map[string]string{cmdPut: "localhost:4000", cmdGet: "localhost:4001"}

func runCLICommand(command string, args []string, stdout, stderr io.Writer) error {
	flagset := flag.NewFlagSet("committer "+command, flag.ContinueOnError)
	flagset.SetOutput(stderr)
	addr := flagset.String("addr", defaultClientAddrs[command],
		"-clientaddr of the target node: a coordinator for put, a cohort for get")
	timeout := flagset.Duration("timeout", defaultCLITimeout, "request deadline, e.g. 5s or 500ms")

	flagset.Usage = func() {
		suffix := map[string]string{cmdPut: "KEY VALUE", cmdGet: "KEY"}[command]
		fmt.Fprintf(stderr, "Usage: committer %s [flags] %s\n", command, suffix)
		flagset.PrintDefaults()
	}
	if err := flagset.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}

		return fmt.Errorf("parse flags: %w", err)
	}

	expected := map[string]int{cmdPut: putArgCount, cmdGet: getArgCount}[command]
	if flagset.NArg() != expected {
		flagset.Usage()

		return fmt.Errorf("%s requires %d arguments; place flags before arguments", command, expected)
	}

	if err := dto.Addr(*addr).Validate(); err != nil {
		return fmt.Errorf("invalid -addr flag: %w", err)
	}

	if *timeout <= 0 {
		return errors.New("-timeout must be positive")
	}

	if flagset.Arg(0) == "" {
		return errors.New("key must not be empty")
	}

	cli, err := cliapi.Dial(*addr)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", *addr, err)
	}

	defer func() {
		_ = cli.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := invokeCLIOperation(cli, command, flagset.Args(), stdout, ctx); err != nil {
		return fmt.Errorf("%s at %s failed: %s%s",
			command, *addr, cliErrorMessage(err), hintForCLIError(command, err))
	}

	return nil
}

// invokeCLIOperation executes one CLI subcommand and prints its result.
// CLI API errors already carry operation context, so they pass through unwrapped.
//
//nolint:wrapcheck
func invokeCLIOperation(
	cli *cliapi.Client,
	command string,
	positional []string,
	stdout io.Writer,
	ctx context.Context,
) error {
	switch command {
	case cmdPut:
		height, err := cli.Commit(ctx, positional[0], []byte(positional[1]))
		if err != nil {
			return err
		}

		fmt.Fprintf(stdout, "Committed transaction %d\n", height)
	case cmdGet:
		value, err := cli.Get(ctx, positional[0])
		if err != nil {
			return err
		}

		fmt.Fprintln(stdout, string(value))
	default:
		return fmt.Errorf("unknown CLI command %q", command)
	}

	return nil
}

// hintForCLIError suggests likely causes for common CLI failures.
func hintForCLIError(command string, err error) string {
	if errors.Is(err, coordinator.ErrAborted) {
		return "; the transaction was aborted and can be retried"
	}

	hint := ""
	code := status.Code(err)

	switch code {
	case codes.Unavailable:
		hint = "; check that the node is running with -clientaddr and --addr matches it"
	case codes.Unimplemented:
		hint = "; --addr must be the node's -clientaddr, not its -nodeaddr"
	case codes.DeadlineExceeded:
		hint = "; check node connectivity or increase --timeout"
	case codes.FailedPrecondition:
		if command == cmdPut {
			hint = "; check the coordinator address and its participants"
		}
	default:
		// No hint for other codes.
	}

	if command == cmdPut && (code == codes.DeadlineExceeded || code == codes.Unavailable) {
		hint += "; the transaction outcome may be unknown"
	}

	return hint
}

// cliErrorMessage drops the gRPC wrapping from status errors.
func cliErrorMessage(err error) string {
	if rpcStatus, ok := status.FromError(err); ok {
		return rpcStatus.Message()
	}

	return err.Error()
}
