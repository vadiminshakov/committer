package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const usage = `Usage: committer <command> [flags] [arguments]

Node commands:
  coordinator -nodeaddr localhost:3000 -cohorts localhost:3001
  cohort -nodeaddr localhost:3001 -coordinator localhost:3000

Client commands (flags must precede arguments):
  put    --addr localhost:3000 KEY VALUE
  get    --addr localhost:3000 KEY
  status --addr localhost:3000

Use 'committer <command> -h' for command options.
The original flag-only node syntax is also supported.
`

const (
	// Client subcommands.
	cmdPut    = "put"
	cmdGet    = "get"
	cmdStatus = "status"
)

// Positional argument counts for client subcommands.
const (
	putArgCount    = 2
	getArgCount    = 1
	statusArgCount = 0
)

// defaultClientTimeout bounds every client request unless -timeout overrides it.
const defaultClientTimeout = 5 * time.Second

func execute(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(stdout, usage)

		return nil
	}

	switch args[0] {
	case cmdPut, cmdGet, cmdStatus:
		return runClientCommand(args[0], args[1:], stdout, stderr)
	case config.RoleCoordinator, config.RoleCohort:
	default:
		if !strings.HasPrefix(args[0], "-") {
			return fmt.Errorf("unknown command %q; run 'committer --help'", args[0])
		}
	}

	return startNodeFromArgs(args, stderr)
}

// startNodeFromArgs parses node flags and starts the node.
func startNodeFromArgs(args []string, stderr io.Writer) error {
	conf, err := config.Parse(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("parse node config: %w", err)
	}

	return startNode(conf)
}

func runClientCommand(command string, args []string, stdout, stderr io.Writer) error {
	flagset := flag.NewFlagSet("committer "+command, flag.ContinueOnError)
	flagset.SetOutput(stderr)
	addr := flagset.String("addr", "localhost:3000", "target node address (put requires a coordinator)")
	timeout := flagset.Duration("timeout", defaultClientTimeout, "request deadline, e.g. 5s or 500ms")

	flagset.Usage = func() {
		suffix := map[string]string{cmdPut: "KEY VALUE", cmdGet: "KEY", cmdStatus: ""}[command]
		fmt.Fprintf(stderr, "Usage: committer %s [flags] %s\n", command, suffix)
		flagset.PrintDefaults()
	}
	if err := flagset.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}

		return fmt.Errorf("parse flags: %w", err)
	}

	expected := map[string]int{cmdPut: putArgCount, cmdGet: getArgCount, cmdStatus: statusArgCount}[command]
	if flagset.NArg() != expected {
		flagset.Usage()

		return fmt.Errorf("%s requires %d arguments; place flags before arguments", command, expected)
	}

	if err := config.ValidateAddress(*addr); err != nil {
		return fmt.Errorf("invalid -addr flag: %w", err)
	}

	if *timeout <= 0 {
		return errors.New("-timeout must be positive")
	}

	if command != cmdStatus && flagset.Arg(0) == "" {
		return errors.New("key must not be empty")
	}

	cli, err := client.NewClientAPI(*addr)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", *addr, err)
	}

	defer func() {
		_ = cli.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := invokeClientOperation(cli, command, flagset.Args(), stdout, *addr, ctx); err != nil {
		return fmt.Errorf("%s at %s failed: %s%s",
			command, *addr, status.Convert(err).Message(), hintForClientError(command, err))
	}

	return nil
}

// invokeClientOperation executes one client subcommand and prints its result.
// Client API errors already carry operation context, so they pass through unwrapped.
//
//nolint:wrapcheck
func invokeClientOperation(
	cli *client.ClientAPIClient,
	command string,
	positional []string,
	stdout io.Writer,
	addr string,
	ctx context.Context,
) error {
	switch command {
	case cmdPut:
		resp, err := cli.Put(ctx, positional[0], []byte(positional[1]))
		if err != nil {
			return err
		}

		if resp.Type != pb.Type_ACK {
			return fmt.Errorf("transaction %d was rejected", resp.Index)
		}

		fmt.Fprintf(stdout, "Committed transaction %d\n", resp.Index)
	case cmdGet:
		resp, err := cli.Get(ctx, positional[0])
		if err != nil {
			return err
		}

		fmt.Fprintln(stdout, string(resp.Value))
	case cmdStatus:
		resp, err := cli.NodeInfo(ctx)
		if err != nil {
			return err
		}

		fmt.Fprintf(stdout, "Node: %s\nReachable: yes\nHeight: %d\n", addr, resp.Height)
	default:
		return fmt.Errorf("unknown client command %q", command)
	}

	return nil
}

// hintForClientError suggests likely causes for common client failures.
func hintForClientError(command string, err error) string {
	hint := ""

	switch status.Code(err) {
	case codes.Unavailable:
		hint = "; check that the node is running and --addr is correct"
	case codes.DeadlineExceeded:
		hint = "; check node connectivity or increase --timeout"
	case codes.FailedPrecondition:
		hint = "; for put, check the coordinator address and its participants"
	default:
		// No hint for other codes.
	}

	if command == cmdPut && (status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Unavailable) {
		hint += "; the transaction outcome may be unknown"
	}

	return hint
}
