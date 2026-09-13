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

func execute(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(stdout, usage)
		return nil
	}
	switch args[0] {
	case "put", "get", "status":
		return runClientCommand(args[0], args[1:], stdout, stderr)
	case "coordinator", "cohort":
	default:
		if !strings.HasPrefix(args[0], "-") {
			return fmt.Errorf("unknown command %q; run 'committer --help'", args[0])
		}
	}
	conf, err := config.Parse(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	return startNode(conf)
}

func runClientCommand(command string, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("committer "+command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", "localhost:3000", "target node address (put requires a coordinator)")
	timeout := fs.Duration("timeout", 5*time.Second, "request deadline, e.g. 5s or 500ms")
	fs.Usage = func() {
		suffix := map[string]string{"put": "KEY VALUE", "get": "KEY", "status": ""}[command]
		fmt.Fprintf(stderr, "Usage: committer %s [flags] %s\n", command, suffix)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	expected := map[string]int{"put": 2, "get": 1, "status": 0}[command]
	if fs.NArg() != expected {
		fs.Usage()
		return fmt.Errorf("%s requires %d arguments; place flags before arguments", command, expected)
	}
	if err := config.ValidateAddress(*addr); err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("-timeout must be positive")
	}
	if command != "status" && fs.Arg(0) == "" {
		return fmt.Errorf("key must not be empty")
	}
	cli, err := client.NewClientAPI(*addr)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", *addr, err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	switch command {
	case "put":
		var resp *pb.Response
		resp, err = cli.Put(ctx, fs.Arg(0), []byte(fs.Arg(1)))
		if err == nil {
			if resp.Type != pb.Type_ACK {
				return fmt.Errorf("transaction %d was rejected", resp.Index)
			}
			fmt.Fprintf(stdout, "Committed transaction %d\n", resp.Index)
		}
	case "get":
		var resp *pb.Value
		resp, err = cli.Get(ctx, fs.Arg(0))
		if err == nil {
			fmt.Fprintln(stdout, string(resp.Value))
		}
	case "status":
		var resp *pb.Info
		resp, err = cli.NodeInfo(ctx)
		if err == nil {
			fmt.Fprintf(stdout, "Node: %s\nReachable: yes\nHeight: %d\n", *addr, resp.Height)
		}
	}
	if err != nil {
		hint := ""
		switch status.Code(err) {
		case codes.Unavailable:
			hint = "; check that the node is running and --addr is correct"
		case codes.DeadlineExceeded:
			hint = "; check node connectivity or increase --timeout"
		case codes.FailedPrecondition:
			hint = "; for put, check the coordinator address and its participants"
		}
		if command == "put" && (status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Unavailable) {
			hint += "; the transaction outcome may be unknown"
		}
		return fmt.Errorf("%s at %s failed: %s%s", command, *addr, status.Convert(err).Message(), hint)
	}
	return nil
}
