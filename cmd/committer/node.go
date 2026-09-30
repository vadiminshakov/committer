package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/vadiminshakov/committer/v2/cmd/committer/internal/cliapi"
	"github.com/vadiminshakov/committer/v2/cmd/committer/internal/viz"
	"github.com/vadiminshakov/committer/v2/core/cohort"
	"github.com/vadiminshakov/committer/v2/core/coordinator"
	"github.com/vadiminshakov/committer/v2/core/dto"
	"github.com/vadiminshakov/committer/v2/events"
	"github.com/vadiminshakov/committer/v2/io/store"
	iowal "github.com/vadiminshakov/committer/v2/io/wal"
)

const (
	roleCoordinator = "coordinator"
	roleCohort      = "cohort"
)

// nodeConfig holds the flags of a node command.
type nodeConfig struct {
	Role        string // roleCoordinator or roleCohort
	Nodeaddr    string // protocol traffic
	ClientAddr  string // serves put and get; empty disables the client API
	Coordinator string // cohort only
	Cohorts     []string
	Protocol    dto.Protocol
	Timeout     time.Duration // 3PC autocommit delay
	DataDir     string
	VizPort     int // 0 disables the visualization
}

// runNode parses node flags, starts the node and stops it on a signal.
func runNode(args []string, stderr io.Writer) error {
	conf, err := parseNodeFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}

	if err != nil {
		return err
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(stderr, nil)))
	slog.Info("Starting node", "role", conf.Role, "protocol", conf.Protocol,
		"addr", conf.Nodeaddr, "clientaddr", conf.ClientAddr,
		"coordinator", conf.Coordinator, "cohorts", conf.Cohorts,
		"wal", iowal.Dir(conf.DataDir, conf.Role, conf.Nodeaddr))

	var emitter events.Emitter = events.NoopEmitter{}

	if conf.VizPort > 0 {
		collector := viz.NewCollector(nil)
		viz.NewServer(collector, viz.Node{
			Role:        conf.Role,
			Addr:        conf.Nodeaddr,
			Coordinator: conf.Coordinator,
			Cohorts:     conf.Cohorts,
			CommitType:  conf.Protocol.String(),
		}, conf.VizPort).Start()
		slog.Info("Protocol visualization", "url", fmt.Sprintf("http://localhost:%d", conf.VizPort))

		emitter = collector
	}

	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer signal.Stop(signals)

	stop, err := startNode(context.Background(), conf, emitter)
	if err != nil {
		return err
	}

	<-signals

	return stop()
}

// startNode starts a cluster node. A cohort keeps the data
// in a Badger store, its resource. With -clientaddr, the coordinator serves put
// and a cohort serves get on that address. The returned function stops the
// node.
func startNode(ctx context.Context, conf *nodeConfig, emitter events.Emitter) (func() error, error) {
	if conf.Role == roleCohort {
		return startCohort(ctx, conf, emitter)
	}

	coord, err := coordinator.Start(coordinator.Config{
		Addr:     dto.Addr(conf.Nodeaddr),
		Cohorts:  addrs(conf.Cohorts),
		Protocol: conf.Protocol,
		DataDir:  conf.DataDir,
		Emitter:  emitter,
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already describes the failure
	}

	return withClientAPI(conf, coord, nil, coord.Close)
}

// addrs converts flag values checked by validateAddresses.
func addrs(values []string) []dto.Addr {
	out := make([]dto.Addr, len(values))
	for i, v := range values {
		out[i] = dto.Addr(v)
	}

	return out
}

func startCohort(ctx context.Context, conf *nodeConfig, emitter events.Emitter) (func() error, error) {
	dbPath := filepath.Join(conf.DataDir, "db", roleCohort, strings.NewReplacer(":", "_", "/", "_").Replace(conf.Nodeaddr))
	slog.Info("State store", "db", dbPath)

	stateStore, err := store.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open state store: %w", err)
	}

	participant, err := cohort.Start(ctx, cohort.Config{
		Addr:        dto.Addr(conf.Nodeaddr),
		Coordinator: dto.Addr(conf.Coordinator),
		Protocol:    conf.Protocol,
		Timeout:     conf.Timeout,
		DataDir:     conf.DataDir,
		Emitter:     emitter,
	}, stateStore)
	if err != nil {
		return nil, errors.Join(err, stateStore.Close())
	}

	return withClientAPI(conf, nil, stateStore, func() error {
		return errors.Join(participant.Close(), stateStore.Close())
	})
}

// withClientAPI serves the client API on -clientaddr, if set, for a started node. The
// returned function stops the client API, then the node through stop. If the
// client API cannot start, the node is stopped.
func withClientAPI(
	conf *nodeConfig,
	committer cliapi.Committer,
	reader cliapi.Reader,
	stop func() error,
) (func() error, error) {
	if conf.ClientAddr == "" {
		return stop, nil
	}

	stopClientAPI, err := cliapi.Serve(conf.ClientAddr, committer, reader)
	if err != nil {
		return nil, errors.Join(err, stop())
	}

	return func() error {
		stopClientAPI()

		return stop()
	}, nil
}

// parseNodeFlags parses the arguments of a node command. The first argument
// may name the role; without it, -cohorts selects the coordinator role.
func parseNodeFlags(args []string, output io.Writer) (*nodeConfig, error) {
	role := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		role, args = args[0], args[1:]
	}

	flagset := flag.NewFlagSet(strings.TrimSpace("committer "+role), flag.ContinueOnError)
	flagset.SetOutput(output)

	conf := &nodeConfig{Role: role}
	flagset.StringVar(&conf.Nodeaddr, "nodeaddr", "localhost:3050", "node listen address (host:port)")
	flagset.StringVar(&conf.ClientAddr, "clientaddr", "",
		"serve put (coordinator) and get (cohort) on this address (host:port); empty disables it")
	flagset.StringVar(&conf.Coordinator, "coordinator", "", "coordinator address (required for cohort command)")
	commitType := flagset.String("committype", dto.ProtocolTwoPhase.String(), "two-phase or three-phase")
	timeout := flagset.String("timeout", "1s", "3PC timeout, e.g. 1s or 500ms (bare numbers mean milliseconds)")
	cohorts := flagset.String("cohorts", "", "comma-separated participant addresses (required for coordinator command)")
	flagset.IntVar(&conf.VizPort, "viz-port", 0, "protocol visualization port (0 disables it)")
	flagset.StringVar(&conf.DataDir, "data-dir", ".data", "persistent data root; keep the same path when restarting")

	if err := flagset.Parse(args); err != nil {
		return nil, err //nolint:wrapcheck // flag already printed the problem
	}

	if flagset.NArg() != 0 {
		return nil, fmt.Errorf("unexpected argument %q; node commands accept flags only", flagset.Arg(0))
	}

	for addr := range strings.SplitSeq(*cohorts, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			conf.Cohorts = append(conf.Cohorts, addr)
		}
	}

	if conf.Role == "" {
		conf.Role = roleCohort
		if len(conf.Cohorts) > 0 {
			conf.Role = roleCoordinator
		}
	}

	switch *commitType {
	case dto.ProtocolTwoPhase.String():
		conf.Protocol = dto.ProtocolTwoPhase
	case dto.ProtocolThreePhase.String():
		conf.Protocol = dto.ProtocolThreePhase
	default:
		return nil, fmt.Errorf("invalid -committype %q: use two-phase or three-phase", *commitType)
	}

	var err error
	if conf.Timeout, err = parseTimeout(*timeout); err != nil {
		return nil, err
	}

	return conf, conf.validate()
}

func (conf *nodeConfig) validate() error {
	switch conf.Role {
	case roleCoordinator:
		if len(conf.Cohorts) == 0 {
			return errors.New("coordinator requires -cohorts=host:port[,host:port]")
		}

		if conf.Coordinator != "" {
			return errors.New("-coordinator is only valid for a cohort")
		}
	case roleCohort:
		if len(conf.Cohorts) > 0 {
			return errors.New("cohort cannot use -cohorts; use -coordinator=host:port")
		}

		if conf.Coordinator == "" {
			return errors.New("cohort requires -coordinator=host:port")
		}
	default:
		return fmt.Errorf("unknown node command %q", conf.Role)
	}

	if err := conf.validateAddresses(); err != nil {
		return err
	}

	if conf.VizPort < 0 || conf.VizPort > 65535 {
		return errors.New("-viz-port must be between 0 and 65535")
	}

	if strings.TrimSpace(conf.DataDir) == "" {
		return errors.New("-data-dir must not be empty")
	}

	return nil
}

func (conf *nodeConfig) validateAddresses() error {
	if err := dto.Addr(conf.Nodeaddr).Validate(); err != nil {
		return fmt.Errorf("-nodeaddr: %w", err)
	}

	if conf.Coordinator != "" {
		if err := dto.Addr(conf.Coordinator).Validate(); err != nil {
			return fmt.Errorf("-coordinator: %w", err)
		}

		if conf.Coordinator == conf.Nodeaddr {
			return errors.New("cohort and coordinator must use different addresses")
		}
	}

	seen := map[string]bool{}

	for _, addr := range conf.Cohorts {
		if err := dto.Addr(addr).Validate(); err != nil {
			return fmt.Errorf("-cohorts: %w", err)
		}

		if addr == conf.Nodeaddr {
			return errors.New("coordinator cannot list itself in -cohorts")
		}

		if seen[addr] {
			return fmt.Errorf("duplicate cohort address %q", addr)
		}

		seen[addr] = true
	}

	if conf.ClientAddr != "" {
		if err := dto.Addr(conf.ClientAddr).Validate(); err != nil {
			return fmt.Errorf("-clientaddr: %w", err)
		}

		if conf.ClientAddr == conf.Nodeaddr {
			return errors.New("-clientaddr must differ from -nodeaddr")
		}
	}

	return nil
}

// parseTimeout parses the -timeout flag: a Go duration string or a bare
// number of milliseconds.
func parseTimeout(raw string) (time.Duration, error) {
	duration, err := time.ParseDuration(raw)
	if ms, parseErr := strconv.ParseUint(raw, 10, 64); parseErr == nil {
		if ms > uint64((1<<63-1)/int64(time.Millisecond)) {
			return 0, errors.New("-timeout is too large")
		}

		duration, err = time.Duration(ms)*time.Millisecond, nil
	}

	if err != nil || duration < time.Millisecond || duration%time.Millisecond != 0 {
		return 0, errors.New("-timeout must be a positive whole number of milliseconds, e.g. 500ms or 1s")
	}

	return duration, nil
}
