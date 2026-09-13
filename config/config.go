// Package config provides configuration management for the committer application.
//
// This package handles command-line flag parsing and configuration validation
// for both coordinator and cohort nodes in the distributed consensus system.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config holds the configuration settings for the committer application.
type Config struct {
	DataDir     string   // Root directory for persistent node data
	Role        string   // Node role: "coordinator" or "cohort"
	Nodeaddr    string   // Address of this node
	Coordinator string   // Address of the coordinator (for cohorts)
	CommitType  string   // Commit protocol: "two-phase" or "three-phase"
	Cohorts     []string // List of cohort addresses (for coordinators)
	Timeout     uint64   // Timeout in milliseconds for 3PC operations
	VizPort     int      // Port for web visualization server (0 = disabled)
}

const (
	// DefaultWalSegmentPrefix is the default prefix for WAL segment files.
	DefaultWalSegmentPrefix string = "msgs_"
	// DefaultWalSegmentThreshold is the default number of entries per WAL segment.
	DefaultWalSegmentThreshold int = 10000
	// DefaultWalMaxSegments is the default maximum number of WAL segments to retain.
	DefaultWalMaxSegments int = 100
	// DefaultWalIsInSyncDiskMode enables synchronous disk writes for WAL by default.
	DefaultWalIsInSyncDiskMode bool = true
)

const (
	// RoleCoordinator runs atomic commit transactions.
	RoleCoordinator = "coordinator"
	// RoleCohort participates in atomic commit transactions.
	RoleCohort = "cohort"
	// CommitTwoPhase selects the two-phase commit protocol.
	CommitTwoPhase = "two-phase"
	// CommitThreePhase selects the three-phase commit protocol.
	CommitThreePhase = "three-phase"
)

// DBPath returns the database directory path for the given role and node address.
// The node address is included so that multiple nodes of the same role running on
// one host (e.g. several cohorts during local testing) do not share a data directory.
func DBPath(role, nodeaddr string) string {
	return "./.data/db/" + role + "/" + sanitizeAddr(nodeaddr)
}

// WalDir returns the WAL directory path for the given role and node address.
// As with DBPath, the node address keeps per-node data directories distinct.
func WalDir(role, nodeaddr string) string {
	return "./.data/wal/" + role + "/" + sanitizeAddr(nodeaddr)
}

// sanitizeAddr turns a node address into a filesystem-safe directory name.
func sanitizeAddr(addr string) string {
	if addr == "" {
		return "default"
	}

	replacer := strings.NewReplacer(":", "_", "/", "_")

	return replacer.Replace(addr)
}

func Get() *Config {
	conf, err := Parse(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}

		fmt.Fprintln(os.Stderr, err)

		os.Exit(2) //nolint:mnd
	}

	return conf
}

func Parse(args []string, output io.Writer) (*Config, error) {
	role := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		role, args = args[0], args[1:]
		if role != RoleCoordinator && role != RoleCohort {
			return nil, fmt.Errorf("unknown node command %q", role)
		}
	}

	flagset := flag.NewFlagSet("committer "+role, flag.ContinueOnError)
	flagset.SetOutput(output)

	conf := &Config{}
	flagset.StringVar(&conf.Nodeaddr, "nodeaddr", "localhost:3050", "node listen address (host:port)")
	flagset.StringVar(&conf.Coordinator, "coordinator", "", "coordinator address (required for cohort command)")
	flagset.StringVar(&conf.CommitType, "committype", CommitTwoPhase, "two-phase or three-phase")
	timeout := flagset.String("timeout", "1s", "3PC timeout, e.g. 1s or 500ms (bare numbers mean milliseconds)")
	cohorts := flagset.String("cohorts", "", "comma-separated participant addresses (required for coordinator command)")
	flagset.IntVar(&conf.VizPort, "viz-port", 0, "protocol visualization port (0 disables it)")
	flagset.StringVar(&conf.DataDir, "data-dir", ".data", "persistent data root; keep the same path when restarting")

	if err := flagset.Parse(args); err != nil {
		return nil, fmt.Errorf("parse flags: %w", err)
	}

	if flagset.NArg() != 0 {
		return nil, fmt.Errorf("unexpected argument %q; node commands accept flags only", flagset.Arg(0))
	}

	conf.Cohorts = filterEmpty(strings.Split(*cohorts, ","))

	conf.Role = role
	if conf.Role == "" {
		conf.Role = RoleCohort
		if len(conf.Cohorts) > 0 {
			conf.Role = RoleCoordinator
		}
	}

	if err := validateConfig(conf, role, *timeout); err != nil {
		return nil, err
	}

	return conf, nil
}

// validateConfig checks flag combinations and derived values.
func validateConfig(conf *Config, role, rawTimeout string) error {
	if conf.CommitType != CommitTwoPhase && conf.CommitType != CommitThreePhase {
		return fmt.Errorf("invalid -committype %q: use two-phase or three-phase", conf.CommitType)
	}

	if conf.Role == RoleCoordinator {
		if err := checkCoordinatorFlags(conf); err != nil {
			return err
		}
	} else if err := checkCohortFlags(conf, role); err != nil {
		return err
	}

	if err := checkAddresses(conf); err != nil {
		return err
	}

	if conf.VizPort < 0 || conf.VizPort > 65535 {
		return errors.New("-viz-port must be between 0 and 65535")
	}

	if strings.TrimSpace(conf.DataDir) == "" {
		return errors.New("-data-dir must not be empty")
	}

	duration, err := parseTimeout(rawTimeout)
	if err != nil {
		return err
	}

	conf.Timeout = uint64(duration / time.Millisecond)

	return nil
}

// checkCoordinatorFlags validates flags that only make sense for a coordinator.
func checkCoordinatorFlags(conf *Config) error {
	if len(conf.Cohorts) == 0 {
		return errors.New("coordinator requires -cohorts=host:port[,host:port]")
	}

	if conf.Coordinator != "" {
		return errors.New("-coordinator is only valid for a cohort")
	}

	return nil
}

// checkCohortFlags validates flags that only make sense for a cohort.
// An explicit cohort subcommand additionally requires -coordinator.
func checkCohortFlags(conf *Config, role string) error {
	if len(conf.Cohorts) > 0 {
		return errors.New("cohort cannot use -cohorts; use -coordinator=host:port")
	}

	if role != "" && conf.Coordinator == "" {
		return errors.New("cohort requires -coordinator=host:port")
	}

	return nil
}

// checkAddresses validates the node, coordinator and cohort addresses.
func checkAddresses(conf *Config) error {
	if err := ValidateAddress(conf.Nodeaddr); err != nil {
		return fmt.Errorf("-nodeaddr: %w", err)
	}

	if conf.Coordinator != "" {
		if err := ValidateAddress(conf.Coordinator); err != nil {
			return fmt.Errorf("-coordinator: %w", err)
		}

		if conf.Coordinator == conf.Nodeaddr {
			return errors.New("cohort and coordinator must use different addresses")
		}
	}

	seen := map[string]bool{}

	for _, addr := range conf.Cohorts {
		if err := ValidateAddress(addr); err != nil {
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

func ValidateAddress(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || strings.TrimSpace(host) == "" || strings.ContainsAny(host, " /\\\t\n") {
		return fmt.Errorf("invalid address %q: expected host:port", addr)
	}

	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid port in %q: expected 1–65535", addr)
	}

	return nil
}

func (c *Config) dataRoot() string {
	if c.DataDir == "" {
		return ".data"
	}

	return c.DataDir
}

func (c *Config) DBPath() string {
	return filepath.Join(c.dataRoot(), "db", c.Role, sanitizeAddr(c.Nodeaddr))
}

func (c *Config) WalDir() string {
	return filepath.Join(c.dataRoot(), "wal", c.Role, sanitizeAddr(c.Nodeaddr))
}

// filterEmpty trims and removes empty entries from a slice of strings.
func filterEmpty(values []string) []string {
	result := make([]string, 0, len(values))
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}
