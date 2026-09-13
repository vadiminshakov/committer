// Package config provides configuration management for the committer application.
//
// This package handles command-line flag parsing and configuration validation
// for both coordinator and cohort nodes in the distributed consensus system.
package config

import (
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

func Get() *Config {
	conf, err := Parse(os.Args[1:], os.Stderr)
	if err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}

		fmt.Fprintln(os.Stderr, err)

		os.Exit(2)
	}

	return conf
}

func Parse(args []string, output io.Writer) (*Config, error) {
	role := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		role, args = args[0], args[1:]
		if role != "coordinator" && role != "cohort" {
			return nil, fmt.Errorf("unknown node command %q", role)
		}
	}
	fs := flag.NewFlagSet("committer "+role, flag.ContinueOnError)
	fs.SetOutput(output)
	conf := &Config{}
	fs.StringVar(&conf.Nodeaddr, "nodeaddr", "localhost:3050", "node listen address (host:port)")
	fs.StringVar(&conf.Coordinator, "coordinator", "", "coordinator address (required for cohort command)")
	fs.StringVar(&conf.CommitType, "committype", "two-phase", "two-phase or three-phase")
	timeout := fs.String("timeout", "1s", "3PC timeout, e.g. 1s or 500ms (bare numbers mean milliseconds)")
	cohorts := fs.String("cohorts", "", "comma-separated participant addresses (required for coordinator command)")
	fs.IntVar(&conf.VizPort, "viz-port", 0, "protocol visualization port (0 disables it)")
	fs.StringVar(&conf.DataDir, "data-dir", ".data", "persistent data root; keep the same path when restarting")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() != 0 {
		return nil, fmt.Errorf("unexpected argument %q; node commands accept flags only", fs.Arg(0))
	}
	conf.Cohorts = filterEmpty(strings.Split(*cohorts, ","))
	conf.Role = role
	if conf.Role == "" {
		conf.Role = "cohort"
		if len(conf.Cohorts) > 0 {
			conf.Role = "coordinator"
		}
	}
	if conf.CommitType != "two-phase" && conf.CommitType != "three-phase" {
		return nil, fmt.Errorf("invalid -committype %q: use two-phase or three-phase", conf.CommitType)
	}
	if conf.Role == "coordinator" {
		if len(conf.Cohorts) == 0 {
			return nil, fmt.Errorf("coordinator requires -cohorts=host:port[,host:port]")
		}
		if conf.Coordinator != "" {
			return nil, fmt.Errorf("-coordinator is only valid for a cohort")
		}
	} else {
		if len(conf.Cohorts) > 0 {
			return nil, fmt.Errorf("cohort cannot use -cohorts; use -coordinator=host:port")
		}
		if role != "" && conf.Coordinator == "" {
			return nil, fmt.Errorf("cohort requires -coordinator=host:port")
		}
	}
	if err := ValidateAddress(conf.Nodeaddr); err != nil {
		return nil, fmt.Errorf("-nodeaddr: %w", err)
	}
	if conf.Coordinator != "" {
		if err := ValidateAddress(conf.Coordinator); err != nil {
			return nil, fmt.Errorf("-coordinator: %w", err)
		}
		if conf.Coordinator == conf.Nodeaddr {
			return nil, fmt.Errorf("cohort and coordinator must use different addresses")
		}
	}
	seen := map[string]bool{}
	for _, addr := range conf.Cohorts {
		if err := ValidateAddress(addr); err != nil {
			return nil, fmt.Errorf("-cohorts: %w", err)
		}
		if addr == conf.Nodeaddr {
			return nil, fmt.Errorf("coordinator cannot list itself in -cohorts")
		}
		if seen[addr] {
			return nil, fmt.Errorf("duplicate cohort address %q", addr)
		}
		seen[addr] = true
	}
	if conf.VizPort < 0 || conf.VizPort > 65535 {
		return nil, fmt.Errorf("-viz-port must be between 0 and 65535")
	}
	if strings.TrimSpace(conf.DataDir) == "" {
		return nil, fmt.Errorf("-data-dir must not be empty")
	}
	duration, err := time.ParseDuration(*timeout)
	if ms, parseErr := strconv.ParseUint(*timeout, 10, 64); parseErr == nil {
		if ms > uint64((1<<63-1)/int64(time.Millisecond)) {
			return nil, fmt.Errorf("-timeout is too large")
		}
		duration, err = time.Duration(ms)*time.Millisecond, nil
	}
	if err != nil || duration < time.Millisecond || duration%time.Millisecond != 0 {
		return nil, fmt.Errorf("-timeout must be a positive whole number of milliseconds, e.g. 500ms or 1s")
	}
	conf.Timeout = uint64(duration / time.Millisecond)
	return conf, nil
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

// filterEmpty trims and removes empty entries from a slice of strings
func filterEmpty(values []string) []string {
	result := make([]string, 0, len(values))
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
