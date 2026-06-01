// Package config provides configuration management for the committer application.
//
// This package handles command-line flag parsing and configuration validation
// for both coordinator and cohort nodes in the distributed consensus system.
package config

import (
	"flag"
	"strings"
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
	Role        string   // Node role: "coordinator" or "cohort"
	Nodeaddr    string   // Address of this node
	Coordinator string   // Address of the coordinator (for cohorts)
	CommitType  string   // Commit protocol: "two-phase" or "three-phase"
	Cohorts     []string // List of cohort addresses (for coordinators)
	Timeout     uint64   // Timeout in milliseconds for 3PC operations
	NoUI        bool     // Disable TUI, use plain slog text logging to stderr
	VizPort     int      // Port for web visualization server (0 = disabled)
}

// Get creates configuration from yaml configuration file (if '-config=' flag specified) or command-line arguments.
func Get() *Config {
	// command-line flags
	nodeaddr := flag.String("nodeaddr", "localhost:3050", "node address")
	coordinator := flag.String("coordinator", "", "coordinator address")
	committype := flag.String("committype", "two-phase", "two-phase or three-phase commit mode")
	timeout := flag.Uint64("timeout", 1000, "ms, timeout after which the message is considered unacknowledged (only for three-phase mode, because two-phase is blocking by design)")
	cohorts := flag.String("cohorts", "", "cohort addresses")
	noUI := flag.Bool("no-ui", false, "disable TUI, use plain slog text logging to stderr")
	vizPort := flag.Int("viz-port", 0, "port for web visualization server (0 = disabled)")
	flag.Parse()

	cohortsArray := filterEmpty(strings.Split(*cohorts, ","))

	role := "cohort"
	if len(cohortsArray) > 0 {
		role = "coordinator"
	}

	return &Config{
		Role:        role,
		Nodeaddr:    *nodeaddr,
		Coordinator: *coordinator,
		CommitType:  *committype,
		Cohorts:     cohortsArray,
		Timeout:     *timeout,
		NoUI:        *noUI,
		VizPort:     *vizPort,
	}

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
