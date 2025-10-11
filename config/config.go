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
	// DefaultWalDir is the default directory for write-ahead logs.
	DefaultWalDir string = "wal"
	// DefaultWalSegmentPrefix is the default prefix for WAL segment files.
	DefaultWalSegmentPrefix string = "msgs_"
	// DefaultWalSegmentThreshold is the default number of entries per WAL segment.
	DefaultWalSegmentThreshold int = 10000
	// DefaultWalMaxSegments is the default maximum number of WAL segments to retain.
	DefaultWalMaxSegments int = 100
	// DefaultWalIsInSyncDiskMode enables synchronous disk writes for WAL by default.
	DefaultWalIsInSyncDiskMode bool = true
)

// Config holds the configuration settings for the committer application.
type Config struct {
	Role        string   // Node role: "coordinator" or "cohort"
	Nodeaddr    string   // Address of this node
	Coordinator string   // Address of the coordinator (for cohorts)
	CommitType  string   // Commit protocol: "two-phase" or "three-phase"
	DBPath      string   // Path to the database directory
	Cohorts     []string // List of cohort addresses (for coordinators)
	Whitelist   []string // Whitelist of allowed node addresses
	Timeout     uint64   // Timeout in milliseconds for 3PC operations
}

// Get creates configuration from yaml configuration file (if '-config=' flag specified) or command-line arguments.
func Get() *Config {
	// command-line flags
	role := flag.String("role", "cohort", "role (coordinator or cohort)")
	nodeaddr := flag.String("nodeaddr", "localhost:3050", "node address")
	coordinator := flag.String("coordinator", "", "coordinator address")
	committype := flag.String("committype", "two-phase", "two-phase or three-phase commit mode")
	timeout := flag.Uint64("timeout", 1000, "ms, timeout after which the message is considered unacknowledged (only for three-phase mode, because two-phase is blocking by design)")
	dbpath := flag.String("dbpath", "./badger", "database path on filesystem")
	cohorts := flag.String("cohorts", "", "cohort addresses")
	whitelist := flag.String("whitelist", "127.0.0.1", "allowed hosts")
	flag.Parse()

	cohortsArray := strings.Split(*cohorts, ",")
	if *role != "coordinator" {
		if !includes(cohortsArray, *nodeaddr) {
			cohortsArray = append(cohortsArray, *nodeaddr)
		}
	}
	whitelistArray := strings.Split(*whitelist, ",")
	return &Config{Role: *role, Nodeaddr: *nodeaddr, Coordinator: *coordinator,
		CommitType: *committype, DBPath: *dbpath, Cohorts: cohortsArray, Whitelist: whitelistArray,
		Timeout: *timeout}

}

// includes checks that the 'arr' includes 'value'
func includes(arr []string, value string) bool {
	for i := range arr {
		if arr[i] == value {
			return true
		}
	}
	return false
}
