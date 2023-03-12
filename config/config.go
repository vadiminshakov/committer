package config

import (
	"flag"
	"strings"
)

type Config struct {
	Role        string
	Nodeaddr    string
	Coordinator string
	Followers   []string
	Whitelist   []string
	CommitType  string
	Timeout     uint64
	DBPath      string
	WithTrace   bool
}

type followers []string

func (i *followers) String() string {
	return strings.Join(*i, ",")
}

func (i *followers) Set(value string) error {
	*i = append(*i, value)
	return nil
}

type whitelist []string

func (i *whitelist) String() string {
	return strings.Join(*i, ",")
}

func (i *whitelist) Set(value string) error {
	*i = append(*i, value)
	return nil
}

// Get creates configuration from yaml configuration file (if '-config=' flag specified) or command-line arguments.
func Get() *Config {
	// command-line flags
	role := flag.String("role", "follower", "role (coordinator of follower)")
	nodeaddr := flag.String("nodeaddr", "localhost:3050", "node address")
	coordinator := flag.String("coordinator", "", "coordinator address")
	committype := flag.String("committype", "three-phase", "two-phase or three-phase commit mode")
	timeout := flag.Uint64("timeout", 1000, "ms, timeout after which the message is considered unacknowledged (only for three-phase mode, because two-phase is blocking by design)")
	dbpath := flag.String("dbpath", "./badger", "database path on filesystem")
	withTrace := flag.Bool("withtrace", false, "use distributed tracer or not (true/false)")
	followers := flag.String("followers", "", "follower's addresses")
	whitelist := flag.String("whitelist", "127.0.0.1", "allowed hosts")
	flag.Parse()

	followersArray := strings.Split(*followers, ",")
	if *role != "coordinator" {
		if !includes(followersArray, *nodeaddr) {
			followersArray = append(followersArray, *nodeaddr)
		}
	}
	whitelistArray := strings.Split(*whitelist, ",")
	return &Config{*role, *nodeaddr, *coordinator,
		followersArray, whitelistArray, *committype,
		*timeout, *dbpath, *withTrace}

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
