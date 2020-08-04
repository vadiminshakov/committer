package config

import (
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"github.com/vadiminshakov/committer/helpers"
	"log"
	"strings"
)

type Config struct {
	Role        string
	Nodeaddr    string
	Coordinator string
	Followers   []string
	Whitelist   []string
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
	var (
		followersArray followers
		whitelistArray whitelist
	)
	config := flag.String("config", "", "path to config")
	role := flag.String("role", "follower", "role (coordinator of follower)")
	nodeaddr := flag.String("nodeaddr", "localhost:3050", "node address")
	coordinator := flag.String("coordinator", "", "coordinator address")
	flag.Var(&followersArray, "follower", "follower address")
	flag.Var(&whitelistArray, "whitelist", "allowed hosts")
	flag.Parse()

	if *config == "" {
		if *role != "coordinator" {
			if !helpers.Includes(followersArray, *nodeaddr) {
				followersArray = append(followersArray, *nodeaddr)
			}
		}
		if !helpers.Includes(whitelistArray, "127.0.0.1") {
			whitelistArray = append(whitelistArray, "127.0.0.1")
		}
		return &Config{*role, *nodeaddr, *coordinator, followersArray, whitelistArray}
	}

	// viper configuration
	var configFromFile Config
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(*config)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(fmt.Sprintf("Error reading config file, %s", err))
	}
	err := viper.Unmarshal(&configFromFile)
	if err != nil {
		log.Fatal("Unable to unmarshal config")
	}

	if configFromFile.Role != "coordinator" {
		if !helpers.Includes(configFromFile.Followers, configFromFile.Nodeaddr) {
			configFromFile.Followers = append(configFromFile.Followers, configFromFile.Nodeaddr)
		}
	}

	if !helpers.Includes(configFromFile.Whitelist, "127.0.0.1") {
		configFromFile.Whitelist = append(configFromFile.Whitelist, "127.0.0.1")
	}

	return &Config{configFromFile.Role, configFromFile.Nodeaddr, configFromFile.Coordinator, configFromFile.Followers, configFromFile.Whitelist}
}
