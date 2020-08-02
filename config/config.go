package config

import (
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"strings"
)

type Config struct {
	Role        string
	Nodeaddr    string
	Coordinator string
	Followers   []string
}

type followers []string

func (i *followers) String() string {
	return strings.Join(*i, ",")
}

func (i *followers) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func Get() *Config {
	// command-line flags
	var followersArray followers
	config := flag.String("config", "", "path to config")
	role := flag.String("role", "follower", "role (coordinator of follower)")
	nodeaddr := flag.String("nodeaddr", "localhost:3050", "node address")
	coordinator := flag.String("coordinator", "", "coordinator address")
	flag.Var(&followersArray, "follower", "follower address")
	flag.Parse()

	if *config == "" {
		if *role != "coordinator" {
			if !includes(followersArray, *nodeaddr) {
				followersArray = append(followersArray, *nodeaddr)
			}
		}
		return &Config{*role, *nodeaddr, *coordinator, followersArray}
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
		if !includes(configFromFile.Followers, configFromFile.Nodeaddr) {
			configFromFile.Followers = append(configFromFile.Followers, configFromFile.Nodeaddr)
		}
	}

	return &Config{configFromFile.Role, configFromFile.Nodeaddr, configFromFile.Coordinator, configFromFile.Followers}
}

func includes(arr []string, value string) bool {
	for i := range arr {
		if arr[i] == value {
			return true
		}
	}
	return false
}
