package main

import (
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/server"
)

func main() {
	conf := config.Get()
	s, err := server.NewCommitServer(conf.Nodeaddr, server.WithFollowers(conf.Followers))
	if err != nil {
		panic(err)
	}
	s.Run()
}
