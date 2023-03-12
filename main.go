package main

import (
	"github.com/vadiminshakov/committer/cache"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/algorithm"
	"github.com/vadiminshakov/committer/core/algorithm/hooks/src"
	"github.com/vadiminshakov/committer/core/coordinator"
	"github.com/vadiminshakov/committer/io/db"
	server2 "github.com/vadiminshakov/committer/io/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	conf := config.Get()

	database, err := db.New(conf.DBPath)
	if err != nil {
		panic(err)
	}

	c := cache.New()
	coord, err := coordinator.New(conf, c, database)
	if err != nil {
		panic(err)
	}

	s, err := server2.New(conf, algorithm.NewCommitter(database, c, src.Propose, src.Commit), coord, database)
	if err != nil {
		panic(err)
	}

	s.Run(server2.WhiteListChecker)
	<-ch
	s.Stop()
}
