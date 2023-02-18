package main

import (
	"github.com/vadiminshakov/committer/algoplagin"
	"github.com/vadiminshakov/committer/algoplagin/hooks/src"
	"github.com/vadiminshakov/committer/cache"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/db"
	"github.com/vadiminshakov/committer/server"
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
	s, err := server.NewCommitServer(conf, algoplagin.NewCommitter(database, c, src.Propose, src.Commit), database, c)
	if err != nil {
		panic(err)
	}

	s.Run(server.WhiteListChecker)
	<-ch
	s.Stop()
}
