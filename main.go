package main

import (
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	conf := config.Get()
	s, err := server.NewCommitServer(conf.Nodeaddr, server.WithFollowers(conf.Followers), server.WithConfig(conf))
	if err != nil {
		panic(err)
	}
	s.Run()
	<-ch
	s.Stop()
}
