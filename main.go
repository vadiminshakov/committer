package main

import (
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/helpers"
	pb "github.com/vadiminshakov/committer/proto"
	"github.com/vadiminshakov/committer/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	conf := config.Get()

	var (
		propose helpers.ProposeHook = func(req *pb.ProposeRequest) bool {
			return true
		}
		commit helpers.CommitHook = func(req *pb.CommitRequest) bool {
			return true
		}
	)

	s, err := server.NewCommitServer(conf, server.WithProposeHook(propose),
		server.WithCommitHook(commit))
	if err != nil {
		panic(err)
	}

	s.Run(server.WhiteListChecker)
	<-ch
	s.Stop()
}
