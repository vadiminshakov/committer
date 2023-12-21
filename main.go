package main

import (
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/cohort"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/coordinator"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/committer/voteslog"
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

	var l voteslog.Log
	if t := os.Getenv("LOG"); t == "disk" {
		l, err = voteslog.NewOnDiskLog("./logdata")
		if err != nil {
			panic(err)
		}
	} else {
		l = voteslog.NewInmemLog()
	}

	coordImpl, err := coordinator.New(conf, l, database)
	if err != nil {
		panic(err)
	}

	committer := commitalgo.NewCommitter(database, l, hooks.Propose, hooks.Commit, conf.Timeout)
	cohortImpl := cohort.NewCohort(committer, cohort.Mode(conf.CommitType))

	s, err := server.New(conf, cohortImpl, coordImpl, database)
	if err != nil {
		panic(err)
	}

	s.Run(server.WhiteListChecker)
	<-ch
	s.Stop()
}
