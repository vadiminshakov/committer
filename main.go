package main

import (
	"fmt"
	"github.com/openzipkin/zipkin-go"
	"github.com/vadiminshakov/committer/cache"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/cohort"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/coordinator"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/committer/io/trace"
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
	coordImpl, err := coordinator.New(conf, c, database)
	if err != nil {
		panic(err)
	}

	var tracer *zipkin.Tracer
	if conf.WithTrace {
		tracer, err = trace.Tracer(fmt.Sprintf("%s:%s", conf.Role, conf.Nodeaddr), conf.Nodeaddr)
		if err != nil {
			panic(err)
		}
	}

	committer := commitalgo.NewCommitter(database, c, hooks.Propose, hooks.Commit)
	cohortImpl := cohort.NewCohort(tracer, committer, cohort.Mode(conf.CommitType))

	s, err := server.New(conf, tracer, cohortImpl, coordImpl, database)
	if err != nil {
		panic(err)
	}

	s.Run(server.WhiteListChecker)
	<-ch
	s.Stop()
}
