package main

import (
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/cohort"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/coordinator"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/gowal"
	"os"
	"os/signal"
	"syscall"
)

const (
	walDir              string = "wal"
	walSegmentPrefix    string = "msgs_"
	walSegmentThreshold int    = 10000
	walMaxSegments      int    = 100
	walIsInSyncDiskMode bool   = true
)

func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	conf := config.Get()

	database, err := db.New(conf.DBPath)
	if err != nil {
		panic(err)
	}

	walConfig := gowal.Config{
		Dir:              walDir,
		Prefix:           walSegmentPrefix,
		SegmentThreshold: walSegmentThreshold,
		MaxSegments:      walMaxSegments,
		IsInSyncDiskMode: walIsInSyncDiskMode,
	}

	wal, err := gowal.NewWAL(walConfig)
	if err != nil {
		panic(err)
	}

	coordImpl, err := coordinator.New(conf, wal, database)
	if err != nil {
		panic(err)
	}

	committer := commitalgo.NewCommitter(database, conf.CommitType, wal, hooks.Propose, hooks.Commit, conf.Timeout)
	cohortImpl := cohort.NewCohort(committer, cohort.Mode(conf.CommitType))

	s, err := server.New(conf, cohortImpl, coordImpl, database)
	if err != nil {
		panic(err)
	}

	s.Run(server.WhiteListChecker)
	<-ch
	s.Stop()
}
