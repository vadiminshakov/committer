package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/cohort"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/coordinator"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/gowal"
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
		log.Fatalf("failed to open database: %v", err)
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
		log.Fatalf("failed to create WAL: %v", err)
	}

	committer := commitalgo.NewCommitter(database, conf.CommitType, wal, hooks.Propose, hooks.Commit, conf.Timeout)
	cohortImpl := cohort.NewCohort(committer, cohort.Mode(conf.CommitType))
	coordinatorImpl, err := coordinator.New(conf, wal, database)
	if err != nil {
		log.Fatalf("failed to create coordinator: %v", err)
	}

	s, err := server.New(conf, cohortImpl, coordinatorImpl, database)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	s.Run(server.WhiteListChecker)

	<-ch
	s.Stop()
}
