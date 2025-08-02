package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/cohort"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo"
	"github.com/vadiminshakov/committer/core/coordinator"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/gowal"
)

func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	conf := config.Get()

	database := initDB(conf.DBPath)
	wal := initWAL()

	s := initServer(conf, database, wal)
	s.Run(server.WhiteListChecker)

	<-ch
	s.Stop()
}

func initDB(dbpath string) db.Repository {
	database, err := db.New(dbpath)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	return database
}

func initWAL() *gowal.Wal {
	walConfig := gowal.Config{
		Dir:              config.DefaultWalDir,
		Prefix:           config.DefaultWalSegmentPrefix,
		SegmentThreshold: config.DefaultWalSegmentThreshold,
		MaxSegments:      config.DefaultWalMaxSegments,
		IsInSyncDiskMode: config.DefaultWalIsInSyncDiskMode,
	}

	w, err := gowal.NewWAL(walConfig)
	if err != nil {
		log.Fatalf("failed to create WAL: %v", err)
	}

	return w
}

func initServer(conf *config.Config, database db.Repository, wal *gowal.Wal) *server.Server {
	committer := commitalgo.NewCommitter(database, conf.CommitType, wal, conf.Timeout)
	cohortImpl := cohort.NewCohort(committer, cohort.Mode(conf.CommitType))
	coordinatorImpl, err := coordinator.New(conf, wal, database)
	if err != nil {
		log.Fatalf("failed to create coordinator: %v", err)
	}

	s, err := server.New(conf, cohortImpl, coordinatorImpl, database)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	return s
}
