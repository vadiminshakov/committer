// Package main provides a distributed consensus system implementing Two-Phase Commit (2PC)
// and Three-Phase Commit (3PC) protocols for distributed transactions.
//
// Committer is a Go implementation of distributed consensus protocols that allows
// multiple nodes to agree on transaction outcomes in a distributed environment.
// The system consists of coordinators that manage transactions and cohorts that
// participate in the consensus process.
//
// Key features:
//   - Support for both 2PC and 3PC protocols
//   - BadgerDB for persistent storage with WAL for reliability
//   - Configurable timeouts and node addresses
//   - Extensible hook system for custom validation and business logic
//   - gRPC-based communication between nodes
//
// Usage:
//
//	# Start coordinator
//	./committer -role=coordinator -nodeaddr=localhost:3000 -cohorts=localhost:3001,3002
//
//	# Start cohort
//	./committer -role=cohort -coordinator=localhost:3000 -nodeaddr=localhost:3001
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
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/committer/io/store"
	"github.com/vadiminshakov/gowal"
)

func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	conf := config.Get()

	wal := initWAL()
	stateStore, recovery := initStore(conf, wal)

	s := initServer(conf, stateStore, wal, recovery.NextHeight)
	s.Run(server.WhiteListChecker)

	<-ch
	s.Stop()
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

func initStore(conf *config.Config, wal *gowal.Wal) (*store.Store, *store.RecoveryState) {
	stateStore, recovery, err := store.New(wal, conf.DBPath)
	if err != nil {
		log.Fatalf("failed to initialize state store: %v", err)
	}

	log.Printf("Recovered state from WAL: next height %d, keys %d\n", recovery.NextHeight, stateStore.Size())
	return stateStore, recovery
}

func initServer(conf *config.Config, stateStore *store.Store, wal *gowal.Wal, initialHeight uint64) *server.Server {
	committer := commitalgo.NewCommitter(stateStore, conf.CommitType, wal, conf.Timeout)
	committer.SetHeight(initialHeight)
	cohortImpl := cohort.NewCohort(committer, cohort.Mode(conf.CommitType))
	coordinatorImpl, err := coordinator.New(conf, wal, stateStore)
	if err != nil {
		log.Fatalf("failed to create coordinator: %v", err)
	}
	coordinatorImpl.SetHeight(initialHeight)

	s, err := server.New(conf, cohortImpl, coordinatorImpl, stateStore)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	return s
}
