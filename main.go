// Package main provides a distributed consensus system implementing Two-Phase Commit (2PC)
// and Three-Phase Commit (3PC) protocols for distributed transactions.
//
// Committer is a Go implementation of distributed atomic commit protocols that allows
// you to achieve data consistency in distributed systems using Two-Phase Commit (2PC)
// and Three-Phase Commit (3PC) protocols for distributed transactions.
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
	"fmt"
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
	if err := run(); err != nil {
		log.Fatalf("committer failed: %v", err)
	}
}

func run() error {
	ctx := make(chan os.Signal, 1)
	signal.Notify(ctx, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	conf := config.Get()

	wal, err := newWAL()
	if err != nil {
		return err
	}
	defer wal.Close()

	stateStore, recovery, err := newStore(conf, wal)
	if err != nil {
		return err
	}

	roles, err := buildRoles(conf, stateStore, wal, recovery.NextHeight)
	if err != nil {
		return err
	}

	srv, err := server.New(conf, roles.cohort, roles.coordinator, stateStore)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	srv.Run(server.WhiteListChecker)
	<-ctx
	srv.Stop()

	return nil
}

func newWAL() (*gowal.Wal, error) {
	walConfig := gowal.Config{
		Dir:              config.DefaultWalDir,
		Prefix:           config.DefaultWalSegmentPrefix,
		SegmentThreshold: config.DefaultWalSegmentThreshold,
		MaxSegments:      config.DefaultWalMaxSegments,
		IsInSyncDiskMode: config.DefaultWalIsInSyncDiskMode,
	}

	w, err := gowal.NewWAL(walConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	return w, nil
}

func newStore(conf *config.Config, wal *gowal.Wal) (*store.Store, *store.RecoveryState, error) {
	stateStore, recovery, err := store.New(wal, conf.DBPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize state store: %w", err)
	}

	log.Printf("Recovered state from WAL: next height %d, keys %d\n", recovery.NextHeight, stateStore.Size())
	return stateStore, recovery, nil
}

type roleComponents struct {
	cohort      server.Cohort
	coordinator server.Coordinator
}

func buildRoles(conf *config.Config, stateStore *store.Store, wal *gowal.Wal, initialHeight uint64) (*roleComponents, error) {
	rc := &roleComponents{}
	switch conf.Role {
	case "cohort":
		committer := commitalgo.NewCommitter(stateStore, conf.CommitType, wal, conf.Timeout)
		committer.SetHeight(initialHeight)
		rc.cohort = cohort.NewCohort(committer, cohort.Mode(conf.CommitType))
	case "coordinator":
		coord, err := coordinator.New(conf, wal, stateStore)
		if err != nil {
			return nil, fmt.Errorf("failed to create coordinator: %w", err)
		}
		coord.SetHeight(initialHeight)
		rc.coordinator = coord
	default:
		return nil, fmt.Errorf("unsupported role %q, expected coordinator or cohort", conf.Role)
	}

	return rc, nil
}
