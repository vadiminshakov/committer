// Package main provides a distributed consensus system implementing Two-Phase Commit (2PC)
// and Three-Phase Commit (3PC) protocols for distributed transactions.
//
// Committer is a Go implementation of distributed atomic commit protocols that allows
// you to achieve data consistency in distributed systems using Two-Phase Commit (2PC)
// and Three-Phase Commit (3PC) protocols for distributed transactions.
// The system consists of coordinators that manage transactions and cohorts that
// participate in the consensus process.
//
// Usage:
//
//	# Start coordinator (presence of -cohorts implies coordinator role)
//	./committer -nodeaddr=localhost:3000 -cohorts=localhost:3001,localhost:3002
//
//	# Start cohort (no -cohorts implies cohort role)
//	./committer -coordinator=localhost:3000 -nodeaddr=localhost:3001
package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/cohort"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo"
	"github.com/vadiminshakov/committer/core/coordinator"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/committer/io/store"
	"github.com/vadiminshakov/committer/io/wal"
	"github.com/vadiminshakov/gowal"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	if err := run(); err != nil {
		slog.Error("committer failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := make(chan os.Signal, 1)
	signal.Notify(ctx, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	conf := config.Get()

	wal, err := newWAL(conf)
	if err != nil {
		return err
	}
	defer wal.Close()

	stateStore, recovery, err := newStore(wal, conf)
	if err != nil {
		return err
	}

	roles, err := buildRoles(conf, stateStore, wal, recovery)
	if err != nil {
		return err
	}

	srv, err := server.New(conf, roles.cohort, roles.coordinator, stateStore)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	srv.Run(server.CoordinatorCheck)
	<-ctx
	srv.Stop()

	return nil
}

func newWAL(conf *config.Config) (*wal.Wal, error) {
	walConfig := gowal.Config{
		Dir:              config.WalDir(conf.Role),
		Prefix:           config.DefaultWalSegmentPrefix,
		SegmentThreshold: config.DefaultWalSegmentThreshold,
		MaxSegments:      config.DefaultWalMaxSegments,
		IsInSyncDiskMode: config.DefaultWalIsInSyncDiskMode,
	}

	w, err := gowal.NewWAL(walConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	return wal.New(w), nil
}

func newStore(w *wal.Wal, conf *config.Config) (*store.Store, *wal.RecoveryState, error) {
	stateStore, recovery, err := store.New(w, config.DBPath(conf.Role))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize state store: %w", err)
	}

	slog.Info("Recovered state from WAL", "next_height", recovery.Height, "keys", stateStore.Size())
	return stateStore, recovery, nil
}

type roleComponents struct {
	cohort      server.Cohort
	coordinator server.Coordinator
}

func buildRoles(conf *config.Config, stateStore *store.Store, w *wal.Wal, recovery *wal.RecoveryState) (*roleComponents, error) {
	rc := &roleComponents{}
	switch conf.Role {
	case "cohort":
		committer := commitalgo.NewCommitter(stateStore, conf.CommitType, w, conf.Timeout)
		committer.SetHeight(recovery.Height)
		committer.SetPendingPayload(recovery.PendingPayload)
		rc.cohort = cohort.NewCohort(committer, cohort.Mode(conf.CommitType))
	case "coordinator":
		coord, err := coordinator.New(conf, w, stateStore)
		if err != nil {
			return nil, fmt.Errorf("failed to create coordinator: %w", err)
		}
		coord.SetHeight(recovery.Height)
		rc.coordinator = coord
	default:
		return nil, fmt.Errorf("unsupported role %q, expected coordinator or cohort", conf.Role)
	}

	return rc, nil
}
