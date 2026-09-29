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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/vadiminshakov/committer/v2/internal/config"
	"github.com/vadiminshakov/committer/v2/internal/events"
	"github.com/vadiminshakov/committer/v2/internal/io/store"
	"github.com/vadiminshakov/committer/v2/internal/node"
	"github.com/vadiminshakov/committer/v2/internal/viz"
)

func main() {
	if err := execute(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func startNode(conf *config.Config) error {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	slog.Info("Starting node", "role", conf.Role, "protocol", conf.CommitType,
		"addr", conf.Nodeaddr, "coordinator", conf.Coordinator, "cohorts", conf.Cohorts,
		"db", conf.DBPath(), "wal", conf.WalDir())

	if conf.VizPort > 0 {
		slog.Info("Protocol visualization", "url", fmt.Sprintf("http://localhost:%d", conf.VizPort))
	}

	var emitter events.Emitter = events.NoopEmitter{}
	if conf.VizPort > 0 {
		collector := viz.NewCollector(emitter)
		viz.NewServer(collector, conf, conf.VizPort).Start()
		emitter = collector
	}

	return run(conf, emitter)
}

func run(conf *config.Config, emitter events.Emitter) error {
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer signal.Stop(signals)

	stop, err := startKVNode(context.Background(), conf, emitter)
	if err != nil {
		return err
	}

	<-signals

	return stop()
}

// startKVNode starts a node whose state is a Badger key/value store: the
// resource of a cohort, and a readable copy of committed data on the
// coordinator. The returned function stops the node and closes the store.
func startKVNode(ctx context.Context, conf *config.Config, emitter events.Emitter) (func() error, error) {
	stateStore, err := store.Open(conf.DBPath())
	if err != nil {
		return nil, fmt.Errorf("open state store: %w", err)
	}

	params := node.Params{Config: conf, Reader: stateStore, Emitter: emitter}
	if conf.Role == config.RoleCohort {
		params.Resource = stateStore
	} else {
		params.LocalStore = stateStore
	}

	started, err := node.Start(ctx, params)
	if err != nil {
		return nil, errors.Join(err, stateStore.Close())
	}

	return func() error {
		return errors.Join(started.Close(), stateStore.Close())
	}, nil
}
