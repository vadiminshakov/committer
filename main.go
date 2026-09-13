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
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/events"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/committer/io/store"
	"github.com/vadiminshakov/committer/io/wal"
	"github.com/vadiminshakov/committer/viz"
	"github.com/vadiminshakov/gowal"
)

type roleComponents struct {
	cohort      server.Cohort
	coordinator server.Coordinator
	close       func() error
}

func (r *roleComponents) Close() error {
	if r == nil || r.close == nil {
		return nil
	}

	return r.close()
}

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
	ctx := make(chan os.Signal, 1)

	signal.Notify(ctx, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer signal.Stop(ctx)

	journal, err := newWAL(conf)
	if err != nil {
		return err
	}

	defer func() {
		if err := journal.Close(); err != nil {
			slog.Warn("failed to close WAL", "err", err)
		}
	}()

	stateStore, recovery, err := newStore(journal, conf)
	if err != nil {
		return err
	}

	serverOwnsStore := false
	defer func() {
		if !serverOwnsStore {
			if err := stateStore.Close(); err != nil {
				slog.Warn("failed to close state store after startup error", "err", err)
			}
		}
	}()

	roles, err := buildRoles(conf, stateStore, journal, recovery, emitter)
	if err != nil {
		return err
	}
	defer func() {
		if err := roles.Close(); err != nil {
			slog.Warn("failed to close role dependencies", "err", err)
		}
	}()

	srv, err := server.New(conf, roles.cohort, roles.coordinator, stateStore)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	serverOwnsStore = true

	srv.Run(server.CoordinatorCheck)
	<-ctx
	srv.Stop()

	return nil
}

func newWAL(conf *config.Config) (*wal.Wal, error) {
	walConfig := gowal.Config{
		Dir:              conf.WalDir(),
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

func newStore(journal *wal.Wal, conf *config.Config) (*store.Store, *wal.RecoveryState, error) {
	if conf.Role == config.RoleCoordinator {
		stateStore, err := store.Open(conf.DBPath())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize coordinator state store: %w", err)
		}

		slog.Info("Opened coordinator state store; transaction lifecycle will replay WAL", "keys", stateStore.Size())

		return stateStore, nil, nil
	}

	stateStore, recovery, err := store.New(journal, conf.DBPath())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize state store: %w", err)
	}

	slog.Info("Recovered state from WAL", "next_height", recovery.NextHeight, "keys", stateStore.Size())

	return stateStore, recovery, nil
}

func buildRoles(
	conf *config.Config,
	stateStore *store.Store,
	journal *wal.Wal,
	recovery *wal.RecoveryState,
	emitter events.Emitter,
) (*roleComponents, error) {
	roles := &roleComponents{}

	switch conf.Role {
	case config.RoleCohort:
		committer := commitalgo.NewCommitter(stateStore, conf.CommitType, journal, conf.Timeout)
		committer.SetEmitter(emitter)

		if conf.Coordinator != "" {
			coordinatorClient, err := client.NewCoordinatorClient(conf.Coordinator)
			if err != nil {
				slog.Warn("failed to create coordinator client, decision requests disabled", "err", err)
			} else {
				committer.SetDecisionRequester(coordinatorClient)
				roles.close = coordinatorClient.Close
			}
		}

		committer.Resume(recovery)
		roles.cohort = cohort.NewCohort(committer, cohort.Mode(conf.CommitType))
	case config.RoleCoordinator:
		coord, err := newReadyCoordinator(conf, journal, stateStore, emitter)
		if err != nil {
			return nil, fmt.Errorf("failed to create coordinator: %w", err)
		}

		roles.coordinator = coord
		roles.close = coord.Close
	default:
		return nil, fmt.Errorf("unsupported role %q, expected coordinator or cohort", conf.Role)
	}

	return roles, nil
}

func newReadyCoordinator(
	conf *config.Config,
	journal *wal.Wal,
	stateStore *store.Store,
	emitter events.Emitter,
) (*coordinator.Coordinator, error) {
	protocol, err := coordinatorProtocol(conf.CommitType)
	if err != nil {
		return nil, fmt.Errorf("resolve commit protocol: %w", err)
	}

	cohorts := make([]coordinator.Cohort, 0, len(conf.Cohorts))

	addresses := make(map[string]struct{}, len(conf.Cohorts))
	for _, address := range conf.Cohorts {
		if _, exists := addresses[address]; exists {
			for _, cohort := range cohorts {
				_ = cohort.Close()
			}

			return nil, fmt.Errorf("duplicate cohort address %q", address)
		}

		cohort, err := client.NewCohortClient(address)
		if err != nil {
			for _, opened := range cohorts {
				_ = opened.Close()
			}

			return nil, fmt.Errorf("create cohort client for %q: %w", address, err)
		}

		addresses[address] = struct{}{}

		cohorts = append(cohorts, cohort)
	}

	coord, err := coordinator.New(protocol, journal, stateStore, cohorts, emitter)
	if err != nil {
		return nil, fmt.Errorf("create coordinator: %w", err)
	}

	return coord, nil
}

func coordinatorProtocol(commitType string) (dto.Protocol, error) {
	switch commitType {
	case server.TWO_PHASE:
		return dto.ProtocolTwoPhase, nil
	case server.THREE_PHASE:
		return dto.ProtocolThreePhase, nil
	default:
		return 0, fmt.Errorf("unsupported coordinator commit type %q", commitType)
	}
}
