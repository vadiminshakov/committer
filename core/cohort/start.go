package cohort

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/vadiminshakov/committer/v2/core/dto"
	"github.com/vadiminshakov/committer/v2/events"
	"github.com/vadiminshakov/committer/v2/io/gateway/grpc/client"
	"github.com/vadiminshakov/committer/v2/io/gateway/grpc/server"
	iowal "github.com/vadiminshakov/committer/v2/io/wal"
)

// DefaultTimeout is used when Config leaves Timeout zero.
const DefaultTimeout = time.Second

// Config configures a cohort started with Start.
type Config struct {
	// Addr is the gRPC listen address, host:port.
	Addr dto.Addr
	// Coordinator is the coordinator's address, host:port: the only allowed
	// caller and the source of decisions for an in-doubt cohort.
	Coordinator dto.Addr
	// Protocol must match the coordinator's. Zero means two-phase commit.
	Protocol dto.Protocol
	// Timeout is the 3PC autocommit delay and the in-doubt retry interval.
	// Zero means DefaultTimeout.
	Timeout time.Duration
	// DataDir holds the WAL in <DataDir>/wal/cohort/<Addr>; reuse it across
	// restarts. Empty means ".data".
	DataDir string
	// Emitter optionally receives protocol events.
	Emitter events.Emitter
}

// Start recovers a cohort from its WAL and serves the coordinator. Recovery
// replays the last decision on resource and aborts a Prepare that crashed
// before reaching the WAL.
func Start(ctx context.Context, cfg Config, resource Resource) (*Cohort, error) {
	if resource == nil {
		return nil, errors.New("resource is nil")
	}

	timeout, err := cfg.validate()
	if err != nil {
		return nil, err
	}

	journal, err := iowal.Open(iowal.Dir(cfg.DataDir, "cohort", cfg.Addr.String()))
	if err != nil {
		return nil, err //nolint:wrapcheck // already names the WAL
	}

	recovery, err := journal.Recover(nil)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("recover WAL: %w", err), journal.Close())
	}

	coordinatorClient, err := client.NewCoordinatorClient(cfg.Coordinator.String())
	if err != nil {
		return nil, errors.Join(err, journal.Close())
	}

	release := func() error { return errors.Join(coordinatorClient.Close(), journal.Close()) }

	cohort := newCohort(resource, cfg.Protocol.String(), journal, uint64(timeout.Milliseconds()))
	cohort.SetEmitter(cfg.Emitter)
	cohort.SetDecisionRequester(coordinatorClient)

	if err := cohort.Resume(ctx, recovery); err != nil {
		return nil, errors.Join(fmt.Errorf("resume cohort: %w", err), release())
	}

	slog.Info("Recovered cohort from WAL", "next_height", cohort.Height())

	srv := server.New(cfg.Addr.String(), cfg.Coordinator.String(), cohort, nil)
	if err := srv.Run(); err != nil {
		return nil, errors.Join(fmt.Errorf("serve: %w", err), release())
	}

	cohort.shutdown = sync.OnceValue(func() error {
		srv.Stop()

		return release()
	})

	return cohort, nil
}

// Close stops the server and closes the coordinator client and WAL.
// The caller remains responsible for the Resource.
func (c *Cohort) Close() error {
	if c.shutdown == nil {
		return nil
	}

	return c.shutdown()
}

// validate checks cfg and returns the effective timeout.
func (cfg Config) validate() (time.Duration, error) {
	if cfg.Protocol != 0 && cfg.Protocol != dto.ProtocolTwoPhase && cfg.Protocol != dto.ProtocolThreePhase {
		return 0, fmt.Errorf("unknown protocol %d", cfg.Protocol)
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	if timeout < time.Millisecond {
		return 0, fmt.Errorf("timeout %s is shorter than 1ms", timeout)
	}

	if err := cfg.Addr.Validate(); err != nil {
		return 0, fmt.Errorf("cohort address: %w", err)
	}

	if err := cfg.Coordinator.Validate(); err != nil {
		return 0, fmt.Errorf("coordinator address: %w", err)
	}

	return timeout, nil
}
