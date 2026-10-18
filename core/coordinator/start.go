package coordinator

import (
	"errors"
	"fmt"
	"sync"

	"github.com/vadiminshakov/committer/v2/core/dto"
	"github.com/vadiminshakov/committer/v2/events"
	"github.com/vadiminshakov/committer/v2/io/gateway/grpc/client"
	"github.com/vadiminshakov/committer/v2/io/gateway/grpc/server"
	iowal "github.com/vadiminshakov/committer/v2/io/wal"
)

// Config configures a coordinator started with Start.
type Config struct {
	// Addr is the gRPC listen address, host:port. Cohorts ask it for
	// decisions, and remote clients may send transactions here.
	Addr dto.Addr
	// Cohorts lists every cohort's address, host:port. A transaction commits
	// only if all of them vote YES.
	Cohorts []dto.Addr
	// Protocol must match every cohort's. Zero means two-phase commit.
	Protocol dto.Protocol
	// DataDir is the root for the WAL, kept in <DataDir>/wal/coordinator/<Addr>.
	// Reuse it across restarts. Empty means ".data".
	DataDir string
	// Emitter optionally receives protocol events.
	Emitter events.Emitter
}

// Start recovers a coordinator from its WAL, connects to the cohorts and
// serves gRPC. A transaction left in progress by a crash is finished first:
// aborted if it was only prepared, committed if 3PC had already precommitted
// it.
func Start(cfg Config) (*Coordinator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	journal, err := iowal.Open(iowal.Dir(cfg.DataDir, "coordinator", cfg.Addr.String()))
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	cohorts, err := dialCohorts(cfg.Cohorts)
	if err != nil {
		return nil, errors.Join(err, journal.Close())
	}

	protocol := dto.ProtocolTwoPhase
	if cfg.Protocol == dto.ProtocolThreePhase {
		protocol = dto.ProtocolThreePhase
	}

	coord, err := newCoordinator(protocol, journal, cohorts, cfg.Emitter)
	if err != nil {
		return nil, errors.Join(err, journal.Close())
	}

	srv := server.New(cfg.Addr.String(), "", nil, coord)
	if err := srv.Run(); err != nil {
		return nil, errors.Join(fmt.Errorf("serve: %w", err), coord.Close(), journal.Close())
	}

	coord.shutdown = sync.OnceValue(func() error {
		srv.Stop()

		return journal.Close()
	})

	return coord, nil
}

func (cfg Config) validate() error {
	if cfg.Protocol != 0 && cfg.Protocol != dto.ProtocolTwoPhase && cfg.Protocol != dto.ProtocolThreePhase {
		return fmt.Errorf("unknown protocol %d", cfg.Protocol)
	}

	if err := cfg.Addr.Validate(); err != nil {
		return fmt.Errorf("coordinator address: %w", err)
	}

	if len(cfg.Cohorts) == 0 {
		return errors.New("at least one cohort is required")
	}

	for _, addr := range cfg.Cohorts {
		if err := addr.Validate(); err != nil {
			return fmt.Errorf("cohort address: %w", err)
		}
	}

	return nil
}

// dialCohorts connects to every cohort; duplicates are rejected.
func dialCohorts(addresses []dto.Addr) ([]Cohort, error) {
	cohorts := make([]Cohort, 0, len(addresses))
	seen := make(map[dto.Addr]struct{}, len(addresses))

	for _, addr := range addresses {
		if _, exists := seen[addr]; exists {
			return nil, errors.Join(fmt.Errorf("duplicate cohort address %q", addr), closeCohorts(cohorts))
		}

		cohortClient, err := client.NewCohortClient(addr.String())
		if err != nil {
			return nil, errors.Join(fmt.Errorf("connect to cohort %q: %w", addr, err), closeCohorts(cohorts))
		}

		seen[addr] = struct{}{}

		cohorts = append(cohorts, cohortClient)
	}

	return cohorts, nil
}
