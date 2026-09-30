// Package coordinator implements the coordinator side of 2PC/3PC.
//
// A Coordinator orchestrates synchronous voting while delegating durable
// transaction semantics and ordered cohort delivery to deep internal modules.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/vadiminshakov/committer/v2/core/dto"
	"github.com/vadiminshakov/committer/v2/events"
	iowal "github.com/vadiminshakov/committer/v2/io/wal"
)

var (
	// ErrAborted means the transaction is durably aborted: no cohort applied
	// it, and the same change can be retried.
	ErrAborted = errors.New("transaction aborted")
	// ErrPrecommitVote means a 3PC cohort did not acknowledge PRECOMMIT; the
	// outcome is left to recovery.
	ErrPrecommitVote = errors.New("failed to send precommit")
	// ErrInvalidTransaction reports a request rejected before any durable
	// transaction record is written.
	ErrInvalidTransaction = errors.New("invalid transaction")
	// ErrCoordinatorNotReady reports that an unresolved or failed transaction
	// prevents the coordinator from accepting another transaction.
	ErrCoordinatorNotReady = errors.New("coordinator is not ready")
)

//go:generate mockgen -destination=../../mocks/mock_coordinator.go -package=mocks -mock_names=wal=MockCoordinatorWAL,Cohort=MockCoordinatorCohort . wal,Cohort
type wal interface {
	Write(key string, value []byte) error
	Recover(applyFn func(key string, value []byte) error) (*iowal.RecoveryState, error)
}

type Coordinator struct {
	protocol  dto.Protocol
	lifecycle *transactionLifecycle
	delivery  *cohortDelivery
	emitter   events.Emitter
	shutdown  func() error

	mu sync.Mutex
}

func newCoordinator(
	protocol dto.Protocol,
	wal wal,
	cohorts []Cohort,
	emitter events.Emitter,
) (*Coordinator, error) {
	if emitter == nil {
		emitter = events.NoopEmitter{}
	}

	if err := validateCohorts(cohorts); err != nil {
		return nil, errors.Join(err, closeCohorts(cohorts))
	}

	lifecycle, recovered, err := newTransactionLifecycle(
		protocol,
		wal,
	)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("construct transaction lifecycle: %w", err),
			closeCohorts(cohorts),
		)
	}

	coordinator := &Coordinator{
		protocol:  protocol,
		lifecycle: lifecycle,
		emitter:   emitter,
	}
	coordinator.delivery = newCohortDelivery(
		cohorts,
		lifecycle.Decision,
		emitter,
	)

	if recovered != nil {
		if err := coordinator.delivery.DeliverFinal(*recovered); err != nil {
			return nil, errors.Join(
				fmt.Errorf("schedule recovered final decision: %w", err),
				coordinator.delivery.Close(),
			)
		}
	}

	return coordinator, nil
}

// Commit runs one 2PC/3PC transaction and returns its height once COMMIT is
// durable. Voting is synchronous; cohorts receive the decision asynchronously.
func (c *Coordinator) Commit(ctx context.Context, key string, value []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	transaction := dto.Transaction{Key: key, Value: value}

	height, err := c.lifecycle.Prepare(transaction)
	if err != nil {
		return 0, fmt.Errorf("prepare transaction: %w", err)
	}

	c.emitter.Emit(events.Event{
		Kind:   events.EvCoordPropose,
		Key:    key,
		Height: height,
	})

	if err := c.delivery.VoteProposal(ctx, dto.Proposal{
		Height:      height,
		Protocol:    c.protocol,
		Transaction: transaction,
	}); err != nil {
		return c.abortTransaction(height, key, err)
	}

	if c.protocol == dto.ProtocolThreePhase {
		if err := c.lifecycle.Precommit(); err != nil {
			return height, fmt.Errorf("persist precommit: %w", err)
		}

		c.emitter.Emit(events.Event{
			Kind:   events.EvCoordPrecommit,
			Key:    key,
			Height: height,
		})

		if err := c.delivery.VotePrecommit(ctx, height); err != nil {
			return height, fmt.Errorf("%w: %w", ErrPrecommitVote, err)
		}
	}

	return c.commitTransaction(height, key)
}

// commitTransaction makes COMMIT durable, publishes the outcome, and starts
// cohort delivery before acknowledging the request.
func (c *Coordinator) commitTransaction(height uint64, key string) (uint64, error) {
	decision, err := c.lifecycle.Commit()
	if err != nil {
		return height, fmt.Errorf("failed to commit: %w", err)
	}

	c.emitter.Emit(events.Event{
		Kind:   events.EvCoordCommit,
		Key:    key,
		Height: decision.Height,
		Result: "ok",
	})

	if err := c.delivery.DeliverFinal(decision); err != nil {
		slog.Warn("failed to start final decision delivery",
			"height", decision.Height,
			"outcome", decision.Outcome,
			"err", err,
		)
	}

	return decision.Height, nil
}

// abortTransaction makes ABORT durable after a failed proposal vote, publishes
// the outcome, starts cohort delivery, and reports the original voting error.
func (c *Coordinator) abortTransaction(height uint64, key string, voteErr error) (uint64, error) {
	decision, abortErr := c.lifecycle.Abort()
	if abortErr != nil {
		return height, fmt.Errorf(
			"failed to record abort after failed to send propose (%w): %w",
			voteErr,
			abortErr,
		)
	}

	c.emitter.Emit(events.Event{
		Kind:    events.EvCoordAbort,
		Key:     key,
		Height:  decision.Height,
		Result:  "abort",
		Message: voteErr.Error(),
	})

	if err := c.delivery.DeliverFinal(decision); err != nil {
		// Delivery cannot revise a durable outcome or coordinator readiness.
		slog.Warn("failed to start final decision delivery",
			"height", decision.Height,
			"outcome", decision.Outcome,
			"err", err,
		)
	}

	return height, fmt.Errorf("%w: %w", ErrAborted, voteErr)
}

// Height returns the protocol height at which the next ready transaction will run.
func (c *Coordinator) Height() uint64 {
	return c.lifecycle.Height()
}

// Decision returns the durable final outcome recorded for height.
func (c *Coordinator) Decision(height uint64) dto.Outcome {
	return c.lifecycle.Decision(height)
}

// Close stops cohort delivery, releases all cohort clients, shuts down the
// server, and closes the WAL. Delivery of undelivered decisions resumes on
// the next start.
func (c *Coordinator) Close() error {
	c.delivery.cancel()

	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.delivery.Close()
	if c.shutdown != nil {
		err = errors.Join(err, c.shutdown())
	}

	return err
}

func validateCohorts(cohorts []Cohort) error {
	addresses := make(map[string]struct{}, len(cohorts))
	for _, cohort := range cohorts {
		if cohort == nil {
			return errors.New("cohort is nil")
		}

		address := cohort.Addr()
		if address == "" {
			return errors.New("cohort address is empty")
		}

		if _, exists := addresses[address]; exists {
			return fmt.Errorf("duplicate cohort address %q", address)
		}

		addresses[address] = struct{}{}
	}

	return nil
}

func closeCohorts(cohorts []Cohort) error {
	cohorts = append([]Cohort(nil), cohorts...)
	sort.Slice(cohorts, func(idx1, idx2 int) bool {
		if cohorts[idx1] == nil {
			return cohorts[idx2] != nil
		}

		if cohorts[idx2] == nil {
			return false
		}

		return cohorts[idx1].Addr() < cohorts[idx2].Addr()
	})

	var result error

	for _, cohort := range cohorts {
		if cohort == nil {
			continue
		}

		if err := cohort.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close cohort %s: %w", cohort.Addr(), err))
		}
	}

	return result
}
