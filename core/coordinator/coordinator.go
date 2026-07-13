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

	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/events"
	iowal "github.com/vadiminshakov/committer/io/wal"
)

var (
	ErrProposeVote   = errors.New("failed to send propose")
	ErrPrecommitVote = errors.New("failed to send precommit")
)

//go:generate mockgen -destination=../../mocks/mock_coordinator.go -package=mocks -mock_names=wal=MockCoordinatorWAL,stateStore=MockCoordinatorStateStore,Cohort=MockCoordinatorCohort . wal,stateStore,Cohort
type wal interface {
	Write(key string, value []byte) error
	Recover(applyFn func(key string, value []byte) error) (*iowal.RecoveryState, error)
}

type stateStore interface {
	Put(key string, value []byte) error
}

type Coordinator struct {
	protocol  dto.Protocol
	lifecycle *transactionLifecycle
	delivery  *cohortDelivery
	emitter   events.Emitter

	mu sync.Mutex
}

func New(
	protocol dto.Protocol,
	wal wal,
	store stateStore,
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
		store,
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

// Broadcast runs one 2PC/3PC transaction. Voting is synchronous; delivery of a
// successful final decision starts asynchronously before returning.
func (c *Coordinator) Broadcast(ctx context.Context, request dto.BroadcastRequest) (*dto.BroadcastResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	transaction := dto.Transaction{Key: request.Key, Value: request.Value}
	height, err := c.lifecycle.Prepare(transaction)
	if err != nil {
		return nackResponse(c.lifecycle.Height(), fmt.Errorf("prepare transaction: %w", err))
	}

	c.emitter.Emit(events.Event{
		Kind:   events.EvCoordPropose,
		Key:    request.Key,
		Height: height,
	})
	if err := c.delivery.VoteProposal(ctx, dto.Proposal{
		Height:      height,
		Protocol:    c.protocol,
		Transaction: transaction,
	}); err != nil {
		return c.abortTransaction(height, request.Key, err)
	}

	if c.protocol == dto.ProtocolThreePhase {
		if err := c.lifecycle.Precommit(); err != nil {
			return nackResponse(height, fmt.Errorf("persist precommit: %w", err))
		}
		c.emitter.Emit(events.Event{
			Kind:   events.EvCoordPrecommit,
			Key:    request.Key,
			Height: height,
		})
		if err := c.delivery.VotePrecommit(ctx, height); err != nil {
			return nackResponse(height, fmt.Errorf("%w: %w", ErrPrecommitVote, err))
		}
	}

	return c.commitTransaction(height, request.Key)
}

// commitTransaction makes COMMIT durable, applies it locally, publishes the
// outcome, and starts cohort delivery before acknowledging the request.
func (c *Coordinator) commitTransaction(height uint64, key string) (*dto.BroadcastResponse, error) {
	decision, err := c.lifecycle.Commit()
	if err != nil {
		// A CommittedNotAppliedError deliberately does not trigger cohort
		// delivery. The durable decision is visible through Decision, while
		// this coordinator remains fenced until restart/recovery applies it.
		return nackResponse(height, fmt.Errorf("failed to commit: %w", err))
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

	return &dto.BroadcastResponse{
		Type:   dto.ResponseTypeAck,
		Height: decision.Height,
	}, nil
}

// abortTransaction makes ABORT durable after a failed proposal vote, publishes
// the outcome, starts cohort delivery, and reports the original voting error.
func (c *Coordinator) abortTransaction(height uint64, key string, voteErr error) (*dto.BroadcastResponse, error) {
	decision, abortErr := c.lifecycle.Abort()
	if abortErr != nil {
		return nackResponse(height, fmt.Errorf(
			"failed to record abort after failed to send propose (%v): %w",
			voteErr,
			abortErr,
		))
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

	return nackResponse(height, fmt.Errorf("%w: %w", ErrProposeVote, voteErr))
}

// Height returns the protocol height at which the next ready transaction will run.
func (c *Coordinator) Height() uint64 {
	return c.lifecycle.Height()
}

// Decision returns the durable final outcome recorded for height.
func (c *Coordinator) Decision(height uint64) dto.Outcome {
	return c.lifecycle.Decision(height)
}

// Close stops cohort delivery and releases all cohort clients.
func (c *Coordinator) Close() error {
	c.delivery.cancel()

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.delivery.Close()
}

func nackResponse(height uint64, err error) (*dto.BroadcastResponse, error) {
	return &dto.BroadcastResponse{
		Type:   dto.ResponseTypeNack,
		Height: height,
	}, err
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
	sort.Slice(cohorts, func(i, j int) bool {
		if cohorts[i] == nil {
			return cohorts[j] != nil
		}
		if cohorts[j] == nil {
			return false
		}
		return cohorts[i].Addr() < cohorts[j].Addr()
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
