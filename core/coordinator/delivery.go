package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/vadiminshakov/committer/v2/internal/core/dto"
	"github.com/vadiminshakov/committer/v2/internal/events"
)

const (
	finalDecisionRetryBackoff = 100 * time.Millisecond
	deliveryMaxCatchUpRounds  = 3
)

// Cohort is the transport-neutral port used by cohort delivery. Addr
// identifies the cohort in membership, errors, and emitted events.
type Cohort interface {
	Addr() string
	Propose(context.Context, dto.Proposal) (dto.ParticipantReply, error)
	Precommit(context.Context, uint64) (dto.ParticipantReply, error)
	ApplyFinalDecision(context.Context, dto.FinalDecision) (dto.ParticipantReply, error)
	Close() error
}

type decisionLookup func(uint64) dto.Outcome

// cohortDelivery owns membership and transport-neutral protocol delivery.
// Voting fans out synchronously. Final decisions retry asynchronously, while
// a later proposal repairs any cohort lag through decision catch-up.
type cohortDelivery struct {
	ctx     context.Context
	cancel  context.CancelFunc
	cohorts []Cohort
	lookup  decisionLookup
	emitter events.Emitter

	tasksMu   sync.Mutex
	closed    bool
	tasks     sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func newCohortDelivery(
	cohorts []Cohort,
	lookup decisionLookup,
	emitter events.Emitter,
) *cohortDelivery {
	if emitter == nil {
		emitter = events.NoopEmitter{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	delivery := &cohortDelivery{
		ctx:     ctx,
		cancel:  cancel,
		lookup:  lookup,
		emitter: emitter,
		cohorts: append([]Cohort(nil), cohorts...),
	}
	sort.Slice(delivery.cohorts, func(i, j int) bool {
		return delivery.cohorts[i].Addr() < delivery.cohorts[j].Addr()
	})

	return delivery
}

// VoteProposal concurrently asks every cohort to accept the proposal and
// returns only after every reply has been normalized.
func (d *cohortDelivery) VoteProposal(ctx context.Context, proposal dto.Proposal) error {
	return d.fanOut(ctx, func(operationCtx context.Context, cohort Cohort) error {
		return d.voteProposal(operationCtx, cohort, cloneProposal(proposal))
	})
}

// VotePrecommit concurrently asks every cohort to enter the committable
// phase and waits for every normalized reply.
func (d *cohortDelivery) VotePrecommit(ctx context.Context, height uint64) error {
	return d.fanOut(ctx, func(operationCtx context.Context, cohort Cohort) error {
		reply, err := cohort.Precommit(operationCtx, height)
		if err != nil {
			d.emitVote(events.EvCoordPrecommit, cohort.Addr(), height, "nack")

			return fmt.Errorf("cohort %s precommit: %w", cohort.Addr(), err)
		}

		if !reply.Accepted {
			d.emitVote(events.EvCoordPrecommit, cohort.Addr(), height, "nack")

			return fmt.Errorf("cohort %s rejected precommit at height %d", cohort.Addr(), height)
		}

		d.emitVote(events.EvCoordPrecommit, cohort.Addr(), height, "ok")

		return nil
	})
}

// DeliverFinal starts asynchronous retry of a durable decision for every
// cohort. It deliberately does not serialize a later proposal behind the
// retry: proposal height negotiation performs catch-up when necessary.
func (d *cohortDelivery) DeliverFinal(decision dto.FinalDecision) error {
	if !isFinalOutcome(decision.Outcome) {
		return fmt.Errorf("cannot deliver non-final outcome %d at height %d", decision.Outcome, decision.Height)
	}

	if decision.RequirePrecommit && decision.Outcome != dto.OutcomeCommit {
		return fmt.Errorf("precommit is only valid for a commit decision at height %d", decision.Height)
	}

	if err := d.reserveTasks(len(d.cohorts)); err != nil {
		return err
	}

	for _, cohort := range d.cohorts {
		go func() {
			defer d.tasks.Done()

			_ = d.retryFinal(d.ctx, cohort, decision)
		}()
	}

	return nil
}

func (d *cohortDelivery) fanOut(
	ctx context.Context,
	operation func(context.Context, Cohort) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := d.reserveTasks(len(d.cohorts)); err != nil {
		return err
	}

	results := make(chan error, len(d.cohorts))
	for _, cohort := range d.cohorts {
		go func() {
			defer d.tasks.Done()

			operationCtx, cancel := d.operationContext(ctx)
			defer cancel()

			results <- operation(operationCtx, cohort)
		}()
	}

	var result error

	for range d.cohorts {
		select {
		case err := <-results:
			result = errors.Join(result, err)
		case <-ctx.Done():
			return errors.Join(result, ctx.Err())
		case <-d.ctx.Done():
			return errors.Join(result, d.ctx.Err())
		}
	}

	return result
}

// reserveTasks prevents WaitGroup.Add from racing with Close.Wait.
func (d *cohortDelivery) reserveTasks(count int) error {
	d.tasksMu.Lock()
	defer d.tasksMu.Unlock()

	if d.closed {
		return d.ctx.Err()
	}

	d.tasks.Add(count)

	return nil
}

func (d *cohortDelivery) voteProposal(ctx context.Context, cohort Cohort, proposal dto.Proposal) error {
	var (
		catchUpRounds int
		lastLagHeight uint64
		hasLagHeight  bool
	)

	for {
		reply, err := cohort.Propose(ctx, proposal)
		if err != nil {
			d.emitVote(events.EvCoordPropose, cohort.Addr(), proposal.Height, "nack")

			return fmt.Errorf("cohort %s proposal: %w", cohort.Addr(), err)
		}

		if reply.Accepted {
			d.emitVote(events.EvCoordPropose, cohort.Addr(), proposal.Height, "ok")

			return nil
		}

		d.emitVote(events.EvCoordPropose, cohort.Addr(), proposal.Height, "nack")

		if reply.Height >= proposal.Height {
			if reply.Reason != "" {
				return fmt.Errorf("cohort %s rejected proposal at height %d: %s",
					cohort.Addr(), proposal.Height, reply.Reason)
			}

			return fmt.Errorf("cohort %s rejected proposal at height %d", cohort.Addr(), proposal.Height)
		}

		if hasLagHeight && reply.Height <= lastLagHeight {
			return fmt.Errorf(
				"cohort %s did not advance during proposal catch-up: height %d",
				cohort.Addr(),
				reply.Height,
			)
		}

		if catchUpRounds >= deliveryMaxCatchUpRounds {
			return fmt.Errorf("cohort %s exceeded proposal catch-up limit at height %d", cohort.Addr(), reply.Height)
		}

		if err := d.catchUp(ctx, cohort, reply.Height, proposal.Height, proposal.Protocol); err != nil {
			return err
		}

		lastLagHeight = reply.Height
		hasLagHeight = true
		catchUpRounds++
	}
}

func (d *cohortDelivery) catchUp(
	ctx context.Context,
	cohort Cohort,
	from, to uint64,
	protocol dto.Protocol,
) error {
	for height := from; height < to; height++ {
		if d.lookup == nil {
			return fmt.Errorf("cohort %s catch-up stopped: no final decision at height %d", cohort.Addr(), height)
		}

		outcome := d.lookup(height)
		if !isFinalOutcome(outcome) {
			return fmt.Errorf("cohort %s catch-up stopped: no final decision at height %d", cohort.Addr(), height)
		}

		decision := dto.FinalDecision{
			Height:           height,
			Outcome:          outcome,
			RequirePrecommit: protocol == dto.ProtocolThreePhase && outcome == dto.OutcomeCommit,
		}

		if err := d.retryFinal(ctx, cohort, decision); err != nil {
			return fmt.Errorf("cohort %s catch-up at height %d: %w", cohort.Addr(), height, err)
		}
	}

	return nil
}

func (d *cohortDelivery) retryFinal(ctx context.Context, cohort Cohort, decision dto.FinalDecision) error {
	// A final-decision NACK cannot revise the durable outcome. Retry until ACK
	// or shutdown; a concurrent later proposal independently negotiates height
	// and performs catch-up if this cohort is still behind.
	for {
		if decision.RequirePrecommit {
			reply, err := cohort.Precommit(ctx, decision.Height)
			if err == nil && reply.Accepted {
				d.emitVote(events.EvCoordPrecommit, cohort.Addr(), decision.Height, "ok")
			} else {
				// The cohort may already have committed (for example via
				// its 3PC timeout), in which case PRECOMMIT is expected to fail
				// but the idempotent COMMIT below will ACK. Otherwise the next
				// retry preserves PREPARED -> PRECOMMIT -> COMMIT ordering.
				d.emitVote(events.EvCoordPrecommit, cohort.Addr(), decision.Height, "nack")
			}
		}

		reply, err := cohort.ApplyFinalDecision(ctx, decision)
		if err == nil && reply.Accepted {
			d.emitFinal(cohort.Addr(), decision, "ok")

			return nil
		}

		d.emitFinal(cohort.Addr(), decision, "nack")

		timer := time.NewTimer(finalDecisionRetryBackoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()

			return ctx.Err()
		}
	}
}

func (d *cohortDelivery) emitVote(kind events.EventKind, cohort string, height uint64, result string) {
	d.emitter.Emit(events.Event{
		Kind:   kind,
		Cohort: cohort,
		Height: height,
		Result: result,
	})
}

func (d *cohortDelivery) emitFinal(cohort string, decision dto.FinalDecision, result string) {
	kind := events.EvCoordAbort
	if decision.Outcome == dto.OutcomeCommit {
		kind = events.EvCoordCommit
	}

	d.emitter.Emit(events.Event{
		Kind:   kind,
		Cohort: cohort,
		Height: decision.Height,
		Result: result,
	})
}

func isFinalOutcome(outcome dto.Outcome) bool {
	return outcome == dto.OutcomeCommit || outcome == dto.OutcomeAbort
}

func (d *cohortDelivery) operationContext(request context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(request)
	stop := context.AfterFunc(d.ctx, cancel)

	return ctx, func() {
		stop()
		cancel()
	}
}

func cloneProposal(proposal dto.Proposal) dto.Proposal {
	proposal.Transaction.Value = append([]byte(nil), proposal.Transaction.Value...)

	return proposal
}

// Close cancels all delivery tasks before closing cohort adapters.
// Repeated calls return the first shutdown result without closing twice.
func (d *cohortDelivery) Close() error {
	d.closeOnce.Do(func() {
		d.tasksMu.Lock()
		d.closed = true
		d.cancel()
		d.tasksMu.Unlock()
		d.tasks.Wait()

		for _, cohort := range d.cohorts {
			if err := cohort.Close(); err != nil {
				d.closeErr = errors.Join(d.closeErr, fmt.Errorf("close cohort %s: %w", cohort.Addr(), err))
			}
		}
	})

	return d.closeErr
}
