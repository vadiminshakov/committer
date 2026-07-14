// Package commitalgo implements cohort-side 2PC and 3PC.
package commitalgo

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/events"
	iowal "github.com/vadiminshakov/committer/io/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wal persists protocol records.
type wal interface {
	Write(key string, value []byte) error
	Close() error
}

// StateStore persists committed values.
//
//go:generate mockgen -destination=../../../mocks/mock_commitalgo_state_store.go -package=mocks -mock_names=StateStore=MockCommitalgoStateStore . StateStore
type StateStore interface {
	Put(key string, value []byte) error
	Close() error
}

// DecisionRequester asks the coordinator for the recorded outcome of a height.
//
//go:generate mockgen -destination=../../../mocks/mock_decision_requester.go -package=mocks -mock_names=DecisionRequester=MockDecisionRequester . DecisionRequester
type DecisionRequester interface {
	Decision(ctx context.Context, height uint64) (dto.Outcome, error)
}

// CommitterImpl runs the cohort commit state machine.
type CommitterImpl struct {
	store          StateStore
	wal            wal
	hookRegistry   *hooks.Registry
	state          *stateMachine
	height         atomic.Uint64
	timeout        uint64
	mu             sync.Mutex
	pendingPayload []byte            // encoded payload of the current transaction
	decisions      map[uint64]string // final outcome of each resolved height
	coordClient    DecisionRequester // coordinator used for decision requests
	emitter        events.Emitter
}

// NewCommitter creates a committer.
func NewCommitter(store StateStore, commitType string, wal wal, timeout uint64, customHooks ...hooks.Hook) *CommitterImpl {
	registry := hooks.NewRegistry()

	for _, hook := range customHooks {
		registry.Register(hook)
	}

	if len(customHooks) == 0 {
		registry.Register(hooks.NewDefaultHook())
	}

	return &CommitterImpl{
		hookRegistry: registry,
		store:        store,
		wal:          wal,
		timeout:      timeout,
		state:        newStateMachine(mode(commitType)),
		decisions:    make(map[uint64]string),
		emitter:      events.NoopEmitter{},
	}
}

// SetEmitter sets the event emitter. A nil emitter disables events.
func (c *CommitterImpl) SetEmitter(e events.Emitter) {
	if e == nil {
		e = events.NoopEmitter{}
	}
	c.emitter = e
}

// SetDecisionRequester sets the coordinator used by the termination protocol.
// Call it before serving requests.
func (c *CommitterImpl) SetDecisionRequester(dr DecisionRequester) {
	c.coordClient = dr
}

// RegisterHook adds a hook.
func (c *CommitterImpl) RegisterHook(hook hooks.Hook) {
	c.hookRegistry.Register(hook)
}

// Height returns the current transaction height.
func (c *CommitterImpl) Height() uint64 {
	return c.height.Load()
}

// SetHeight initializes committer height from recovered WAL state.
func (c *CommitterImpl) SetHeight(height uint64) {
	c.height.Store(height)
}

// Propose handles the propose phase.
func (c *CommitterImpl) Propose(ctx context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := c.height.Load()
	if req.Height != currentHeight {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	payload, err := iowal.Encode(iowal.Tx{Key: req.Key, Value: req.Value})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode error: %v", err)
	}

	// duplicate proposals are idempotent only when their payload matches.
	if st := c.state.getCurrentState(); st != proposeStage {
		if bytes.Equal(payload, c.pendingPayload) {
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
		}

		slog.Warn("rejecting conflicting proposal for occupied height", "height", req.Height, "state", st)
		// NACK prevents retries from replacing the payload at this height.
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	if !c.hookRegistry.ExecutePropose(req) {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: req.Height}, nil
	}

	c.emitter.Emit(events.Event{Kind: events.EvCohortPropose, Height: req.Height, Key: req.Key})

	if err := c.wal.Write(iowal.PreparedKey(req.Height), payload); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write wal on index %d: %v", req.Height, err)
	}
	c.pendingPayload = payload

	if err := c.enterPrepared(req.Height); err != nil {
		return nil, status.Errorf(codes.Internal, "state error: %v", err)
	}

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
}

// Precommit handles the 3PC precommit phase.
func (c *CommitterImpl) Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := c.height.Load()
	if index != currentHeight {
		return nil, status.Errorf(codes.FailedPrecondition, "invalid precommit height: expected %d, got %d", currentHeight, index)
	}

	currentState := c.state.getCurrentState()
	if currentState == precommitStage && c.pendingPayload != nil {
		// repeat the ACK without rewriting the durable PRECOMMIT record.
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: index}, nil
	}

	if currentState != preparedStage {
		return nil, status.Errorf(codes.FailedPrecondition, "precommit allowed only from prepared/waiting state, current: %s", currentState)
	}

	if c.pendingPayload == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no prepared record for height %d", index)
	}

	if err := c.wal.Write(iowal.PrecommitKey(index), c.pendingPayload); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write precommit: %v", err)
	}

	if err := c.state.Transition(precommitStage); err != nil {
		return nil, status.Errorf(codes.Internal, "state error: %v", err)
	}
	c.emitter.Emit(events.Event{Kind: events.EvCohortPrecommit, Height: index})

	go c.handlePrecommitTimeout(index)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: index}, nil
}

// Commit validates and serializes an external commit request.
func (c *CommitterImpl) Commit(ctx context.Context, req *dto.CommitRequest) (*dto.CohortResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "commit request is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.commit(req)
}

func (c *CommitterImpl) commit(req *dto.CommitRequest) (*dto.CohortResponse, error) {
	height := req.Height
	currentHeight := c.height.Load()

	if height < currentHeight {
		if c.decisions[height] == iowal.PhaseKeyCommit {
			slog.Debug("commit already applied", "height", height, "current_height", currentHeight)
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: height}, nil
		}
		slog.Warn("rejecting commit for height resolved as abort", "height", height)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	if height > currentHeight {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	currentState := c.state.getCurrentState()
	expectedState := c.getExpectedCommitState()
	if currentState != expectedState && currentState != commitStage {
		return nil, status.Errorf(codes.FailedPrecondition,
			"invalid state for commit: expected %s for %s mode, but current state is %s",
			expectedState, c.state.GetMode(), currentState)
	}

	if !c.hookRegistry.ExecuteCommit(req) {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, nil
	}

	if currentState != commitStage {
		if err := c.state.Transition(commitStage); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "invalid state transition to commit: %v", err)
		}
	}
	c.emitter.Emit(events.Event{Kind: events.EvCohortCommit, Height: height})

	if c.pendingPayload == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no pending payload")
	}

	walTx, err := iowal.Decode(c.pendingPayload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode error: %v", err)
	}

	if err := c.wal.Write(iowal.CommitKey(height), c.pendingPayload); err != nil {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, err
	}

	if err := c.store.Put(walTx.Key, walTx.Value); err != nil {
		slog.Error("CRITICAL: failed to apply committed tx to store, height stuck until restart; "+
			"restart this cohort to reapply the commit from WAL", "height", height, "err", err)
		return nil, err
	}

	c.decisions[height] = iowal.PhaseKeyCommit
	c.height.Store(currentHeight + 1)
	c.resetPending()

	if terr := c.state.Transition(proposeStage); terr != nil {
		slog.Error("failed to transition back to propose state after successful commit", "err", terr)
	}
	c.emitter.Emit(events.Event{Kind: events.EvCohortCommit, Height: currentHeight, Result: "ok"})

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
}

// Abort handles coordinator abort requests.
func (c *CommitterImpl) Abort(ctx context.Context, req *dto.AbortRequest) (*dto.CohortResponse, error) {
	slog.Warn("received abort request", "height", req.Height, "reason", req.Reason)

	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := c.height.Load()

	if req.Height != currentHeight {
		slog.Debug("ignoring abort for non-current height", "height", req.Height, "current", currentHeight)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: currentHeight}, nil
	}

	slog.Info("processing abort for current height", "height", req.Height)

	if err := c.abortCurrent(req.Height, req.Reason); err != nil {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, err
	}

	slog.Info("successfully processed abort", "height", req.Height)
	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: c.height.Load()}, nil
}

func (c *CommitterImpl) abortCurrent(height uint64, reason string) error {
	currentState := c.state.getCurrentState()
	if c.state.GetMode() == threephase && (currentState == precommitStage || currentState == commitStage) {
		return status.Errorf(codes.FailedPrecondition,
			"cannot abort 3PC transaction after precommit, current state: %s", currentState)
	}
	if currentState == commitStage {
		return status.Errorf(codes.FailedPrecondition, "cannot abort transaction while commit is in progress")
	}

	if err := c.wal.Write(iowal.AbortKey(height), nil); err != nil {
		slog.Error("failed to write abort record", "height", height, "err", err)
		return err
	}

	c.decisions[height] = iowal.PhaseKeyAbort
	c.height.Store(height + 1)
	c.resetPending()

	c.emitter.Emit(events.Event{Kind: events.EvCohortAbort, Height: height, Message: reason})
	c.resetToPropose(height, reason)

	return nil
}

func (c *CommitterImpl) resetToPropose(height uint64, reason string) {
	slog.Debug("resetting state to propose", "height", height, "reason", reason)

	currentState := c.state.getCurrentState()

	if c.state.GetMode() == threephase && currentState == precommitStage {
		// The 3PC state machine reaches propose through commit.
		if err := c.state.Transition(commitStage); err != nil {
			slog.Error("failed to transition to commit state during reset", "height", height, "err", err)
			return
		}
	}

	if err := c.state.Transition(proposeStage); err != nil {
		slog.Error("failed to reset to propose state", "height", height, "err", err, "current", c.state.getCurrentState())
	}
}

func (c *CommitterImpl) getExpectedCommitState() string {
	if c.state.GetMode() == twophase {
		return preparedStage
	}
	return precommitStage
}

func (c *CommitterImpl) handlePrecommitTimeout(height uint64) {
	timer := time.NewTimer(time.Duration(c.timeout) * time.Millisecond)
	defer timer.Stop()
	<-timer.C

	c.mu.Lock()
	defer c.mu.Unlock()

	currentState := c.state.getCurrentState()
	currentHeight := c.height.Load()

	slog.Debug("precommit timeout handler", "state", currentState, "height", currentHeight, "index", height)

	if currentState != precommitStage || currentHeight != height {
		slog.Debug("skipping autocommit", "height", height, "state", currentState, "current_height", currentHeight)
		return
	}

	if c.pendingPayload == nil {
		slog.Error("no pending payload during precommit timeout", "height", height)
		return
	}

	slog.Warn("performing autocommit after precommit timeout", "height", height)

	response, err := c.commit(&dto.CommitRequest{Height: height})
	if err != nil {
		slog.Error("autocommit failed", "height", height, "err", err)
		return
	}

	if response != nil && response.ResponseType == dto.ResponseTypeNack {
		slog.Warn("autocommit returned NACK", "height", height)
		return
	}

	slog.Info("successfully autocommitted after precommit timeout", "height", height)
}

// awaitDecision polls the coordinator while the height remains PREPARED.
func (c *CommitterImpl) awaitDecision(height uint64) {
	if c.coordClient == nil {
		return
	}

	interval := time.Duration(c.timeout) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if !c.stillPrepared(height) {
			return
		}

		reqCtx, cancel := context.WithTimeout(context.Background(), interval)
		outcome, err := c.coordClient.Decision(reqCtx, height)
		cancel()
		if err != nil {
			slog.Warn("decision request failed, will retry", "height", height, "err", err)
			continue
		}

		if outcome == dto.OutcomeUnknown {
			continue
		}
		if c.applyDecision(height, outcome) {
			return
		}
	}
}

func (c *CommitterImpl) stillPrepared(height uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.getCurrentState() == preparedStage && c.height.Load() == height
}

// applyDecision applies the outcome and reports whether the height is resolved.
func (c *CommitterImpl) applyDecision(height uint64, outcome dto.Outcome) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state.getCurrentState() != preparedStage || c.height.Load() != height {
		return true
	}

	slog.Info("applying coordinator decision", "height", height, "outcome", outcome)

	switch outcome {
	case dto.OutcomeCommit:
		if _, err := c.commit(&dto.CommitRequest{Height: height}); err != nil {
			slog.Error("failed to apply commit decision", "height", height, "err", err)
			return false
		}
	case dto.OutcomeAbort:
		if err := c.abortCurrent(height, "coordinator decision"); err != nil {
			slog.Error("failed to apply abort decision", "height", height, "err", err)
			return false
		}
	default:
		return false
	}

	return true
}

// enterPrepared enters PREPARED and starts decision polling.
func (c *CommitterImpl) enterPrepared(height uint64) error {
	if err := c.state.Transition(preparedStage); err != nil {
		return err
	}
	go c.awaitDecision(height)
	return nil
}

// Resume restores WAL state and resumes termination handling.
func (c *CommitterImpl) Resume(rec *iowal.RecoveryState) {
	if rec.Decisions != nil {
		c.decisions = rec.Decisions
	}

	if rec.Unresolved == nil {
		c.height.Store(rec.NextHeight)
		return
	}

	tx := rec.Unresolved
	c.height.Store(tx.Height)
	c.pendingPayload = tx.Payload

	if c.state.mode == twophase {
		c.resumeTwoPhaseInDoubt(tx)
		return
	}

	switch tx.Phase {
	case iowal.PhaseKeyPrepared:
		c.resumeThreePhasePrepared(tx)
	case iowal.PhaseKeyPrecommit:
		c.resumeThreePhasePrecommit(tx)
	default:
		slog.Error("cannot resume transaction with unexpected WAL phase", "height", tx.Height, "phase", tx.Phase)
	}
}

// resumeTwoPhaseInDoubt restores PREPARED and awaits the coordinator decision.
func (c *CommitterImpl) resumeTwoPhaseInDoubt(tx *iowal.UnresolvedTransaction) {
	if err := c.enterPrepared(tx.Height); err != nil {
		slog.Error("failed to restore prepared state", "err", err)
		return
	}
	slog.Warn("recovered in-doubt transaction, awaiting coordinator decision", "height", tx.Height)
}

// resumeThreePhasePrepared restores PREPARED and awaits the coordinator decision.
func (c *CommitterImpl) resumeThreePhasePrepared(tx *iowal.UnresolvedTransaction) {
	if err := c.enterPrepared(tx.Height); err != nil {
		slog.Error("failed to restore prepared state", "err", err)
		return
	}
	slog.Warn("recovered prepared 3PC transaction, awaiting coordinator decision", "height", tx.Height)
}

// resumeThreePhasePrecommit restores PRECOMMIT and resumes autocommit.
func (c *CommitterImpl) resumeThreePhasePrecommit(tx *iowal.UnresolvedTransaction) {
	if err := c.state.Transition(preparedStage); err != nil {
		slog.Error("failed to restore prepared state before precommit", "err", err)
		return
	}
	if err := c.state.Transition(precommitStage); err != nil {
		slog.Error("failed to restore precommit state", "err", err)
		return
	}
	slog.Warn("recovered precommitted 3PC transaction, resuming commit timeout", "height", tx.Height)
	go c.handlePrecommitTimeout(tx.Height)
}

func (c *CommitterImpl) resetPending() {
	c.pendingPayload = nil
}

func (c *CommitterImpl) getCurrentState() string {
	return c.state.getCurrentState()
}
