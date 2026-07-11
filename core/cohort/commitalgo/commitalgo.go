// Package commitalgo implements the 2PC/3PC commit state machine for cohort nodes.
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

// wal defines the interface for write-ahead log operations.
type wal interface {
	Write(key string, value []byte) error
	Close() error
}

// StateStore defines the interface for state storage.
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

// CommitterImpl implements the commit algorithm with state machine and hooks.
type CommitterImpl struct {
	store          StateStore
	wal            wal
	hookRegistry   *hooks.Registry
	state          *stateMachine
	height         atomic.Uint64
	timeout        uint64
	mu             sync.Mutex
	pendingPayload []byte            // encoded payload of the current in-progress transaction
	decisions      map[uint64]string // final outcome (commit/abort) of every resolved height
	coordClient    DecisionRequester // coordinator link for decision requests
	emitter        events.Emitter
}

// NewCommitter creates a new committer instance with the specified configuration.
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

// SetEmitter injects an event emitter for dashboard visualization. Safe to call after NewCommitter().
func (c *CommitterImpl) SetEmitter(e events.Emitter) {
	if e == nil {
		e = events.NoopEmitter{}
	}
	c.emitter = e
}

// SetDecisionRequester injects the coordinator client for the termination
// protocol. Call before the node starts serving requests.
func (c *CommitterImpl) SetDecisionRequester(dr DecisionRequester) {
	c.coordClient = dr
}

// RegisterHook adds a new hook to the committer.
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

// Propose handles the propose phase of the commit protocol.
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

	// re-delivered proposal branch
	if c.state.getCurrentState() == preparedStage {
		// repeat the YES vote for the identical payload
		if bytes.Equal(payload, c.pendingPayload) {
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
		}

		slog.Warn("rejecting conflicting proposal for prepared height", "height", req.Height)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	if st := c.state.getCurrentState(); st != proposeStage {
		return nil, status.Errorf(codes.FailedPrecondition, "propose not allowed in state %s", st)
	}

	if !c.hookRegistry.ExecutePropose(req) {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: req.Height}, nil
	}

	c.emitter.Emit(events.Event{Kind: events.EvCohortPropose, Height: req.Height, Key: req.Key})

	if err := c.wal.Write(iowal.PreparedKey(req.Height), payload); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write wal on index %d: %v", req.Height, err)
	}
	c.pendingPayload = payload

	if c.state.mode == twophase {
		if err := c.enterPrepared(req.Height); err != nil {
			return nil, status.Errorf(codes.Internal, "state error: %v", err)
		}

		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
	}

	go c.handleProposeTimeout(req.Height)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
}

// Precommit handles the precommit phase of the three-phase commit protocol.
func (c *CommitterImpl) Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := c.height.Load()
	if index != currentHeight {
		return nil, status.Errorf(codes.FailedPrecondition, "invalid precommit height: expected %d, got %d", currentHeight, index)
	}

	if c.state.getCurrentState() != proposeStage {
		return nil, status.Errorf(codes.FailedPrecondition, "precommit allowed only from propose state, current: %s", c.state.getCurrentState())
	}

	if c.pendingPayload == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no prepared record for height %d", index)
	}

	if err := c.wal.Write(iowal.PrecommitKey(index), c.pendingPayload); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write precommit: %v", err)
	}

	c.state.Transition(precommitStage)
	c.emitter.Emit(events.Event{Kind: events.EvCohortPrecommit, Height: index})

	go c.handlePrecommitTimeout(index)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: index}, nil
}

// Commit handles the commit phase of the atomic commit protocol.
func (c *CommitterImpl) Commit(ctx context.Context, req *dto.CommitRequest) (*dto.CohortResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commit(req.Height)
}

func (c *CommitterImpl) commit(height uint64) (*dto.CohortResponse, error) {
	currentHeight := c.height.Load()

	// re-delivered decision for a resolved height: repeat the recorded answer
	if height < currentHeight {
		if c.decisions[height] == iowal.PhaseKeyCommit {
			slog.Debug("commit already applied", "height", height, "current_height", currentHeight)
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: height}, nil
		}
		slog.Warn("rejecting commit for height resolved as abort", "height", height)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	// future height: reject
	if height > currentHeight {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	currentState := c.state.getCurrentState()
	expectedState := c.getExpectedCommitState()

	if currentState != expectedState {
		return nil, status.Errorf(codes.FailedPrecondition,
			"invalid state for commit: expected %s for %s mode, but current state is %s",
			expectedState, c.state.GetMode(), currentState)
	}

	if err := c.state.Transition(commitStage); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "invalid state transition to commit: %v", err)
	}
	c.emitter.Emit(events.Event{Kind: events.EvCohortCommit, Height: height})

	if !c.hookRegistry.ExecuteCommit(&dto.CommitRequest{Height: height}) {
		if terr := c.state.Transition(proposeStage); terr != nil {
			slog.Error("failed to reset state after hook failure", "err", terr)
		}
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, nil
	}

	if c.pendingPayload == nil {
		c.resetToPropose(height, "no pending payload")
		return nil, status.Errorf(codes.FailedPrecondition, "no pending payload")
	}

	walTx, err := iowal.Decode(c.pendingPayload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode error: %v", err)
	}

	if err := c.wal.Write(iowal.CommitKey(height), c.pendingPayload); err != nil {
		c.resetToPropose(height, "wal write failed")
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

// Abort handles abort requests from coordinator.
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

// abortCurrent journals the abort, records the decision, advances the height
// and returns the FSM to propose. Caller must hold c.mu.
func (c *CommitterImpl) abortCurrent(height uint64, reason string) error {
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
		// 3PC cannot transition directly from precommit -> propose; it must go via commit
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

// handleProposeTimeout aborts a 3PC transaction with no precommit in time;
// a cohort may abort unilaterally before the precommit point.
func (c *CommitterImpl) handleProposeTimeout(height uint64) {
	timer := time.NewTimer(time.Duration(c.timeout) * time.Millisecond)
	defer timer.Stop()
	<-timer.C

	c.mu.Lock()
	defer c.mu.Unlock()

	currentState := c.state.getCurrentState()
	currentHeight := c.height.Load()

	slog.Debug("propose timeout handler", "state", currentState, "height", currentHeight, "index", height)

	if currentState != proposeStage || currentHeight != height || c.pendingPayload == nil {
		slog.Debug("skipping propose timeout handling", "height", height, "state", currentState, "current_height", currentHeight)
		return
	}

	slog.Warn("aborting proposed message after timeout", "height", height)
	if err := c.abortCurrent(height, "propose timeout"); err != nil {
		slog.Error("failed to abort after propose timeout", "height", height, "err", err)
	}
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
		c.resetToPropose(height, "no pending payload")

		return
	}

	slog.Warn("performing autocommit after precommit timeout", "height", height)

	response, err := c.commit(height)
	if err != nil {
		slog.Error("autocommit failed", "height", height, "err", err)
		c.resetToPropose(height, "autocommit failed")
		return
	}

	if response != nil && response.ResponseType == dto.ResponseTypeNack {
		slog.Warn("autocommit returned NACK", "height", height)
		c.resetToPropose(height, "autocommit NACK")
		return
	}

	slog.Info("successfully autocommitted after precommit timeout", "height", height)
}

// awaitDecision is the cohort side of the 2PC termination protocol: while
// prepared at this height, it polls the coordinator for the outcome and applies it.
func (c *CommitterImpl) awaitDecision(height uint64) {
	if c.coordClient == nil {
		return // no coordinator link configured: classic blocking behaviour
	}

	interval := time.Duration(c.timeout) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if !c.stillPrepared(height) {
			return // decision already arrived through the normal path
		}

		reqCtx, cancel := context.WithTimeout(context.Background(), interval)
		outcome, err := c.coordClient.Decision(reqCtx, height)
		cancel()
		if err != nil {
			slog.Warn("decision request failed, will retry", "height", height, "err", err)
			continue
		}

		if outcome == dto.OutcomeUnknown {
			continue // coordinator has not decided yet: keep waiting
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

// applyDecision resolves the prepared transaction with the coordinator's
// outcome. Returns true once the height is resolved or no longer relevant.
func (c *CommitterImpl) applyDecision(height uint64, outcome dto.Outcome) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state.getCurrentState() != preparedStage || c.height.Load() != height {
		return true
	}

	slog.Info("applying coordinator decision", "height", height, "outcome", outcome)

	switch outcome {
	case dto.OutcomeCommit:
		if _, err := c.commit(height); err != nil {
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

// enterPrepared moves the cohort into the 2PC uncertainty period and starts
// polling the coordinator for the outcome.
func (c *CommitterImpl) enterPrepared(height uint64) error {
	if err := c.state.Transition(preparedStage); err != nil {
		return err
	}
	go c.awaitDecision(height)
	return nil
}

// Resume applies recovered WAL state: a 2PC cohort waits for the coordinator's
// decision, a 3PC cohort resumes the timeout matching its recovered phase.
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

// resumeTwoPhaseInDoubt resumes a 2PC cohort that voted YES before crashing;
// it can't decide alone, so it re-enters prepared and polls for the missed outcome.
func (c *CommitterImpl) resumeTwoPhaseInDoubt(tx *iowal.UnresolvedTransaction) {
	if err := c.enterPrepared(tx.Height); err != nil {
		slog.Error("failed to restore prepared state", "err", err)
		return
	}
	slog.Warn("recovered in-doubt transaction, awaiting coordinator decision", "height", tx.Height)
}

// resumeThreePhasePrepared resumes a 3PC cohort that acked propose but never
// saw precommit; no quorum is guaranteed, so it resumes the abort timeout.
func (c *CommitterImpl) resumeThreePhasePrepared(tx *iowal.UnresolvedTransaction) {
	slog.Warn("recovered prepared 3PC transaction, resuming abort timeout", "height", tx.Height)
	go c.handleProposeTimeout(tx.Height)
}

// resumeThreePhasePrecommit resumes a 3PC cohort that reached precommit; a
// quorum is guaranteed, so it resumes the autocommit timeout.
func (c *CommitterImpl) resumeThreePhasePrecommit(tx *iowal.UnresolvedTransaction) {
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
