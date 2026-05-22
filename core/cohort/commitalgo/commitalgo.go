// Package commitalgo implements the core commit algorithms for 2PC and 3PC protocols.
//
// This package provides the finite state machine logic and transaction handling
// for cohort nodes participating in distributed consensus.
package commitalgo

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/tui/events"
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

// CommitterImpl implements the commit algorithm with state machine and hooks.
type CommitterImpl struct {
	store          StateStore
	wal            wal
	hookRegistry   *hooks.Registry
	state          *stateMachine
	height         uint64
	timeout        uint64
	mu             sync.Mutex
	pendingPayload []byte         // encoded payload of the current in-progress transaction
	aborted        bool           // whether the current transaction was aborted
	emitter        events.Emitter
}

// SetEmitter injects an event emitter for TUI visualization. Safe to call after NewCommitter().
func (c *CommitterImpl) SetEmitter(e events.Emitter) {
	if e == nil {
		e = events.NoopEmitter{}
	}
	c.emitter = e
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
		emitter:      events.NoopEmitter{},
	}
}

// RegisterHook adds a new hook to the committer.
func (c *CommitterImpl) RegisterHook(hook hooks.Hook) {
	c.hookRegistry.Register(hook)
}

// Height returns the current transaction height.
func (c *CommitterImpl) Height() uint64 {
	return atomic.LoadUint64(&c.height)
}

// SetHeight initializes committer height from recovered WAL state.
func (c *CommitterImpl) SetHeight(height uint64) {
	atomic.StoreUint64(&c.height, height)
}

// SetPendingPayload restores the in-progress transaction payload after crash recovery.
func (c *CommitterImpl) SetPendingPayload(payload []byte) {
	c.pendingPayload = payload
}

func (c *CommitterImpl) resetPending() {
	c.pendingPayload = nil
	c.aborted = false
}

func (c *CommitterImpl) getCurrentState() string {
	return c.state.getCurrentState()
}

// Propose handles the propose phase of the commit protocol.
func (c *CommitterImpl) Propose(ctx context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := atomic.LoadUint64(&c.height)
	if currentHeight > req.Height {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	if !c.hookRegistry.ExecutePropose(req) {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: req.Height}, nil
	}

	if err := c.state.Transition(proposeStage); err != nil {
		return nil, err
	}
	c.emitter.Emit(events.Event{Kind: events.EvCohortPropose, Height: req.Height, Key: req.Key})

	payload, err := iowal.Encode(iowal.Tx{Key: req.Key, Value: req.Value})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode error: %v", err)
	}

	if err := c.wal.Write(iowal.PreparedKey(req.Height), payload); err != nil {
		if terr := c.state.Transition(proposeStage); terr != nil {
			slog.Error("failed to reset state to propose after WAL error", "err", terr)
		}

		return nil, status.Errorf(codes.Internal, "failed to write wal on index %d: %v", req.Height, err)
	}
	c.pendingPayload = payload

	if c.state.mode == twophase {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
	}

	go c.handleProposeTimeout(req.Height)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
}

func (c *CommitterImpl) handleProposeTimeout(height uint64) {
	timer := time.NewTimer(time.Duration(c.timeout) * time.Millisecond)
	defer timer.Stop()
	<-timer.C

	c.mu.Lock()
	defer c.mu.Unlock()

	currentState := c.state.getCurrentState()
	currentHeight := atomic.LoadUint64(&c.height)

	slog.Debug("propose timeout handler", "state", currentState, "height", currentHeight, "index", height)

	if currentState != proposeStage || currentHeight != height {
		slog.Debug("skipping propose timeout handling", "height", height, "state", currentState, "current_height", currentHeight)
		return
	}

	if err := c.wal.Write(iowal.AbortKey(height), nil); err != nil {
		slog.Error("failed to write skip record", "height", height, "err", err)
	} else {
		c.aborted = true
		slog.Warn("skip proposed message after timeout", "height", height)
	}
}

// Precommit handles the precommit phase of the three-phase commit protocol.
func (c *CommitterImpl) Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := atomic.LoadUint64(&c.height)
	if index != currentHeight {
		return nil, status.Errorf(codes.FailedPrecondition, "invalid precommit height: expected %d, got %d", currentHeight, index)
	}

	if c.state.getCurrentState() != proposeStage {
		return nil, status.Errorf(codes.FailedPrecondition, "precommit allowed only from propose state, current: %s", c.state.getCurrentState())
	}

	if c.aborted {
		return nil, status.Errorf(codes.Aborted, "transaction %d was aborted", index)
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

func (c *CommitterImpl) handlePrecommitTimeout(height uint64) {
	timer := time.NewTimer(time.Duration(c.timeout) * time.Millisecond)
	defer timer.Stop()
	<-timer.C

	c.mu.Lock()
	defer c.mu.Unlock()

	currentState := c.state.getCurrentState()
	currentHeight := atomic.LoadUint64(&c.height)

	slog.Debug("precommit timeout handler", "state", currentState, "height", currentHeight, "index", height)

	if currentState != precommitStage || currentHeight != height {
		slog.Debug("skipping autocommit", "height", height, "state", currentState, "current_height", currentHeight)
		return
	}

	if c.aborted {
		slog.Info("found abort record during precommit timeout", "height", height)
		c.resetToPropose(height, "abort record found")

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

// Commit handles the commit phase of the atomic commit protocol.
func (c *CommitterImpl) Commit(ctx context.Context, req *dto.CommitRequest) (*dto.CohortResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commit(req.Height)
}

func (c *CommitterImpl) commit(height uint64) (*dto.CohortResponse, error) {
	currentHeight := atomic.LoadUint64(&c.height)

	// idempotent commit: if already applied, return ACK without error
	if height < currentHeight {
		slog.Debug("commit already applied", "height", height, "current_height", currentHeight)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: height}, nil
	}

	// future height: reject
	if height > currentHeight {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	if c.aborted {
		slog.Warn("rejecting commit for aborted height", "height", height)
		c.resetToPropose(height, "aborted")

		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, nil
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
		slog.Error("CRITICAL: failed to apply committed tx to store", "err", err)
		return nil, err
	}

	atomic.StoreUint64(&c.height, currentHeight+1)
	c.resetPending()

	if terr := c.state.Transition(proposeStage); terr != nil {
		slog.Error("failed to transition back to propose state after successful commit", "err", terr)
	}
	c.emitter.Emit(events.Event{Kind: events.EvCohortCommit, Height: currentHeight, Result: "ok"})

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
}

func (c *CommitterImpl) getExpectedCommitState() string {
	if c.state.GetMode() == twophase {
		return proposeStage
	}
	return precommitStage
}

// Abort handles abort requests from coordinator.
func (c *CommitterImpl) Abort(ctx context.Context, req *dto.AbortRequest) (*dto.CohortResponse, error) {
	slog.Warn("received abort request", "height", req.Height, "reason", req.Reason)

	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := atomic.LoadUint64(&c.height)

	if req.Height > currentHeight {
		slog.Debug("ignoring abort for future height", "height", req.Height, "current", currentHeight)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
	}

	if req.Height < currentHeight {
		slog.Debug("ignoring abort for past height", "height", req.Height, "current", currentHeight)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
	}

	slog.Info("processing abort for current height", "height", req.Height)

	if err := c.wal.Write(iowal.AbortKey(req.Height), nil); err != nil {
		slog.Error("failed to write abort record", "height", req.Height, "err", err)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, err
	}
	c.aborted = true

	slog.Info("successfully wrote tombstone record", "height", req.Height)
	c.emitter.Emit(events.Event{Kind: events.EvCohortAbort, Height: req.Height, Message: req.Reason})

	c.resetToPropose(req.Height, "abort request")

	slog.Info("successfully processed abort", "height", req.Height)
	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
}
