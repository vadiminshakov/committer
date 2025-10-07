package commitalgo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/dto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type wal interface {
	Write(index uint64, key string, value []byte) error
	WriteTombstone(index uint64) error
	Get(index uint64) (string, []byte, error)
	Close() error
}

//go:generate mockgen -destination=../../../mocks/mock_commitalgo_state_store.go -package=mocks -mock_names=StateStore=MockCommitalgoStateStore . StateStore
type StateStore interface {
	Put(key string, value []byte) error
	Close() error
}

type CommitterImpl struct {
	noAutoCommit map[uint64]struct{}
	store        StateStore
	wal          wal
	hookRegistry *hooks.Registry
	state        *stateMachine
	height       uint64
	timeout      uint64
	timeoutMutex sync.RWMutex
	commitMutex  sync.Mutex
}

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
		noAutoCommit: make(map[uint64]struct{}),
		timeout:      timeout,
		state:        newStateMachine(mode(commitType)),
	}
}

// RegisterHook adds a new hook to the committer
func (c *CommitterImpl) RegisterHook(hook hooks.Hook) {
	c.hookRegistry.Register(hook)
}

func (c *CommitterImpl) Height() uint64 {
	return atomic.LoadUint64(&c.height)
}

// SetHeight initializes committer height from recovered WAL state.
func (c *CommitterImpl) SetHeight(height uint64) {
	atomic.StoreUint64(&c.height, height)
}

func (c *CommitterImpl) Propose(ctx context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error) {
	if err := c.state.Transition(proposeStage); err != nil {
		return nil, err
	}

	if atomic.LoadUint64(&c.height) > req.Height {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: atomic.LoadUint64(&c.height)}, nil
	}

	if !c.hookRegistry.ExecutePropose(req) {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: req.Height}, nil
	}

	log.Infof("received: %s=%s\n", req.Key, string(req.Value))
	c.wal.Write(req.Height, req.Key, req.Value)

	if c.state.mode == twophase {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
	}

	go func(ctx context.Context) {
		deadline := time.After(time.Duration(c.timeout) * time.Millisecond)
		select {
		case <-deadline:
			// check of state and write to WAL under timeout mutex
			c.timeoutMutex.Lock()
			currentState := c.getCurrentState()
			if currentState == precommitStage || currentState == commitStage {
				c.timeoutMutex.Unlock()
				return
			}

			// write skip record atomically with state check
			if err := c.wal.Write(req.Height, "skip", nil); err != nil {
				log.Errorf("Failed to write skip record for height %d: %v", req.Height, err)
			} else {
				log.Warnf("skip proposed message after timeout for height %d", req.Height)
			}
			c.timeoutMutex.Unlock()
		case <-ctx.Done():
			return
		}
	}(ctx)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
}

func (c *CommitterImpl) getCurrentState() string {
	return c.state.GetCurrentState()
}

// getExpectedCommitState returns the expected state for commit based on the protocol mode
func (c *CommitterImpl) getExpectedCommitState() string {
	if c.state.GetMode() == twophase {
		return proposeStage
	}
	return precommitStage
}

func (c *CommitterImpl) Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error) {
	if err := c.state.Transition(precommitStage); err != nil {
		return nil, err
	}

	go func(ctx context.Context) {
		deadline := time.After(time.Duration(c.timeout) * time.Millisecond)
		select {
		case <-deadline:
			log.Debugf("Precommit timeout triggered for height %d", index)
			c.handlePrecommitTimeout(ctx, index)
		case <-ctx.Done():
			log.Debugf("Precommit timeout cancelled for height %d due to context cancellation", index)
			return
		}
	}(ctx)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
}

// handlePrecommitTimeout handles the timeout logic for precommit phase with improved validation and recovery
func (c *CommitterImpl) handlePrecommitTimeout(ctx context.Context, index uint64) {
	c.timeoutMutex.Lock()
	defer c.timeoutMutex.Unlock()

	// enhanced state validation before autocommit
	currentState := c.getCurrentState()
	currentHeight := atomic.LoadUint64(&c.height)

	log.Debugf("Precommit timeout handler: state=%s, height=%d, index=%d", currentState, currentHeight, index)

	// additional checks to prevent incorrect autocommits
	if currentState == commitStage {
		log.Debugf("Skipping autocommit for height %d: already in commit state", index)
		return
	}

	if currentState == proposeStage {
		log.Debugf("Skipping autocommit for height %d: already returned to propose state", index)
		return
	}

	// validate that we're still in precommit state and height matches
	if currentState != precommitStage {
		log.Warnf("Unexpected state %s during precommit timeout for height %d, skipping autocommit", currentState, index)
		return
	}

	if currentHeight != index {
		log.Warnf("Height mismatch during precommit timeout: expected %d, current %d, skipping autocommit", index, currentHeight)
		return
	}

	// check if we have the data in WAL before attempting autocommit
	key, value, err := c.wal.Get(index)
	if err != nil {
		log.Errorf("Failed to read WAL for height %d during precommit timeout: %v", index, err)
		c.recoverToPropose(index)
		return
	}
	if key == "skip" {
		log.Infof("Found skip record for height %d during precommit timeout, transitioning to propose", index)
		c.recoverToPropose(index)
		return
	}

	if value == nil {
		log.Errorf("No data found in WAL for height %d during precommit timeout, cannot autocommit", index)
		c.recoverToPropose(index)
		return
	}

	// perform autocommit with proper error handling
	log.Warnf("Performing autocommit after precommit timeout for height %d", index)

	commitReq := &dto.CommitRequest{Height: index}
	response, err := c.Commit(ctx, commitReq)
	if err != nil {
		log.Errorf("Autocommit failed for height %d: %v", index, err)
		c.recoverToPropose(index)
		return
	}

	if response != nil && response.ResponseType == dto.ResponseTypeNack {
		log.Warnf("Autocommit returned NACK for height %d", index)
		c.recoverToPropose(index)
		return
	}

	log.Infof("Successfully autocommitted height %d after precommit timeout", index)
}

// recoverToPropose attempts to recover state to propose when autocommit fails or is inappropriate
func (c *CommitterImpl) recoverToPropose(index uint64) {
	log.Debugf("Attempting state recovery to propose for height %d", index)

	currentState := c.getCurrentState()

	// for 3PC mode, we need to go through commit state to reach propose
	if c.state.GetMode() == threephase && currentState == precommitStage {
		// First transition to commit state
		if err := c.state.Transition(commitStage); err != nil {
			log.Errorf("Failed to transition to commit state during recovery for height %d: %v", index, err)
			return
		}
		log.Debugf("Transitioned to commit state during recovery for height %d", index)
	}

	// now try to transition to propose state
	if err := c.state.Transition(proposeStage); err != nil {
		log.Errorf("Failed to recover to propose state for height %d: %v", index, err)
		// Log current state for debugging
		log.Errorf("Current state during recovery failure: %s", c.getCurrentState())
	} else {
		log.Debugf("Successfully recovered to propose state for height %d", index)
	}
}

func (c *CommitterImpl) Commit(ctx context.Context, req *dto.CommitRequest) (*dto.CohortResponse, error) {
	c.commitMutex.Lock()
	defer c.commitMutex.Unlock()

	// step 1: validate current state before attempting transition
	currentState := c.state.GetCurrentState()
	expectedState := c.getExpectedCommitState()

	if currentState != expectedState {
		return nil, status.Errorf(codes.FailedPrecondition,
			"invalid state for commit: expected %s for %s mode, but current state is %s",
			expectedState, c.state.GetMode(), currentState)
	}

	// step 2: validate state transition to commit
	if err := c.state.Transition(commitStage); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"invalid state transition to commit: %v", err)
	}

	// step 3: atomic height check and validation
	currentHeight := atomic.LoadUint64(&c.height)
	if req.Height != currentHeight {
		// restore state on height mismatch - transition back to propose
		c.state.Transition(proposeStage)
		return nil, status.Errorf(codes.AlreadyExists, "invalid commit height (got %d, but expected %d)", req.Height, currentHeight)
	}

	// step 4: execute commit operations
	var response *dto.CohortResponse
	if c.hookRegistry.ExecuteCommit(req) {
		log.Printf("Committing on height: %d\n", req.Height)

		key, value, err := c.wal.Get(req.Height)
		if err != nil {
			// restore state on WAL error - transition back to propose
			c.state.Transition(proposeStage)
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, fmt.Errorf("failed to read wal on index %d: %w", req.Height, err)
		}
		if key == "skip" {
			log.Infof("Cannot commit height %d: operation was cancelled (skip record found)", req.Height)
			// restore state on skip record - transition back to propose
			c.state.Transition(proposeStage)
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, nil
		} else {
			if value == nil {
				// restore state on WAL miss - transition back to propose
				c.state.Transition(proposeStage)
				return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, fmt.Errorf("no value in node cache on the index %d", req.Height)
			}
			if err := c.store.Put(key, value); err != nil {
				// restore state on state write error - transition back to propose
				c.state.Transition(proposeStage)
				return nil, err
			}
		}

		// step 5: atomic height increment only after successful execution
		if !atomic.CompareAndSwapUint64(&c.height, currentHeight, currentHeight+1) {
			// this should not happen under mutex, but handle gracefully
			// restore state on height CAS failure - transition back to propose
			c.state.Transition(proposeStage)
			return nil, status.Errorf(codes.Internal, "height was modified during commit operation")
		}

		response = &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}
	} else {
		// restore state on hook failure - transition back to propose
		c.state.Transition(proposeStage)
		response = &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}
	}

	// step 6: return to propose state for next transaction (only on success)
	if response.ResponseType == dto.ResponseTypeAck {
		if err := c.state.Transition(proposeStage); err != nil {
			log.Errorf("Failed to transition back to propose state after successful commit: %v", err)
		}
	}

	return response, nil
}

// Abort handles abort requests from coordinator
func (c *CommitterImpl) Abort(ctx context.Context, req *dto.AbortRequest) (*dto.CohortResponse, error) {
	log.Warnf("Received abort request for height %d: %s", req.Height, req.Reason)

	c.commitMutex.Lock()
	defer c.commitMutex.Unlock()

	currentHeight := atomic.LoadUint64(&c.height)

	// if the abort is for a future height, we can ignore it
	if req.Height > currentHeight {
		log.Debugf("Ignoring abort for future height %d (current: %d)", req.Height, currentHeight)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
	}

	// if the abort is for a past height, the transaction is already processed
	if req.Height < currentHeight {
		log.Debugf("Ignoring abort for past height %d (current: %d)", req.Height, currentHeight)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
	}

	// for current height, we need to rollback any changes and reset state
	log.Infof("Processing abort for current height %d", req.Height)

	// write tombstone record to mark this transaction as aborted
	if err := c.wal.WriteTombstone(req.Height); err != nil {
		log.Errorf("Failed to write tombstone record for aborted transaction at height %d: %v", req.Height, err)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, err
	}

	log.Infof("Successfully wrote tombstone record for height %d", req.Height)

	// reset state to propose for next transaction
	currentState := c.getCurrentState()
	if currentState != proposeStage {
		// for 3PC mode, we might need to go through commit state to reach propose
		if c.state.GetMode() == threephase && currentState == precommitStage {
			if err := c.state.Transition(commitStage); err != nil {
				log.Errorf("Failed to transition to commit state during abort recovery: %v", err)
			}
		}

		if err := c.state.Transition(proposeStage); err != nil {
			log.Errorf("Failed to transition to propose state after abort: %v", err)
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, err
		}
	}

	log.Infof("Successfully processed abort for height %d", req.Height)
	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
}
