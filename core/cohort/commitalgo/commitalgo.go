// Package commitalgo implements the core commit algorithms for 2PC and 3PC protocols.
//
// This package provides the finite state machine logic and transaction handling
// for cohort nodes participating in distributed consensus.
package commitalgo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/core/walrecord"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wal defines the interface for write-ahead log operations.
type wal interface {
	Write(index uint64, key string, value []byte) error
	WriteTombstone(index uint64) error
	Get(index uint64) (string, []byte, error)
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
	store        StateStore
	wal          wal
	hookRegistry *hooks.Registry
	state        *stateMachine
	height       uint64
	timeout      uint64
	mu           sync.Mutex
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

	// prepare payload
	ptx := walrecord.WalTx{Key: req.Key, Value: req.Value}
	pb, err := walrecord.Encode(ptx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode error: %v", err)
	}

	// save
	if err := c.wal.Write(walrecord.PreparedSlot(req.Height), walrecord.KeyPrepared, pb); err != nil {
		if terr := c.state.Transition(proposeStage); terr != nil {
			log.Errorf("failed to reset state to propose after WAL error: %v", terr)
		}
		return nil, status.Errorf(codes.Internal, "failed to write wal on index %d: %v", req.Height, err)
	}

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

	log.Debugf("propose timeout handler: state=%s, height=%d, index=%d", currentState, currentHeight, height)

	if currentState != proposeStage || currentHeight != height {
		log.Debugf("skipping propose timeout handling for height %d: state=%s, currentHeight=%d", height, currentState, currentHeight)
		return
	}

	if err := c.wal.Write(walrecord.AbortSlot(height), walrecord.KeyAbort, nil); err != nil {
		log.Errorf("failed to write skip record for height %d: %v", height, err)
	} else {
		log.Warnf("skip proposed message after timeout for height %d", height)
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

	// check if already aborted
	if k, _, _ := c.wal.Get(walrecord.AbortSlot(index)); k == walrecord.KeyAbort {
		return nil, status.Errorf(codes.Aborted, "transaction %d was aborted", index)
	}

	// read prepared to ensure we have data
	pSlot := walrecord.PreparedSlot(index)
	k, val, err := c.wal.Get(pSlot)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read prepared slot %d: %v", pSlot, err)
	}
	if k != walrecord.KeyPrepared {
		return nil, status.Errorf(codes.FailedPrecondition, "not prepared (key=%s)", k)
	}

	if err := c.wal.Write(walrecord.PrecommitSlot(index), walrecord.KeyPrecommit, val); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write precommit: %v", err)
	}

	c.state.Transition(precommitStage)

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

	log.Debugf("precommit timeout handler: state=%s, height=%d, index=%d", currentState, currentHeight, height)

	if currentState != precommitStage || currentHeight != height {
		log.Debugf("skipping autocommit for height %d: state=%s, currentHeight=%d", height, currentState, currentHeight)
		return
	}

	// check abort
	if k, _, _ := c.wal.Get(walrecord.AbortSlot(height)); k == walrecord.KeyAbort {
		log.Infof("found abort record for height %d during precommit timeout", height)
		c.resetToPropose(height, "abort record found")
		return
	}

	// check data (prepared/precommit)
	key, value, err := c.wal.Get(walrecord.PreparedSlot(height))
	if err != nil {
		log.Errorf("failed to read WAL for height %d during precommit timeout: %v", height, err)
		c.resetToPropose(height, "WAL read error")
		return
	}
	if key != walrecord.KeyPrepared {
		log.Warnf("unexpected key at prepared slot: %s", key)
		c.resetToPropose(height, "invalid prepared key")
		return
	}
	if value == nil {
		log.Errorf("no data found in WAL for height %d during precommit timeout", height)
		c.resetToPropose(height, "no WAL data")
		return
	}

	log.Warnf("performing autocommit after precommit timeout for height %d", height)

	response, err := c.commit(height)
	if err != nil {
		log.Errorf("autocommit failed for height %d: %v", height, err)
		c.resetToPropose(height, "autocommit failed")
		return
	}

	if response != nil && response.ResponseType == dto.ResponseTypeNack {
		log.Warnf("autocommit returned NACK for height %d", height)
		c.resetToPropose(height, "autocommit NACK")
		return
	}

	log.Infof("successfully autocommitted height %d after precommit timeout", height)
}

func (c *CommitterImpl) resetToPropose(height uint64, reason string) {
	log.Debugf("resetting state to propose for height %d: %s", height, reason)

	currentState := c.state.getCurrentState()

	if c.state.GetMode() == threephase && currentState == precommitStage {
		if err := c.state.Transition(commitStage); err != nil {
			log.Errorf("failed to transition to commit state during reset for height %d: %v", height, err)
			return
		}
	}

	if err := c.state.Transition(proposeStage); err != nil {
		log.Errorf("failed to reset to propose state for height %d: %v (current: %s)", height, err, c.state.getCurrentState())
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
		log.Debugf("commit for height %d already applied (current height: %d)", height, currentHeight)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: height}, nil
	}

	// future height: reject
	if height > currentHeight {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	// height == currentHeight: process the commit
	currentState := c.state.getCurrentState()
	expectedState := c.getExpectedCommitState()

	if currentState != expectedState {
		return nil, status.Errorf(codes.FailedPrecondition,
			"invalid state for commit: expected %s for %s mode, but current state is %s",
			expectedState, c.state.GetMode(), currentState)
	}

	// check if already Aborted
	if k, _, _ := c.wal.Get(walrecord.AbortSlot(height)); k == walrecord.KeyAbort {
		log.Warnf("rejecting commit for aborted height %d", height)
		c.resetToPropose(height, "wal aborted")
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, nil
	}

	if err := c.state.Transition(commitStage); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "invalid state transition to commit: %v", err)
	}

	if !c.hookRegistry.ExecuteCommit(&dto.CommitRequest{Height: height}) {
		if terr := c.state.Transition(proposeStage); terr != nil {
			log.Errorf("failed to reset state after hook failure: %v", terr)
		}
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, nil
	}

	// retrieve payload (try Precommit, then Prepared)
	var payload []byte
	if k, v, err := c.wal.Get(walrecord.PrecommitSlot(height)); err == nil && k == walrecord.KeyPrecommit {
		payload = v
	} else if k, v, err := c.wal.Get(walrecord.PreparedSlot(height)); err == nil && k == walrecord.KeyPrepared {
		payload = v
	} else {
		c.resetToPropose(height, "no prepared/precommit data found")
		return nil, status.Errorf(codes.FailedPrecondition, "no prepared/precommit data found")
	}

	// decode
	walTx, err := walrecord.Decode(payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode error: %v", err)
	}

	// write commit to WAL
	if err := c.wal.Write(walrecord.CommitSlot(height), walrecord.KeyCommit, payload); err != nil {
		c.resetToPropose(height, "wal write failed")
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, err
	}

	// 2. apply to storage
	if err := c.store.Put(walTx.Key, walTx.Value); err != nil {
		log.Errorf("CRITICAL: failed to apply committed tx to store: %v", err)
		return nil, err
	}

	atomic.StoreUint64(&c.height, currentHeight+1)

	if terr := c.state.Transition(proposeStage); terr != nil {
		log.Errorf("failed to transition back to propose state after successful commit: %v", terr)
	}

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
	log.Warnf("received abort request for height %d: %s", req.Height, req.Reason)

	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := atomic.LoadUint64(&c.height)

	if req.Height > currentHeight {
		log.Debugf("ignoring abort for future height %d (current: %d)", req.Height, currentHeight)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
	}

	if req.Height < currentHeight {
		log.Debugf("ignoring abort for past height %d (current: %d)", req.Height, currentHeight)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
	}

	log.Infof("processing abort for current height %d", req.Height)

	if err := c.wal.Write(walrecord.AbortSlot(req.Height), walrecord.KeyAbort, nil); err != nil {
		log.Errorf("failed to write abort record for aborted transaction at height %d: %v", req.Height, err)
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, err
	}

	log.Infof("successfully wrote tombstone record for height %d", req.Height)

	c.resetToPropose(req.Height, "abort request")

	log.Infof("successfully processed abort for height %d", req.Height)
	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
}
