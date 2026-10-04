// Package cohort implements the participant side of 2PC and 3PC.
package cohort

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vadiminshakov/committer/v2/core/dto"
	"github.com/vadiminshakov/committer/v2/events"
	iowal "github.com/vadiminshakov/committer/v2/io/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wal persists protocol records.
type wal interface {
	Write(key string, value []byte) error
	Close() error
}

// Resource is the state a cohort protects. The cohort calls it from one
// goroutine at a time, in height order, and logs every protocol step in its
// WAL so it can repeat calls after a crash.
//
//go:generate mockgen -destination=../../mocks/mock_resource.go -package=mocks . Resource
type Resource interface {
	// Prepare votes on tx. Returning nil is a YES vote: from then on Commit
	// for this height must succeed eventually, so reserve or lock whatever
	// it needs. A non-nil error is a NO vote; its text reaches the caller of
	// Coordinator.Commit.
	Prepare(ctx context.Context, tx dto.Tx) error
	// Commit applies tx. It must be idempotent: after a crash it may be
	// called again for a height that was already applied. A returned error
	// is retried until the call succeeds.
	Commit(ctx context.Context, tx dto.Tx) error
	// Abort discards whatever Prepare reserved for height. It must be
	// idempotent and must accept heights it has never prepared. A returned
	// error is retried until the call succeeds.
	Abort(ctx context.Context, height uint64) error
}

// DecisionRequester asks the coordinator for the recorded outcome of a height.
//
//go:generate mockgen -destination=../../mocks/mock_decision_requester.go -package=mocks -mock_names=DecisionRequester=MockDecisionRequester . DecisionRequester
type DecisionRequester interface {
	Decision(ctx context.Context, height uint64) (dto.Outcome, error)
}

type mode string

const (
	twophase   mode = "two-phase"
	threephase mode = "three-phase"
)

type phase string

const (
	// proposeStage means that the cohort is ready to accept a transaction at
	// the current height.
	proposeStage phase = "propose"
	// preparedStage is the PREPARED/WAITING state. The cohort has voted YES,
	// durably stored the payload, and must wait for the coordinator's next
	// decision. In 3PC this is deliberately distinct from proposeStage.
	preparedStage phase = "prepared"
	// precommitStage is committable: the cohort must not choose ABORT locally.
	precommitStage phase = "precommit"
	// commitStage means that the final commit is being applied. The cohort
	// returns to proposeStage only after the WAL and resource operations succeed.
	commitStage phase = "commit"
)

// Cohort runs the cohort commit state machine.
type Cohort struct {
	resource       Resource
	wal            wal
	mode           mode
	phase          phase
	height         atomic.Uint64
	timeout        uint64
	mu             sync.Mutex        // protects phase, pendingPayload, decisions, and Resource calls
	pendingPayload []byte            // encoded payload of the current transaction
	decisions      map[uint64]string // final outcome of each resolved height
	coordClient    DecisionRequester // coordinator used for decision requests
	emitter        events.Emitter
	shutdown       func() error
}

// newCohort creates a cohort state machine over resource and wal.
func newCohort(resource Resource, commitType string, wal wal, timeout uint64) *Cohort {
	return &Cohort{
		resource:  resource,
		wal:       wal,
		timeout:   timeout,
		mode:      mode(commitType),
		phase:     proposeStage,
		decisions: make(map[uint64]string),
		emitter:   events.NoopEmitter{},
	}
}

// SetEmitter sets the event emitter. A nil emitter disables events.
func (c *Cohort) SetEmitter(e events.Emitter) {
	if e == nil {
		e = events.NoopEmitter{}
	}

	c.emitter = e
}

// SetDecisionRequester sets the coordinator used by the termination protocol.
// Call it before serving requests.
func (c *Cohort) SetDecisionRequester(dr DecisionRequester) {
	c.coordClient = dr
}

// Height returns the current transaction height.
func (c *Cohort) Height() uint64 {
	return c.height.Load()
}

// SetHeight initializes the cohort height from recovered WAL state.
func (c *Cohort) SetHeight(height uint64) {
	c.height.Store(height)
}

// Propose handles the propose phase.
func (c *Cohort) Propose(ctx context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error) {
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
	if c.phase != proposeStage {
		if bytes.Equal(payload, c.pendingPayload) {
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
		}

		slog.Warn("rejecting conflicting proposal for occupied height", "height", req.Height, "state", c.phase)
		// NACK prevents retries from replacing the payload at this height.
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, nil
	}

	if err := c.resource.Prepare(ctx, dto.Tx{Height: req.Height, Key: req.Key, Value: req.Value}); err != nil {
		slog.Info("resource rejected proposal", "height", req.Height, "err", err)

		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: req.Height, Reason: err.Error()}, nil
	}

	c.emitter.Emit(events.Event{Kind: events.EvCohortPropose, Height: req.Height, Key: req.Key})

	if err := c.wal.Write(iowal.PreparedKey(req.Height), payload); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write wal on index %d: %v", req.Height, err)
	}

	c.pendingPayload = payload
	c.phase = preparedStage

	go c.awaitDecision(req.Height)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
}

// Precommit handles the 3PC precommit phase.
func (c *Cohort) Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mode != threephase {
		return nil, status.Error(codes.FailedPrecondition, "precommit is allowed for 3PC mode only")
	}

	currentHeight := c.height.Load()
	if index != currentHeight {
		return nil, status.Errorf(codes.FailedPrecondition,
			"invalid precommit height: expected %d, got %d", currentHeight, index)
	}

	if c.phase == precommitStage && c.pendingPayload != nil {
		// repeat the ACK without rewriting the durable PRECOMMIT record.
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: index}, nil
	}

	if c.phase != preparedStage {
		return nil, status.Errorf(codes.FailedPrecondition,
			"precommit allowed only from prepared/waiting state, current: %s", c.phase)
	}

	if c.pendingPayload == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no prepared record for height %d", index)
	}

	if err := c.wal.Write(iowal.PrecommitKey(index), c.pendingPayload); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write precommit: %v", err)
	}

	c.phase = precommitStage

	c.emitter.Emit(events.Event{Kind: events.EvCohortPrecommit, Height: index})

	go c.handlePrecommitTimeout(index)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: index}, nil
}

// Commit validates and serializes an external commit request.
func (c *Cohort) Commit(ctx context.Context, req *dto.CommitRequest) (*dto.CohortResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "commit request is required")
	}

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.commit(ctx, req)
}

// staleCommitResponse answers a commit for an already-resolved height.
// It reports handled=false when the commit targets the current height.
func (c *Cohort) staleCommitResponse(height, currentHeight uint64) (*dto.CohortResponse, bool) {
	if height < currentHeight {
		if c.decisions[height] == iowal.PhaseKeyCommit {
			slog.Debug("commit already applied", "height", height, "current_height", currentHeight)

			return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: height}, true
		}

		slog.Warn("rejecting commit for height resolved as abort", "height", height)

		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, true
	}

	if height > currentHeight {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: currentHeight}, true
	}

	return nil, false
}

func (c *Cohort) commit(ctx context.Context, req *dto.CommitRequest) (*dto.CohortResponse, error) {
	currentHeight := c.height.Load()

	if resp, handled := c.staleCommitResponse(req.Height, currentHeight); handled {
		return resp, nil
	}

	expectedState := c.getExpectedCommitState()
	if c.phase != expectedState && c.phase != commitStage {
		return nil, status.Errorf(codes.FailedPrecondition,
			"invalid state for commit: expected %s for %s mode, but current state is %s",
			expectedState, c.mode, c.phase)
	}

	c.phase = commitStage

	c.emitter.Emit(events.Event{Kind: events.EvCohortCommit, Height: req.Height})

	if c.pendingPayload == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no pending payload")
	}

	walTx, err := iowal.Decode(c.pendingPayload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode error: %v", err)
	}

	if err := c.wal.Write(iowal.CommitKey(req.Height), c.pendingPayload); err != nil {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack},
			fmt.Errorf("write commit record at height %d: %w", req.Height, err)
	}

	// COMMIT is durable, so the height stays in commitStage until the resource
	// applies it: a redelivered COMMIT or a restart retries the call.
	if err := c.resource.Commit(ctx, dto.Tx{Height: req.Height, Key: walTx.Key, Value: walTx.Value}); err != nil {
		slog.Error("resource failed to apply committed tx, will retry on redelivery or restart",
			"height", req.Height, "err", err)

		return nil, status.Errorf(codes.Unavailable, "apply committed tx at height %d: %v", req.Height, err)
	}

	c.decisions[req.Height] = iowal.PhaseKeyCommit
	c.height.Store(currentHeight + 1)
	c.resetPending()

	c.phase = proposeStage

	c.emitter.Emit(events.Event{Kind: events.EvCohortCommit, Height: currentHeight, Result: "ok"})

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
}

// Abort handles coordinator abort requests.
func (c *Cohort) Abort(ctx context.Context, req *dto.AbortRequest) (*dto.CohortResponse, error) {
	slog.Warn("received abort request", "height", req.Height, "reason", req.Reason)

	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := c.height.Load()

	if req.Height != currentHeight {
		slog.Debug("ignoring abort for non-current height", "height", req.Height, "current", currentHeight)

		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: currentHeight}, nil
	}

	slog.Info("processing abort for current height", "height", req.Height)

	if err := c.abortCurrent(ctx, req.Height, req.Reason); err != nil {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, err
	}

	slog.Info("successfully processed abort", "height", req.Height)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: c.height.Load()}, nil
}

func (c *Cohort) abortCurrent(ctx context.Context, height uint64, reason string) error {
	if c.mode == threephase && (c.phase == precommitStage || c.phase == commitStage) {
		return status.Errorf(codes.FailedPrecondition,
			"cannot abort 3PC transaction after precommit, current state: %s", c.phase)
	}

	if c.phase == commitStage {
		return status.Errorf(codes.FailedPrecondition, "cannot abort transaction while commit is in progress")
	}

	if err := c.wal.Write(iowal.AbortKey(height), nil); err != nil {
		slog.Error("failed to write abort record", "height", height, "err", err)

		return fmt.Errorf("write abort record at height %d: %w", height, err)
	}

	// ABORT is durable; keeping the height lets a redelivered ABORT retry.
	if err := c.resource.Abort(ctx, height); err != nil {
		slog.Error("resource failed to abort tx", "height", height, "err", err)

		return status.Errorf(codes.Unavailable, "abort tx at height %d: %v", height, err)
	}

	c.decisions[height] = iowal.PhaseKeyAbort
	c.height.Store(height + 1)
	c.resetPending()

	c.emitter.Emit(events.Event{Kind: events.EvCohortAbort, Height: height, Message: reason})
	c.phase = proposeStage

	return nil
}

func (c *Cohort) getExpectedCommitState() phase {
	if c.mode == twophase {
		return preparedStage
	}

	return precommitStage
}

func (c *Cohort) handlePrecommitTimeout(height uint64) {
	timer := time.NewTimer(time.Duration(c.timeout) * time.Millisecond)
	defer timer.Stop()

	<-timer.C

	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := c.height.Load()

	slog.Debug("precommit timeout handler", "state", c.phase, "height", currentHeight, "index", height)

	if c.phase != precommitStage || currentHeight != height {
		slog.Debug("skipping autocommit", "height", height, "state", c.phase, "current_height", currentHeight)

		return
	}

	if c.pendingPayload == nil {
		slog.Error("no pending payload during precommit timeout", "height", height)

		return
	}

	slog.Warn("performing autocommit after precommit timeout", "height", height)

	response, err := c.commit(context.Background(), &dto.CommitRequest{Height: height})
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
func (c *Cohort) awaitDecision(height uint64) {
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

func (c *Cohort) stillPrepared(height uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.phase == preparedStage && c.height.Load() == height
}

// applyDecision applies the outcome and reports whether the height is resolved.
func (c *Cohort) applyDecision(height uint64, outcome dto.Outcome) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase != preparedStage || c.height.Load() != height {
		return true
	}

	slog.Info("applying coordinator decision", "height", height, "outcome", outcome)

	switch outcome {
	case dto.OutcomeCommit:
		if _, err := c.commit(context.Background(), &dto.CommitRequest{Height: height}); err != nil {
			slog.Error("failed to apply commit decision", "height", height, "err", err)

			return false
		}
	case dto.OutcomeAbort:
		if err := c.abortCurrent(context.Background(), height, "coordinator decision"); err != nil {
			slog.Error("failed to apply abort decision", "height", height, "err", err)

			return false
		}
	default:
		return false
	}

	return true
}

// Resume restores WAL state, brings the resource in line with it, and resumes
// termination handling. Call it once before serving requests.
func (c *Cohort) Resume(ctx context.Context, rec *iowal.RecoveryState) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if rec.Decisions != nil {
		c.decisions = rec.Decisions
	}

	if err := c.reapplyLastDecision(ctx, rec.LastDecided); err != nil {
		return err
	}

	if rec.Unresolved == nil {
		c.height.Store(rec.NextHeight)

		// Prepare runs before the PREPARED record is written, so a crash in
		// between can leave the resource holding the next height. No ACK was
		// sent for it, hence the coordinator cannot have committed it.
		if err := c.resource.Abort(ctx, rec.NextHeight); err != nil {
			return fmt.Errorf("release unlogged prepare at height %d: %w", rec.NextHeight, err)
		}

		return nil
	}

	c.height.Store(rec.Unresolved.Height)
	c.pendingPayload = rec.Unresolved.Payload

	if c.mode == twophase {
		c.restorePreparedAndPollDecision(rec.Unresolved)

		return nil
	}

	switch rec.Unresolved.Phase {
	case iowal.PhaseKeyPrepared:
		c.restoreThreePhasePreparedAndPollDecision(rec.Unresolved)
	case iowal.PhaseKeyPrecommit:
		c.restorePrecommitAndStartCommitTimer(rec.Unresolved)
	default:
		return fmt.Errorf("cannot resume transaction at height %d with unexpected WAL phase %q",
			rec.Unresolved.Height, rec.Unresolved.Phase)
	}

	return nil
}

// reapplyLastDecision repeats the newest durable outcome on the resource. A
// crash may land between the WAL decision record and the resource call; every
// older height was applied before the cohort moved past it.
func (c *Cohort) reapplyLastDecision(ctx context.Context, last *iowal.DecidedTransaction) error {
	if last == nil {
		return nil
	}

	switch last.Phase {
	case iowal.PhaseKeyCommit:
		walTx, err := iowal.Decode(last.Payload)
		if err != nil {
			return fmt.Errorf("decode committed tx at height %d: %w", last.Height, err)
		}

		if err := c.resource.Commit(ctx, dto.Tx{Height: last.Height, Key: walTx.Key, Value: walTx.Value}); err != nil {
			return fmt.Errorf("reapply commit at height %d: %w", last.Height, err)
		}
	case iowal.PhaseKeyAbort:
		if err := c.resource.Abort(ctx, last.Height); err != nil {
			return fmt.Errorf("reapply abort at height %d: %w", last.Height, err)
		}
	}

	return nil
}

// restorePreparedAndPollDecision restores PREPARED and starts background
// polling for the coordinator's decision.
func (c *Cohort) restorePreparedAndPollDecision(unresolved *iowal.UnresolvedTransaction) {
	c.phase = preparedStage
	go c.awaitDecision(unresolved.Height)

	slog.Warn("recovered in-doubt transaction, awaiting coordinator decision", "height", unresolved.Height)
}

// restoreThreePhasePreparedAndPollDecision restores 3PC PREPARED and starts
// background polling for the coordinator's decision.
func (c *Cohort) restoreThreePhasePreparedAndPollDecision(unresolved *iowal.UnresolvedTransaction) {
	c.phase = preparedStage
	go c.awaitDecision(unresolved.Height)

	slog.Warn("recovered prepared 3PC transaction, awaiting coordinator decision", "height", unresolved.Height)
}

// restorePrecommitAndStartCommitTimer restores PRECOMMIT and starts the
// background autocommit timer.
func (c *Cohort) restorePrecommitAndStartCommitTimer(unresolved *iowal.UnresolvedTransaction) {
	c.phase = precommitStage

	slog.Warn("recovered precommitted 3PC transaction, resuming commit timeout", "height", unresolved.Height)
	go c.handlePrecommitTimeout(unresolved.Height)
}

func (c *Cohort) resetPending() {
	c.pendingPayload = nil
}
