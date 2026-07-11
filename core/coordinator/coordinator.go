// Package coordinator implements the coordinator side of 2PC/3PC: it drives
// cohorts through propose/precommit/commit and collects their responses.
package coordinator

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"log/slog"

	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/events"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	iowal "github.com/vadiminshakov/committer/io/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// commitDeliveryAttempts caps synchronous commit retries; an unreachable
	// cohort learns the outcome later via termination protocol or catch-up.
	commitDeliveryAttempts = 3
	commitDeliveryBackoff  = 100 * time.Millisecond
	// maxCatchUpRounds bounds retries against a cohort that keeps NACKing
	// even after catch-up.
	maxCatchUpRounds = 3
)

//go:generate mockgen -destination=../../mocks/mock_wal.go -package=mocks . wal
type wal interface {
	Write(key string, value []byte) error
	Close() error
}

// StateStore defines the interface for persistent state storage.
//
//go:generate mockgen -destination=../../mocks/mock_state_store.go -package=mocks . StateStore
type StateStore interface {
	Put(key string, value []byte) error
	Close() error
}

type coordinator struct {
	wal            wal
	store          StateStore
	cohorts        map[string]*client.InternalCommitClient
	config         *config.Config
	commitType     pb.CommitType
	threePhase     bool
	height         atomic.Uint64
	pendingPayload []byte     // encoded payload of the current in-progress broadcast
	mu             sync.Mutex // serialize broadcast to keep height/WAL alignment
	decisions      map[uint64]string
	decisionsMu    sync.RWMutex // decisions are read by concurrent Decision requests
	emitter        events.Emitter
}

// SetEmitter injects an event emitter for dashboard visualization. Safe to call after New().
func (c *coordinator) SetEmitter(e events.Emitter) {
	if e == nil {
		e = events.NoopEmitter{}
	}
	c.emitter = e
}

// New creates a new coordinator instance with the specified configuration.
func New(conf *config.Config, wal wal, store StateStore) (*coordinator, error) {
	cohorts := make(map[string]*client.InternalCommitClient, len(conf.Cohorts))
	for _, f := range conf.Cohorts {
		cl, err := client.NewInternalClient(f)
		if err != nil {
			return nil, err
		}

		cohorts[f] = cl
	}

	threePhase := conf.CommitType == server.THREE_PHASE
	commitType := pb.CommitType_TWO_PHASE_COMMIT
	if threePhase {
		commitType = pb.CommitType_THREE_PHASE_COMMIT
	}

	return &coordinator{
		wal:        wal,
		store:      store,
		cohorts:    cohorts,
		config:     conf,
		commitType: commitType,
		threePhase: threePhase,
		decisions:  make(map[uint64]string),
		emitter:    events.NoopEmitter{},
	}, nil
}

// Recover applies WAL recovery state. A transaction stuck at prepared
// resolves as abort: no cohort was ever told to precommit. A 3PC
// transaction that reached precommit everywhere resolves as commit instead,
// since a precommitted cohort autocommits on its own timeout regardless.
func (c *coordinator) Recover(rec *iowal.RecoveryState) {
	c.decisionsMu.Lock()
	maps.Copy(c.decisions, rec.Decisions)
	c.decisionsMu.Unlock()

	if rec.Unresolved == nil {
		c.height.Store(rec.NextHeight)
		return
	}

	tx := rec.Unresolved
	if c.threePhase && tx.Phase == iowal.PhaseKeyPrecommit {
		c.resolveRecoveredAsCommit(tx)
		return
	}

	c.resolveRecoveredAsAbort(tx)
}

func (c *coordinator) resolveRecoveredAsAbort(tx *iowal.UnresolvedTransaction) {
	slog.Warn("resolving transaction using presumed-abort recovery",
		"height", tx.Height,
		"phase", tx.Phase,
	)
	if err := c.wal.Write(iowal.AbortKey(tx.Height), nil); err != nil {
		slog.Error("failed to write abort record during recovery", "height", tx.Height, "err", err)
	}
	c.recordDecision(tx.Height, iowal.PhaseKeyAbort)
	c.height.Store(tx.Height + 1)
}

// resolveRecoveredAsCommit closes out a 3PC transaction that had
// precommitted everywhere before the crash, matching the outcome cohorts
// may have already autocommitted on their own.
func (c *coordinator) resolveRecoveredAsCommit(tx *iowal.UnresolvedTransaction) {
	slog.Warn("resolving transaction using presumed-commit recovery",
		"height", tx.Height,
		"phase", tx.Phase,
	)

	if err := c.wal.Write(iowal.CommitKey(tx.Height), tx.Payload); err != nil {
		slog.Error("failed to write commit record during recovery", "height", tx.Height, "err", err)
	}
	c.recordDecision(tx.Height, iowal.PhaseKeyCommit)

	c.pendingPayload = tx.Payload
	if err := c.persistMessage(); err != nil {
		slog.Error("CRITICAL: committed but not applied to local store, will be replayed from WAL on restart", "height", tx.Height, "err", err)
	}
	c.pendingPayload = nil

	c.height.Store(tx.Height + 1)

	// deliver commit to any cohort still waiting, without blocking startup
	// on unreachable ones
	ctx := context.Background()
	for name, cohort := range c.cohorts {
		go c.deliverCommit(ctx, name, cohort, tx.Height)
	}
}

// Broadcast runs the 2PC/3PC protocol for a transaction: propose, precommit
// (3PC only), commit, then persist.
func (c *coordinator) Broadcast(ctx context.Context, req dto.BroadcastRequest) (*dto.BroadcastResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := c.height.Load()

	slog.Info("Proposing key", "key", req.Key)
	c.emitter.Emit(events.Event{Kind: events.EvCoordPropose, Key: req.Key, Height: currentHeight})
	if err := c.propose(ctx, req); err != nil {
		return nackResponse(err, "failed to send propose")
	}

	if c.threePhase {
		slog.Info("Precommitting key", "key", req.Key)
		c.emitter.Emit(events.Event{Kind: events.EvCoordPrecommit, Key: req.Key, Height: currentHeight})
		if err := c.preCommit(ctx); err != nil {
			return nackResponse(err, "failed to send precommit")
		}
	}

	slog.Info("Committing key", "key", req.Key)
	c.emitter.Emit(events.Event{Kind: events.EvCoordCommit, Key: req.Key, Height: currentHeight})
	if err := c.commit(ctx); err != nil {
		return nackResponse(err, "failed to commit")
	}

	slog.Info("coordinator committed key", "key", req.Key)

	newHeight := c.height.Add(1)
	c.emitter.Emit(events.Event{Kind: events.EvCoordCommit, Key: req.Key, Height: newHeight, Result: "ok"})
	return &dto.BroadcastResponse{Type: dto.ResponseTypeAck, Height: newHeight}, nil
}

func (c *coordinator) propose(ctx context.Context, req dto.BroadcastRequest) error {
	for name, cohort := range c.cohorts {
		if err := c.sendProposal(ctx, cohort, name, req, c.commitType); err != nil {
			return err
		}
	}

	currentHeight := c.height.Load()

	// record Prepared so the payload survives a crash
	ptx := iowal.Tx{Key: req.Key, Value: req.Value}
	pbBytes, err := iowal.Encode(ptx)
	if err != nil {
		return err
	}
	if err := c.wal.Write(iowal.PreparedKey(currentHeight), pbBytes); err != nil {
		return err
	}
	c.pendingPayload = pbBytes
	return nil
}

func (c *coordinator) sendProposal(ctx context.Context, cohort *client.InternalCommitClient, name string, req dto.BroadcastRequest, commitType pb.CommitType) error {
	var (
		resp          *pb.Response
		err           error
		catchUpRounds int
	)

	for {
		currentHeight := c.height.Load()
		resp, err = cohort.Propose(ctx, &pb.ProposeRequest{
			Key:        req.Key,
			Value:      req.Value,
			CommitType: commitType,
			Index:      currentHeight,
		})

		if err == nil && resp != nil && resp.Type == pb.Type_ACK {
			c.emitter.Emit(events.Event{Kind: events.EvCoordPropose, Cohort: name, Height: currentHeight, Result: "ok"})
			break // success
		}

		// cohort lags behind: replay the decisions it missed, then retry
		if err == nil && resp != nil && resp.Index < currentHeight && catchUpRounds < maxCatchUpRounds {
			catchUpRounds++
			if cerr := c.catchUpCohort(ctx, name, cohort, resp.Index, currentHeight); cerr == nil {
				continue
			} else {
				slog.Error("failed to catch up cohort", "cohort", name, "err", cerr)
			}
		}

		if err != nil {
			c.emitter.Emit(events.Event{Kind: events.EvCoordPropose, Cohort: name, Height: currentHeight, Result: "nack"})
			// send abort to all cohorts on error
			c.abort(ctx, fmt.Sprintf("node %s rejected proposed msg: %v", name, err))
			return fmt.Errorf("node %s rejected proposed msg: %w", name, err)
		}

		c.emitter.Emit(events.Event{Kind: events.EvCoordPropose, Cohort: name, Height: currentHeight, Result: "nack"})
		// send abort to all cohorts on NACK
		c.abort(ctx, fmt.Sprintf("cohort %s sent NACK for propose", name))
		return fmt.Errorf("cohort %s not acknowledged msg %v", name, req)
	}
	return nil
}

// catchUpCohort replays recorded decisions to a lagging cohort so it can
// resolve old heights and accept new proposals.
func (c *coordinator) catchUpCohort(ctx context.Context, name string, cohort *client.InternalCommitClient, from, to uint64) error {
	slog.Warn("catching up lagging cohort", "cohort", name, "from", from, "to", to)

	for h := from; h < to; h++ {
		switch c.decisionFor(h) {
		case iowal.PhaseKeyCommit:
			resp, err := cohort.Commit(ctx, &pb.CommitRequest{Index: h})
			if err != nil {
				return fmt.Errorf("catch-up commit at height %d: %w", h, err)
			}
			if !isAck(resp) {
				return fmt.Errorf("catch-up commit at height %d: NACK", h)
			}
		case iowal.PhaseKeyAbort:
			if _, err := cohort.Abort(ctx, &dto.AbortRequest{Height: h, Reason: "catch-up: height was aborted"}); err != nil {
				return fmt.Errorf("catch-up abort at height %d: %w", h, err)
			}
		default:
			return fmt.Errorf("no recorded decision for height %d", h)
		}
	}

	return nil
}

func (c *coordinator) preCommit(ctx context.Context) error {
	currentHeight := c.height.Load()
	for name, cohort := range c.cohorts {
		resp, err := cohort.Precommit(ctx, &pb.PrecommitRequest{Index: currentHeight})
		if err != nil {
			c.emitter.Emit(events.Event{Kind: events.EvCoordPrecommit, Cohort: name, Height: currentHeight, Result: "nack"})
			c.abort(ctx, fmt.Sprintf("cohort %s precommit error: %v", name, err))
			return status.Error(codes.FailedPrecondition, "cohort not acknowledged msg")
		}
		if !isAck(resp) {
			c.emitter.Emit(events.Event{Kind: events.EvCoordPrecommit, Cohort: name, Height: currentHeight, Result: "nack"})
			c.abort(ctx, fmt.Sprintf("cohort %s sent NACK for precommit", name))
			return status.Error(codes.FailedPrecondition, "cohort not acknowledged msg")
		}
		c.emitter.Emit(events.Event{Kind: events.EvCoordPrecommit, Cohort: name, Height: currentHeight, Result: "ok"})
	}

	// record precommit once every cohort has acked: past this point a cohort
	// autocommits on its own timeout, so recovery must not presume abort
	if err := c.wal.Write(iowal.PrecommitKey(currentHeight), c.pendingPayload); err != nil {
		return status.Errorf(codes.Internal, "failed to write precommit record: %v", err)
	}

	return nil
}

func (c *coordinator) commit(ctx context.Context) error {
	currentHeight := c.height.Load()

	if err := c.wal.Write(iowal.CommitKey(currentHeight), c.pendingPayload); err != nil {
		// no commit record on disk: the transaction resolves as abort
		c.abort(ctx, fmt.Sprintf("failed to write commit record: %v", err))
		return status.Errorf(codes.Internal, "failed to write commit record: %v", err)
	}
	c.recordDecision(currentHeight, iowal.PhaseKeyCommit)

	if err := c.persistMessage(); err != nil {
		slog.Error("CRITICAL: committed but not applied to local store, will be replayed from WAL on restart", "err", err)
	}
	c.pendingPayload = nil

	for name, cohort := range c.cohorts {
		c.deliverCommit(ctx, name, cohort, currentHeight)
	}

	return nil
}

// deliverCommit pushes the commit decision to a cohort, retrying transient
// failures. Giving up is safe: the cohort resolves via decision requests.
func (c *coordinator) deliverCommit(ctx context.Context, name string, cohort *client.InternalCommitClient, height uint64) {
	for attempt := range commitDeliveryAttempts {
		if attempt > 0 {
			time.Sleep(commitDeliveryBackoff)
		}

		resp, err := cohort.Commit(ctx, &pb.CommitRequest{Index: height})
		if err == nil && isAck(resp) {
			c.emitter.Emit(events.Event{Kind: events.EvCoordCommit, Cohort: name, Height: height, Result: "ok"})
			return
		}
		slog.Warn("commit delivery failed", "cohort", name, "height", height, "attempt", attempt+1, "err", err)
	}

	c.emitter.Emit(events.Event{Kind: events.EvCoordCommit, Cohort: name, Height: height, Result: "nack"})
	slog.Error("cohort did not acknowledge commit; it will learn the decision via the termination protocol",
		"cohort", name, "height", height)
}

func isAck(resp *pb.Response) bool {
	return resp != nil && resp.Type == pb.Type_ACK
}

// Height returns the current transaction height.
func (c *coordinator) Height() uint64 {
	return c.height.Load()
}

// SetHeight initializes coordinator height during recovery.
func (c *coordinator) SetHeight(height uint64) {
	c.height.Store(height)
}

// Decision returns the recorded outcome for the given height. Unknown means
// the height is not yet decided (or predates WAL retention).
func (c *coordinator) Decision(height uint64) dto.Outcome {
	switch c.decisionFor(height) {
	case iowal.PhaseKeyCommit:
		return dto.OutcomeCommit
	case iowal.PhaseKeyAbort:
		return dto.OutcomeAbort
	default:
		return dto.OutcomeUnknown
	}
}

func (c *coordinator) decisionFor(height uint64) string {
	c.decisionsMu.RLock()
	defer c.decisionsMu.RUnlock()
	return c.decisions[height]
}

func (c *coordinator) recordDecision(height uint64, phase string) {
	c.decisionsMu.Lock()
	c.decisions[height] = phase
	c.decisionsMu.Unlock()
}

// abort resolves the current height as aborted and notifies all cohorts.
func (c *coordinator) abort(ctx context.Context, reason string) {
	currentHeight := c.height.Load()
	slog.Warn("Aborting transaction", "height", currentHeight, "reason", reason)
	c.emitter.Emit(events.Event{Kind: events.EvCoordAbort, Height: currentHeight, Message: reason})

	if err := c.wal.Write(iowal.AbortKey(currentHeight), nil); err != nil {
		// absence of a commit record already means abort, so keep going
		slog.Error("Failed to write abort to WAL", "err", err)
	}
	c.recordDecision(currentHeight, iowal.PhaseKeyAbort)
	c.pendingPayload = nil
	c.height.Add(1)

	// fire-and-forget: a cohort that misses this learns the outcome via the
	// termination protocol; detach from the request context so the delivery
	// survives the client RPC ending
	abortCtx := context.WithoutCancel(ctx)
	for name, cohort := range c.cohorts {
		go func(name string, cohort *client.InternalCommitClient) {
			if _, err := cohort.Abort(abortCtx, &dto.AbortRequest{Height: currentHeight, Reason: reason}); err != nil {
				slog.Error("Failed to send abort to cohort", "cohort", name, "err", err)
			}
		}(name, cohort)
	}
}

func nackResponse(err error, msg string) (*dto.BroadcastResponse, error) {
	return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, errors.Wrap(err, msg)
}

func (c *coordinator) persistMessage() error {
	if c.pendingPayload == nil {
		return status.Error(codes.Internal, "can't find msg in wal")
	}

	walTx, err := iowal.Decode(c.pendingPayload)
	if err != nil {
		return status.Error(codes.Internal, "failed to decode tx")
	}

	return c.store.Put(walTx.Key, walTx.Value)
}
