// Package coordinator implements the coordinator role in distributed atomic commit protocols.
// The coordinator is responsible for managing the atomic commit process by
// sending prepare requests to cohorts and collecting their responses.
package coordinator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"log/slog"

	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/tui/events"
	iowal "github.com/vadiminshakov/committer/io/wal"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	height         uint64
	pendingPayload []byte     // encoded payload of the current in-progress broadcast
	mu             sync.Mutex // serialize broadcast to keep height/WAL alignment
	emitter        events.Emitter
}

// SetEmitter injects an event emitter for TUI visualization. Safe to call after New().
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
		emitter:    events.NoopEmitter{},
	}, nil
}

// Broadcast executes the complete distributed consensus algorithm (2PC or 3PC) for a transaction.
// It runs through all phases: propose, precommit (if 3PC), commit, and persistence.
func (c *coordinator) Broadcast(ctx context.Context, req dto.BroadcastRequest) (*dto.BroadcastResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentHeight := atomic.LoadUint64(&c.height)

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
		s, ok := status.FromError(err)
		if !ok {
			return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, fmt.Errorf("failed to extract grpc status code from err: %s", err)
		}
		if s.Code() == codes.AlreadyExists {
			return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, nil
		}
		return nackResponse(err, "failed to send commit")
	}

	slog.Info("coordinator committed key", "key", req.Key)

	newHeight := atomic.AddUint64(&c.height, 1)
	c.emitter.Emit(events.Event{Kind: events.EvCoordCommit, Key: req.Key, Height: newHeight, Result: "ok"})
	return &dto.BroadcastResponse{Type: dto.ResponseTypeAck, Height: newHeight}, nil
}

func (c *coordinator) propose(ctx context.Context, req dto.BroadcastRequest) error {
	for name, cohort := range c.cohorts {
		if err := c.sendProposal(ctx, cohort, name, req, c.commitType); err != nil {
			return err
		}
	}

	currentHeight := atomic.LoadUint64(&c.height)

	// write Prepared to WAL to persist payload
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
		resp *pb.Response
		err  error
	)

	for {
		currentHeight := atomic.LoadUint64(&c.height)
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

		// if cohort has bigger height, update coordinator's height and retry
		if resp != nil && resp.Index > currentHeight {
			c.syncHeight(resp.Index)
			continue
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

func (c *coordinator) preCommit(ctx context.Context) error {
	currentHeight := atomic.LoadUint64(&c.height)
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

	return nil
}

func (c *coordinator) commit(ctx context.Context) error {
	currentHeight := atomic.LoadUint64(&c.height)
	var errs []string
	for name, cohort := range c.cohorts {
		resp, err := cohort.Commit(ctx, &pb.CommitRequest{Index: currentHeight})
		if err != nil {
			c.emitter.Emit(events.Event{Kind: events.EvCoordCommit, Cohort: name, Height: currentHeight, Result: "nack"})
			errs = append(errs, fmt.Sprintf("%s: %v", name, err))
			continue
		}
		if !isAck(resp) {
			c.emitter.Emit(events.Event{Kind: events.EvCoordCommit, Cohort: name, Height: currentHeight, Result: "nack"})
			errs = append(errs, fmt.Sprintf("%s: NACK", name))
			continue
		}
		c.emitter.Emit(events.Event{Kind: events.EvCoordCommit, Cohort: name, Height: currentHeight, Result: "ok"})
	}

	if len(errs) > 0 {
		return status.Error(codes.FailedPrecondition, "cohort not acknowledged msg: "+strings.Join(errs, "; "))
	}

	// Persist Decision (Commit) and Apply
	// 1. write commit
	if err := c.wal.Write(iowal.CommitKey(currentHeight), c.pendingPayload); err != nil {
		return status.Errorf(codes.Internal, "failed to write commit val: %v", err)
	}

	// 2. apply
	walTx, err := iowal.Decode(c.pendingPayload)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to decode tx: %v", err)
	}
	if err := c.store.Put(walTx.Key, walTx.Value); err != nil {
		slog.Error("failed to apply to store", "err", err)
		return err // committed but not applied? critical error
	}
	c.pendingPayload = nil

	return nil
}

func isAck(resp *pb.Response) bool {
	return resp != nil && resp.Type == pb.Type_ACK
}

// syncHeight atomically updates coordinator height to match cohort height if needed
func (c *coordinator) syncHeight(cohortHeight uint64) {
	for {
		currentHeight := atomic.LoadUint64(&c.height)
		if cohortHeight <= currentHeight {
			return // height is already up to date
		}

		if atomic.CompareAndSwapUint64(&c.height, currentHeight, cohortHeight) {
			slog.Warn("Updating coordinator height", "from", currentHeight, "to", cohortHeight)
			c.emitter.Emit(events.Event{Kind: events.EvCoordHeightSync, Height: cohortHeight})
			return
		}
	}
}

// Height returns the current transaction height.
func (c *coordinator) Height() uint64 {
	return atomic.LoadUint64(&c.height)
}

// SetHeight initializes coordinator height during recovery.
func (c *coordinator) SetHeight(height uint64) {
	atomic.StoreUint64(&c.height, height)
}

// abort sends abort requests to all cohorts in a fire-and-forget manner
func (c *coordinator) abort(ctx context.Context, reason string) {
	currentHeight := atomic.LoadUint64(&c.height)
	slog.Warn("Aborting transaction", "height", currentHeight, "reason", reason)
	c.emitter.Emit(events.Event{Kind: events.EvCoordAbort, Height: currentHeight, Message: reason})

	if err := c.wal.Write(iowal.AbortKey(currentHeight), nil); err != nil {
		slog.Error("Failed to write abort to WAL", "err", err)
	}

	for name, cohort := range c.cohorts {
		go func(name string, cohort *client.InternalCommitClient) {
			if _, err := cohort.Abort(ctx, &dto.AbortRequest{Height: currentHeight, Reason: reason}); err != nil {
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
