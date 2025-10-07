package coordinator

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:generate mockgen -destination=../../mocks/mock_wal.go -package=mocks . wal
type wal interface {
	Write(index uint64, key string, value []byte) error
	WriteTombstone(index uint64) error
	Get(index uint64) (string, []byte, error)
	Close() error
}

//go:generate mockgen -destination=../../mocks/mock_repository.go -package=mocks . Repository
type Repository interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Close() error
}

type coordinator struct {
	wal      wal
	database Repository
	cohorts  map[string]*client.InternalCommitClient
	config   *config.Config
	height   uint64
}

func New(conf *config.Config, wal wal, database Repository) (*coordinator, error) {
	cohorts := make(map[string]*client.InternalCommitClient, len(conf.Cohorts))
	for _, f := range conf.Cohorts {
		cl, err := client.NewInternalClient(f)
		if err != nil {
			return nil, err
		}

		cohorts[f] = cl
	}

	return &coordinator{
		wal:      wal,
		database: database,
		cohorts:  cohorts,
		config:   conf,
	}, nil
}

func (c *coordinator) Broadcast(ctx context.Context, req dto.BroadcastRequest) (*dto.BroadcastResponse, error) {
	log.Infof("Proposing key %s", req.Key)
	if err := c.propose(ctx, req); err != nil {
		return nackResponse(err, "failed to send propose")
	}

	if c.config.CommitType == server.THREE_PHASE {
		log.Infof("Precommitting key %s", req.Key)
		if err := c.preCommit(ctx); err != nil {
			return nackResponse(err, "failed to send precommit")
		}
	}

	log.Infof("Committing key %s", req.Key)
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

	log.Infof("coordinator committed key %s", req.Key)
	if err := c.persistMessage(); err != nil {
		return nackResponse(err, "failed to persist message")
	}

	newHeight := atomic.AddUint64(&c.height, 1)
	return &dto.BroadcastResponse{Type: dto.ResponseTypeAck, Index: newHeight}, nil
}

func (c *coordinator) propose(ctx context.Context, req dto.BroadcastRequest) error {
	commitType := pb.CommitType_TWO_PHASE_COMMIT
	if c.config.CommitType == server.THREE_PHASE {
		commitType = pb.CommitType_THREE_PHASE_COMMIT
	}

	for name, cohort := range c.cohorts {
		if err := c.sendProposal(ctx, cohort, name, req, commitType); err != nil {
			return err
		}
	}

	currentHeight := atomic.LoadUint64(&c.height)
	return c.wal.Write(currentHeight, req.Key, req.Value)
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

		if err == nil && resp.Type == pb.Type_ACK {
			break // success
		}

		// if cohort has bigger height, update coordinator's height and retry
		if resp != nil && resp.Index > currentHeight {
			c.syncHeight(resp.Index)
			continue
		}
		if err != nil {
			// send abort to all cohorts on error
			c.abort(ctx, fmt.Sprintf("node %s rejected proposed msg: %v", name, err))
			return fmt.Errorf("node %s rejected proposed msg: %w", name, err)
		}

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
			c.abort(ctx, fmt.Sprintf("cohort %s precommit error: %v", name, err))
			return status.Error(codes.FailedPrecondition, "cohort not acknowledged msg")
		}
		if resp.Type != pb.Type_ACK {
			c.abort(ctx, fmt.Sprintf("cohort %s sent NACK for precommit", name))
			return status.Error(codes.FailedPrecondition, "cohort not acknowledged msg")
		}
	}

	return nil
}

func (c *coordinator) commit(ctx context.Context) error {
	currentHeight := atomic.LoadUint64(&c.height)
	for _, cohort := range c.cohorts {
		resp, err := cohort.Commit(ctx, &pb.CommitRequest{Index: currentHeight})
		if err != nil {
			return err
		}

		if resp.Type != pb.Type_ACK {
			return status.Error(codes.FailedPrecondition, "cohort not acknowledged msg")
		}
	}

	return nil
}

func (c *coordinator) persistMessage() error {
	currentHeight := atomic.LoadUint64(&c.height)
	key, value, err := c.wal.Get(currentHeight)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to read msg at height %d from wal: %v", currentHeight, err))
	}
	if value == nil {
		return status.Error(codes.Internal, "can't find msg in wal")
	}

	return c.database.Put(key, value)
}

// syncHeight atomically updates coordinator height to match cohort height if needed
func (c *coordinator) syncHeight(cohortHeight uint64) {
	for {
		currentHeight := atomic.LoadUint64(&c.height)
		if cohortHeight <= currentHeight {
			return // height is already up to date
		}

		if atomic.CompareAndSwapUint64(&c.height, currentHeight, cohortHeight) {
			log.Warnf("Updating coordinator height: %d -> %d", currentHeight, cohortHeight)
			return
		}
	}
}

func (c *coordinator) Height() uint64 {
	return atomic.LoadUint64(&c.height)
}

// abort sends abort requests to all cohorts in a fire-and-forget manner
func (c *coordinator) abort(ctx context.Context, reason string) {
	currentHeight := atomic.LoadUint64(&c.height)
	log.Warnf("Aborting transaction at height %d: %s", currentHeight, reason)

	for name, cohort := range c.cohorts {
		go func(name string, cohort *client.InternalCommitClient) {
			if _, err := cohort.Abort(ctx, &dto.AbortRequest{Height: currentHeight, Reason: reason}); err != nil {
				log.Errorf("Failed to send abort to cohort %s: %v", name, err)
			}
		}(name, cohort)
	}
}

func nackResponse(err error, msg string) (*dto.BroadcastResponse, error) {
	return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, errors.Wrap(err, msg)
}
