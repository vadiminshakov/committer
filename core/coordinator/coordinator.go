package coordinator

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync/atomic"
	"time"
)

type wal interface {
	Write(index uint64, key string, value []byte) error
	Get(index uint64) (string, []byte, bool)
	Close() error
}

type coordinator struct {
	wal       wal
	database  db.Repository
	followers map[string]*client.InternalCommitClient
	config    *config.Config
	height    uint64
}

func New(conf *config.Config, wal wal, database db.Repository) (*coordinator, error) {
	followers := make(map[string]*client.InternalCommitClient, len(conf.Followers))
	for _, f := range conf.Followers {
		cl, err := client.NewInternalClient(f)
		if err != nil {
			return nil, err
		}

		followers[f] = cl
	}

	return &coordinator{
		wal:       wal,
		database:  database,
		followers: followers,
		config:    conf,
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

	atomic.AddUint64(&c.height, 1)
	return &dto.BroadcastResponse{Type: dto.ResponseTypeAck, Index: c.height}, nil
}

func (c *coordinator) propose(ctx context.Context, req dto.BroadcastRequest) error {
	commitType := pb.CommitType_TWO_PHASE_COMMIT
	if c.config.CommitType == server.THREE_PHASE {
		commitType = pb.CommitType_THREE_PHASE_COMMIT
	}

	for name, follower := range c.followers {
		if err := c.sendProposal(ctx, follower, name, req, commitType); err != nil {
			return err
		}
	}

	return c.wal.Write(c.height, req.Key, req.Value)
}

func (c *coordinator) sendProposal(ctx context.Context, follower *client.InternalCommitClient, name string, req dto.BroadcastRequest, commitType pb.CommitType) error {
	for {
		resp, err := follower.Propose(ctx, &pb.ProposeRequest{
			Key:        req.Key,
			Value:      req.Value,
			CommitType: commitType,
			Index:      c.height,
		})
		if err != nil {
			return fmt.Errorf("node %s rejected proposed msg", name)
		}

		if resp != nil && resp.Type != pb.Type_ACK {
			if resp.Index > c.height {
				log.Warnf("Updating stale height: %d -> %d", c.height, resp.Index)
				c.height = resp.Index

				continue
			}

			return fmt.Errorf("follower %s not acknowledged msg %v", name, req)
		}

		break
	}

	return nil
}

func (c *coordinator) preCommit(ctx context.Context) error {
	for _, follower := range c.followers {
		resp, err := follower.Precommit(ctx, &pb.PrecommitRequest{Index: c.height})
		if err != nil || resp.Type != pb.Type_ACK {
			return status.Error(codes.FailedPrecondition, "follower not acknowledged msg")
		}
	}

	return c.maybeBlock(ctx, "precommit")
}

func (c *coordinator) commit(ctx context.Context) error {
	for _, follower := range c.followers {
		resp, err := follower.Commit(ctx, &pb.CommitRequest{Index: c.height})
		if err != nil {
			return err
		}

		if resp.Type != pb.Type_ACK {
			return status.Error(codes.FailedPrecondition, "follower not acknowledged msg")
		}
	}

	return nil
}

func (c *coordinator) persistMessage() error {
	key, value, ok := c.wal.Get(c.height)
	if !ok {
		return status.Error(codes.Internal, "can't find msg in wal")
	}

	return c.database.Put(key, value)
}

func (c *coordinator) maybeBlock(ctx context.Context, phase string) error {
	block, _ := ctx.Value("block").(string)
	blockTime, _ := ctx.Value("blocktime").(string)

	if block == phase {
		dur, err := time.ParseDuration(blockTime)
		if err != nil {
			return err
		}
		time.Sleep(dur)
	}

	return nil
}

func (c *coordinator) Height() uint64 {
	return c.height
}

func nackResponse(err error, msg string) (*dto.BroadcastResponse, error) {
	return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, errors.Wrap(err, msg)
}
