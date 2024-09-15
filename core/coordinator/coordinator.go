package coordinator

import (
	"bytes"
	"context"
	"encoding/gob"
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

const (
	votesPrefix = "votes"
)

type wal interface {
	Set(index uint64, key string, value []byte) error
	Get(index uint64) (string, []byte, bool)
	Close() error
}

type coordinatorImpl struct {
	walVotes  wal
	walMsgs   wal
	database  db.Repository
	followers map[string]*client.CommitClient
	config    *config.Config
	height    uint64
}

func New(conf *config.Config, walMsgs wal, walVotes wal, database db.Repository) (*coordinatorImpl, error) {
	flwrs := make(map[string]*client.CommitClient, len(conf.Followers))
	for _, f := range conf.Followers {
		client, err := client.New(f)
		if err != nil {
			return nil, err
		}
		flwrs[f] = client
	}

	return &coordinatorImpl{
		followers: flwrs,
		height:    0,
		walMsgs:   walMsgs,
		walVotes:  walVotes,
		database:  database,
		config:    conf,
	}, nil
}

func (c *coordinatorImpl) Broadcast(ctx context.Context, req dto.BroadcastRequest) (*dto.BroadcastResponse, error) {
	// propose
	log.Infof("propose key %s", req.Key)
	if err := c.propose(ctx, req); err != nil {
		return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, errors.Wrap(err, "failed to send propose")
	}

	// precommit phase only for three-phase mode
	log.Infof("precommit key %s", req.Key)
	if err := c.preCommit(ctx, req); err != nil {
		return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, errors.Wrap(err, "failed to send precommit")
	}

	// commit
	log.Infof("commit %s", req.Key)
	if err := c.commit(ctx); err != nil {
		s, ok := status.FromError(err)
		if !ok {
			return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, fmt.Errorf("failed to extract grpc status code from err: %s", err)
		}
		if s.Code() == codes.AlreadyExists {
			return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, nil
		}
		return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, errors.Wrap(err, "failed to send commit")
	}

	log.Infof("coordinator got ack from all cohorts, committed key %s", req.Key)

	// the coordinator got all the answers, so it's time to persist msg
	key, value, ok := c.walMsgs.Get(c.height)
	if !ok {
		return nil, status.Error(codes.Internal, "can't to find msg in the coordinator's cache")
	}
	if err := c.database.Put(key, value); err != nil {
		return &dto.BroadcastResponse{Type: dto.ResponseTypeNack}, status.Error(codes.Internal, "failed to save msg on coordinator")
	}

	// increase height for next round
	atomic.AddUint64(&c.height, 1)

	return &dto.BroadcastResponse{Type: dto.ResponseTypeAck, Index: c.height}, nil
}

func (c *coordinatorImpl) propose(ctx context.Context, req dto.BroadcastRequest) error {
	var ctype pb.CommitType
	if c.config.CommitType == server.THREE_PHASE {
		ctype = pb.CommitType_THREE_PHASE_COMMIT
	} else {
		ctype = pb.CommitType_TWO_PHASE_COMMIT
	}

	votes := make([]*dto.Vote, 0, len(c.followers))
	for nodename, follower := range c.followers {
		var (
			resp *pb.Response
			err  error
		)
		isAccepted := true

		for resp == nil || resp != nil && resp.Type == pb.Type_NACK {
			resp, err = follower.Propose(ctx, &pb.ProposeRequest{Key: req.Key,
				Value:      req.Value,
				CommitType: ctype,
				Index:      c.height})
			if err != nil {
				log.Errorf(err.Error())
				isAccepted = false
				return fmt.Errorf("node %s is not accepted proposed msg", nodename)
			}

			if !isAccepted {
				return fmt.Errorf("node %s is not accepted proposed msg", nodename)
			}

			votes = append(votes, &dto.Vote{
				Node:       nodename,
				IsAccepted: isAccepted,
			})

			if resp == nil || resp.Type != pb.Type_ACK {
				log.Warnf("follower %s not acknowledged msg %v", nodename, req)
			}

			if resp != nil && resp.Index > c.height {
				log.Warnf("сoordinator has stale height [%d], update to [%d] and try to send again", c.height, resp.Index)
				c.height = resp.Index
				votes = votes[:0]
			}
		}
	}
	if err := c.walMsgs.Set(c.height, req.Key, req.Value); err != nil {
		return errors.Wrap(err, "failed to save msg in the coordinator's log")
	}

	var bufVotes bytes.Buffer
	encVotes := gob.NewEncoder(&bufVotes)
	if err := encVotes.Encode(votes); err != nil {
		return err
	}

	c.walVotes.Set(c.height, votesPrefix, bufVotes.Bytes())

	return nil
}

func (c *coordinatorImpl) preCommit(ctx context.Context, req dto.BroadcastRequest) error {
	if c.config.CommitType != server.THREE_PHASE {
		return nil
	}

	for _, follower := range c.followers {
		resp, err := follower.Precommit(ctx, &pb.PrecommitRequest{Index: c.height})
		if err != nil {
			return err
		}
		if resp.Type != pb.Type_ACK {
			return status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}

	// block after precommit if needs
	{
		blockPrecommit, ok := ctx.Value("block").(string)
		blockt, okblocktime := ctx.Value("blocktime").(string)
		if ok && okblocktime {
			if blockPrecommit == "precommit" {
				dur, err := time.ParseDuration(blockt)
				if err != nil {
					return err
				}
				time.Sleep(dur)
			}
		}
	}

	return nil
}

func (c *coordinatorImpl) commit(ctx context.Context) error {
	for _, follower := range c.followers {
		r, err := follower.Commit(ctx, &pb.CommitRequest{Index: c.height})
		if err != nil {
			return err
		}
		if r.Type != pb.Type_ACK {
			return status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}

	return nil
}

func (c *coordinatorImpl) Height() uint64 {
	return c.height
}
