package coordinator

import (
	"context"
	"fmt"
	"github.com/openzipkin/zipkin-go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/committer/io/trace"
	"github.com/vadiminshakov/committer/voteslog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync/atomic"
	"time"
)

type coordinatorImpl struct {
	vlog      voteslog.Log
	database  db.Repository
	followers map[string]*client.CommitClient
	tracer    *zipkin.Tracer
	config    *config.Config
	height    uint64
}

func New(conf *config.Config, vlog voteslog.Log, database db.Repository) (*coordinatorImpl, error) {
	var tracer *zipkin.Tracer
	var err error
	if conf.WithTrace {
		// get Zipkin tracer
		tracer, err = trace.Tracer(fmt.Sprintf("%s:%s", conf.Role, conf.Nodeaddr), conf.Nodeaddr)
		if err != nil {
			return nil, err
		}
	}

	flwrs := make(map[string]*client.CommitClient, len(conf.Followers))
	for _, f := range conf.Followers {
		client, err := client.New(f, tracer)
		if err != nil {
			return nil, err
		}
		flwrs[f] = client
	}

	return &coordinatorImpl{
		followers: flwrs,
		tracer:    tracer,
		height:    0,
		vlog:      vlog,
		database:  database,
		config:    conf,
	}, nil
}

func (c *coordinatorImpl) Broadcast(ctx context.Context, req dto.BroadcastRequest) (*dto.BroadcastResponse, error) {
	var (
		err  error
		span zipkin.Span
	)

	if c.tracer != nil {
		span, ctx = c.tracer.StartSpanFromContext(ctx, "PutHandle")
		defer span.Finish()
	}

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
	key, value, ok := c.vlog.Get(c.height)
	if !ok {
		return nil, status.Error(codes.Internal, "can't to find msg in the coordinator's cache")
	}
	if err = c.database.Put(key, value); err != nil {
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
	var span zipkin.Span
	for nodename, follower := range c.followers {
		if c.tracer != nil {
			span, ctx = c.tracer.StartSpanFromContext(ctx, "Propose")
		}
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
			if c.tracer != nil && span != nil {
				span.Finish()
			}
			if err != nil {
				log.Errorf(err.Error())
				isAccepted = false
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
	if err := c.vlog.Set(c.height, req.Key, req.Value); err != nil {
		return errors.Wrap(err, "failed to save msg in the coordinator's log")
	}
	c.vlog.SetVotes(c.height, votes)

	return nil
}

func (c *coordinatorImpl) preCommit(ctx context.Context, req dto.BroadcastRequest) error {
	if c.config.CommitType != server.THREE_PHASE {
		return nil
	}

	var span zipkin.Span
	for _, follower := range c.followers {
		if c.tracer != nil {
			span, ctx = c.tracer.StartSpanFromContext(ctx, "Precommit")
		}

		votes := votesToProto(c.vlog.GetVotes(c.height))
		resp, err := follower.Precommit(ctx, &pb.PrecommitRequest{Index: c.height, Votes: votes})
		if c.tracer != nil && span != nil {
			span.Finish()
		}
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
	var span zipkin.Span
	for _, follower := range c.followers {
		if c.tracer != nil {
			span, ctx = c.tracer.StartSpanFromContext(ctx, "Commit")
		}
		r, err := follower.Commit(ctx, &pb.CommitRequest{Index: c.height})
		if c.tracer != nil && span != nil {
			span.Finish()
		}
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

func votesToProto(votes []*dto.Vote) []*pb.Vote {
	pbvotes := make([]*pb.Vote, 0, len(votes))
	for _, v := range votes {
		pbvotes = append(pbvotes, &pb.Vote{
			Node:       v.Node,
			IsAccepted: v.IsAccepted,
		})
	}

	return pbvotes
}
