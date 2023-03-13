package coordinator

import (
	"context"
	"fmt"
	"github.com/openzipkin/zipkin-go"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/cache"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/entity"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/committer/io/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"sync/atomic"
	"time"
)

type coordinatorImpl struct {
	followers map[string]*client.CommitClient
	tracer    *zipkin.Tracer
	config    *config.Config
	height    uint64
	nodeCache *cache.Cache
	database  db.Repository
}

func New(conf *config.Config, nodeCache *cache.Cache, database db.Repository) (*coordinatorImpl, error) {
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
		nodeCache: nodeCache,
		database:  database,
		config:    conf,
	}, nil
}

func (c *coordinatorImpl) Broadcast(ctx context.Context, req entity.BroadcastRequest) (*entity.BroadcastResponse, error) {
	var blocktime time.Duration
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		blockcommitdur, found := md["blockcommit"]
		if found {
			d, err := time.ParseDuration(blockcommitdur[0])
			if err != nil {
				return nil, err
			}
			blocktime = d
		}
	}

	var (
		err  error
		span zipkin.Span
	)

	if c.tracer != nil {
		span, ctx = c.tracer.StartSpanFromContext(ctx, "PutHandle")
		defer span.Finish()
	}

	var ctype pb.CommitType
	if c.config.CommitType == server.THREE_PHASE {
		ctype = pb.CommitType_THREE_PHASE_COMMIT
	} else {
		ctype = pb.CommitType_TWO_PHASE_COMMIT
	}

	// propose
	log.Infof("propose key %s", req.Key)
	votes := make([]*entity.Vote, 0, len(c.followers))
	for nodename, follower := range c.followers {
		if c.config.CommitType == server.THREE_PHASE {
			ctx, _ = context.WithTimeout(ctx, time.Duration(c.config.Timeout)*time.Millisecond)
		}
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
			votes = append(votes, &entity.Vote{
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
	c.nodeCache.Set(c.height, req.Key, req.Value)
	c.nodeCache.SetVotes(c.height, votes)

	// precommit phase only for three-phase mode
	if c.config.CommitType == server.THREE_PHASE {
		log.Infof("precommit key %s", req.Key)
		for _, follower := range c.followers {
			ctx, _ = context.WithTimeout(ctx, time.Duration(c.config.Timeout)*time.Millisecond)
			if c.tracer != nil {
				span, ctx = c.tracer.StartSpanFromContext(ctx, "Precommit")
			}

			votes := votesToProto(c.nodeCache.GetVotes(c.height))
			resp, err := follower.Precommit(ctx, &pb.PrecommitRequest{Index: c.height, Votes: votes})
			if c.tracer != nil && span != nil {
				span.Finish()
			}
			if err != nil {
				log.Errorf(err.Error())
				return &entity.BroadcastResponse{Type: int32(pb.Type_NACK)}, nil
			}
			if resp.Type != pb.Type_ACK {
				return nil, status.Error(codes.Internal, "follower not acknowledged msg")
			}
		}
	}

	// the coordinator got all the answers, so it's time to persist msg and send commit command to followers
	key, value, ok := c.nodeCache.Get(c.height)
	if !ok {
		return nil, status.Error(codes.Internal, "can't to find msg in the coordinator's cache")
	}
	if err = c.database.Put(key, value); err != nil {
		return &entity.BroadcastResponse{Type: entity.ResponseNack}, status.Error(codes.Internal, "failed to save msg on coordinator")
	}

	// commit
	log.Infof("commit %s", req.Key)
	for _, follower := range c.followers {
		time.Sleep(blocktime)
		if c.tracer != nil {
			span, ctx = c.tracer.StartSpanFromContext(ctx, "Commit")
		}
		r, err := follower.Commit(ctx, &pb.CommitRequest{Index: c.height})
		if c.tracer != nil && span != nil {
			span.Finish()
		}
		if err != nil {
			log.Errorf(err.Error())
			return &entity.BroadcastResponse{Type: entity.ResponseNack}, nil
		}
		if r.Type != pb.Type_ACK {
			return nil, status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}
	log.Infof("coordinator got ack from all cohorts, committed key %s", req.Key)
	// increase height for next round
	atomic.AddUint64(&c.height, 1)
	fmt.Println("coordinator height", c.height)

	return &entity.BroadcastResponse{entity.ResponseAck, c.height}, nil
}

func (c *coordinatorImpl) Height() uint64 {
	return c.height
}

func votesToProto(votes []*entity.Vote) []*pb.Vote {
	pbvotes := make([]*pb.Vote, 0, len(votes))
	for _, v := range votes {
		pbvotes = append(pbvotes, &pb.Vote{
			Node:       v.Node,
			IsAccepted: v.IsAccepted,
		})
	}

	return pbvotes
}
