package commitalgo

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/cache"
	"github.com/vadiminshakov/committer/core/entity"
	"github.com/vadiminshakov/committer/io/db"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"sync/atomic"
)

type Committer struct {
	proposeHook   func(req *entity.ProposeRequest) bool
	precommitHook func(height uint64) bool
	commitHook    func(req *entity.CommitRequest) bool
	height        uint64
	db            db.Repository
	nodeCache     *cache.Cache
	noAutoCommit  map[uint64]struct{}
}

func NewCommitter(d db.Repository, nodeCache *cache.Cache,
	proposeHook func(req *entity.ProposeRequest) bool,
	commitHook func(req *entity.CommitRequest) bool) *Committer {
	return &Committer{
		proposeHook:   proposeHook,
		precommitHook: nil,
		commitHook:    commitHook,
		db:            d,
		nodeCache:     nodeCache,
		noAutoCommit:  make(map[uint64]struct{}),
	}
}

func (c *Committer) Height() uint64 {
	return c.height
}

func (c *Committer) Propose(_ context.Context, req *entity.ProposeRequest) (*entity.CohortResponse, error) {
	var response *entity.CohortResponse
	if c.proposeHook(req) {
		log.Infof("received: %s=%s\n", req.Key, string(req.Value))
		c.nodeCache.Set(req.Height, req.Key, req.Value)
		response = &entity.CohortResponse{ResponseType: entity.ResponseTypeAck, Height: req.Height}
	} else {
		response = &entity.CohortResponse{ResponseType: entity.ResponseTypeNack, Height: req.Height}
	}
	if c.height > req.Height {
		response = &entity.CohortResponse{ResponseType: entity.ResponseTypeNack, Height: c.height}
	}

	return response, nil
}

func (c *Committer) Precommit(ctx context.Context, index uint64, votes []*entity.Vote) (*entity.CohortResponse, error) {
	go func(ctx context.Context) {
	ForLoop:
		for {
			select {
			case <-ctx.Done():
				if _, ok := c.noAutoCommit[index]; ok {
					return
				}
				md := metadata.Pairs("mode", "autocommit")
				ctx := metadata.NewOutgoingContext(context.Background(), md)
				if !isAllNodesAccepted(votes) {
					c.rollback()
					break ForLoop
				}
				c.Commit(ctx, &entity.CommitRequest{Height: index})
				log.Warn("committed without coordinator after timeout")
				break ForLoop
			}
		}
	}(ctx)
	for _, v := range votes {
		if !v.IsAccepted {
			log.Printf("Node %s is not accepted proposal with index %d\n", v.Node, index)
			return &entity.CohortResponse{ResponseType: entity.ResponseTypeNack}, nil
		}
	}

	return &entity.CohortResponse{ResponseType: entity.ResponseTypeAck}, nil
}

func isAllNodesAccepted(votes []*entity.Vote) bool {
	for _, v := range votes {
		if !v.IsAccepted {
			return false
		}
	}

	return true
}

func (c *Committer) Commit(ctx context.Context, req *entity.CommitRequest) (*entity.CohortResponse, error) {
	c.noAutoCommit[req.Height] = struct{}{}
	if req.IsRollback {
		c.rollback()
	}

	var response *entity.CohortResponse
	if req.Height < c.height {
		return nil, status.Errorf(codes.AlreadyExists, "stale commit proposed by coordinator (got %d, but actual height is %d)", req.Height, c.height)
	}
	if c.commitHook(req) {
		log.Printf("Committing on height: %d\n", req.Height)
		key, value, ok := c.nodeCache.Get(req.Height)
		if !ok {
			return &entity.CohortResponse{ResponseType: entity.ResponseTypeNack}, fmt.Errorf("no value in node cache on the index %d", req.Height)
		}

		if err := c.db.Put(key, value); err != nil {
			return nil, err
		}
		response = &entity.CohortResponse{ResponseType: entity.ResponseTypeAck}
	} else {
		c.nodeCache.Delete(req.Height)
		response = &entity.CohortResponse{ResponseType: entity.ResponseTypeNack}
	}

	if response.ResponseType == entity.ResponseTypeAck {
		fmt.Println("ack cohort", c.height)
		atomic.AddUint64(&c.height, 1)
	}

	return response, nil
}

func (c *Committer) rollback() {
	c.nodeCache.Delete(c.height)
}
