package algoplagin

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/cache"
	"github.com/vadiminshakov/committer/db"
	"github.com/vadiminshakov/committer/entity"
)

type Committer struct {
	proposeHook   func(req *entity.ProposeRequest) bool
	precommitHook func(height uint64) bool
	commitHook    func(req *entity.CommitRequest) bool
	height        uint64
	db            db.Database
	nodeCache     *cache.Cache
}

func NewCommitter(d db.Database, nodeCache *cache.Cache,
	proposeHook func(req *entity.ProposeRequest) bool,
	commitHook func(req *entity.CommitRequest) bool) *Committer {
	return &Committer{
		proposeHook:   proposeHook,
		precommitHook: nil,
		commitHook:    commitHook,
		db:            d,
		nodeCache:     nodeCache,
	}
}

func (c *Committer) Propose(_ context.Context, req *entity.ProposeRequest) (*entity.Response, error) {
	var response *entity.Response
	if c.proposeHook(req) {
		log.Infof("received: %s=%s\n", req.Key, string(req.Value))
		c.nodeCache.Set(req.Height, req.Key, req.Value)
		response = &entity.Response{ResponseType: entity.ResponseTypeAck, Height: req.Height}
	} else {
		response = &entity.Response{ResponseType: entity.ResponseTypeNack, Height: req.Height}
	}
	if c.height > req.Height {
		response = &entity.Response{ResponseType: entity.ResponseTypeNack, Height: c.height}
	}

	return response, nil
}

func (c *Committer) Precommit(_ context.Context, index uint64, votes []*entity.Vote) (*entity.Response, error) {
	for _, v := range c.nodeCache.GetVotes(index) {
		if !v.IsAccepted {
			log.Printf("Node %s is not accepted proposal with index %d\n", v.Node, v.Height)
			return &entity.Response{ResponseType: entity.ResponseTypeNack}, nil
		}
	}

	return &entity.Response{ResponseType: entity.ResponseTypeAck}, nil
}

func (c *Committer) Commit(_ context.Context, req *entity.CommitRequest) (*entity.Response, error) {
	var response *entity.Response
	if c.commitHook(req) {
		log.Printf("Committing on height: %d\n", req.Height)
		key, value, ok := c.nodeCache.Get(req.Height)
		if !ok {
			return &entity.Response{ResponseType: entity.ResponseTypeNack}, fmt.Errorf("no value in node cache on the index %d", req.Height)
		}

		if err := c.db.Put(key, value); err != nil {
			return nil, err
		}
		response = &entity.Response{ResponseType: entity.ResponseTypeAck}
	} else {
		c.nodeCache.Delete(req.Height)
		response = &entity.Response{ResponseType: entity.ResponseTypeNack}
	}
	return response, nil
}
