package commitalgo

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/db"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync/atomic"
	"time"
)

type wal interface {
	Set(index uint64, key string, value []byte) error
	Get(index uint64) (string, []byte, bool)
	Close() error
}

type Committer struct {
	noAutoCommit  map[uint64]struct{}
	db            db.Repository
	wal           wal
	proposeHook   func(req *dto.ProposeRequest) bool
	precommitHook func(height uint64) bool
	commitHook    func(req *dto.CommitRequest) bool
	state         *stateMachine
	height        uint64
	timeout       uint64
}

func NewCommitter(d db.Repository, commitType string, wal wal,
	proposeHook func(req *dto.ProposeRequest) bool,
	commitHook func(req *dto.CommitRequest) bool,
	timeout uint64) *Committer {
	return &Committer{
		proposeHook:   proposeHook,
		precommitHook: nil,
		commitHook:    commitHook,
		db:            d,
		wal:           wal,
		noAutoCommit:  make(map[uint64]struct{}),
		timeout:       timeout,
		state:         newStateMachine(mode(commitType)),
	}
}

func (c *Committer) Height() uint64 {
	return c.height
}

func (c *Committer) Propose(ctx context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error) {
	if err := c.state.Transition(proposeStage); err != nil {
		return nil, err
	}

	if atomic.LoadUint64(&c.height) > req.Height {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: atomic.LoadUint64(&c.height)}, nil
	}

	if !c.proposeHook(req) {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: req.Height}, nil
	}

	log.Infof("received: %s=%s\n", req.Key, string(req.Value))
	c.wal.Set(req.Height, req.Key, req.Value)

	if c.state.mode == twophase {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
	}

	go func(ctx context.Context) {
		deadline := time.After(time.Duration(c.timeout) * time.Millisecond)
		for {
			select {
			case <-deadline:
				if c.state.currentState == commitStage {
					return
				}

				c.wal.Set(req.Height, "skip", nil)

				log.Warn("skip proposed message after timeout")
			}
		}
	}(ctx)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
}

func (c *Committer) Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error) {
	if err := c.state.Transition(precommitStage); err != nil {
		return nil, err
	}

	go func(ctx context.Context) {
		deadline := time.After(time.Duration(c.timeout) * time.Millisecond)
		for {
			select {
			case <-deadline:
				if c.state.currentState == commitStage {
					return
				}

				c.Commit(ctx, &dto.CommitRequest{Height: index})
				c.state.currentState = proposeStage
				log.Warn("committed without coordinator after timeout")
			}
		}
	}(ctx)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
}

func (c *Committer) Commit(ctx context.Context, req *dto.CommitRequest) (*dto.CohortResponse, error) {
	var response *dto.CohortResponse
	if req.Height < atomic.LoadUint64(&c.height) {
		return nil, status.Errorf(codes.AlreadyExists, "stale commit proposed by coordinator (got %d, but actual height is %d)", req.Height, c.height)
	}

	if err := c.state.Transition(commitStage); err != nil {
		return nil, err
	}

	if c.commitHook(req) {
		log.Printf("Committing on height: %d\n", req.Height)
		key, value, ok := c.wal.Get(req.Height)
		if !ok {
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, fmt.Errorf("no value in node cache on the index %d", req.Height)
		}

		if err := c.db.Put(key, value); err != nil {
			return nil, err
		}
		response = &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}
	} else {
		response = &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}
	}

	if response.ResponseType == dto.ResponseTypeAck {
		fmt.Println("ack cohort", atomic.LoadUint64(&c.height))
		atomic.AddUint64(&c.height, 1)
	}

	c.state.currentState = proposeStage

	return response, nil
}
