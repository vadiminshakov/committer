package commitalgo

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/db"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type wal interface {
	Write(index uint64, key string, value []byte) error
	Get(index uint64) (string, []byte, bool)
	Close() error
}

type Committer struct {
	noAutoCommit map[uint64]struct{}
	db           db.Repository
	wal          wal
	hookRegistry *hooks.Registry
	state        *stateMachine
	height       uint64
	timeout      uint64
}

func NewCommitter(d db.Repository, commitType string, wal wal, timeout uint64, customHooks ...hooks.Hook) *Committer {
	registry := hooks.NewRegistry()

	for _, hook := range customHooks {
		registry.Register(hook)
	}

	if len(customHooks) == 0 {
		registry.Register(hooks.NewDefaultHook())
	}

	return &Committer{
		hookRegistry: registry,
		db:           d,
		wal:          wal,
		noAutoCommit: make(map[uint64]struct{}),
		timeout:      timeout,
		state:        newStateMachine(mode(commitType)),
	}
}

// RegisterHook adds a new hook to the committer
func (c *Committer) RegisterHook(hook hooks.Hook) {
	c.hookRegistry.Register(hook)
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

	if !c.hookRegistry.ExecutePropose(req) {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: req.Height}, nil
	}

	log.Infof("received: %s=%s\n", req.Key, string(req.Value))
	c.wal.Write(req.Height, req.Key, req.Value)

	if c.state.mode == twophase {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
	}

	go func(ctx context.Context) {
		deadline := time.After(time.Duration(c.timeout) * time.Millisecond)
		select {
		case <-deadline:
			currentState := c.getCurrentState()
			if currentState == precommitStage || currentState == commitStage {
				return
			}

			c.wal.Write(req.Height, "skip", nil)
			log.Warn("skip proposed message after timeout")
		case <-ctx.Done():
			return
		}
	}(ctx)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}, nil
}

func (c *Committer) getCurrentState() string {
	return c.state.GetCurrentState()
}

func (c *Committer) Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error) {
	if err := c.state.Transition(precommitStage); err != nil {
		return nil, err
	}

	go func(ctx context.Context) {
		deadline := time.After(time.Duration(c.timeout) * time.Millisecond)
		select {
		case <-deadline:
			currentState := c.getCurrentState()
			if currentState == commitStage || currentState == proposeStage {
				return
			}

			_, err := c.Commit(ctx, &dto.CommitRequest{Height: index})
			if err != nil {
				log.Errorf("Failed to auto-commit after timeout: %v", err)
				// try to recover by forcing state back to propose
				c.state.SetCurrentState(proposeStage)
			} else {
				log.Warn("committed without coordinator after timeout")
			}
			return
		case <-ctx.Done():
			return
		}
	}(ctx)

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
}

func (c *Committer) Commit(ctx context.Context, req *dto.CommitRequest) (*dto.CohortResponse, error) {
	var response *dto.CohortResponse
	if req.Height != atomic.LoadUint64(&c.height) {
		return nil, status.Errorf(codes.AlreadyExists, "invalid commit height (got %d, but expected %d)", req.Height, c.height)
	}

	if err := c.state.Transition(commitStage); err != nil {
		return nil, err
	}

	if c.hookRegistry.ExecuteCommit(req) {
		log.Printf("Committing on height: %d\n", req.Height)
		key, value, ok := c.wal.Get(req.Height)
		if !ok {
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, fmt.Errorf("no value in node cache on the index %d", req.Height)
		}

		if key == "skip" {
			log.Infof("Skipping commit for height %d (operation was cancelled)", req.Height)
		} else {
			if err := c.db.Put(key, value); err != nil {
				return nil, err
			}
		}
		
		atomic.AddUint64(&c.height, 1)
		response = &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}
	} else {
		response = &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}
	}

	c.state.Transition(proposeStage)

	return response, nil
}
