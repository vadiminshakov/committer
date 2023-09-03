package commitalgo

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/entity"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/voteslog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"sync"
	"sync/atomic"
	"time"
)

type Committer struct {
	proposeHook   func(req *entity.ProposeRequest) bool
	precommitHook func(height uint64) bool
	commitHook    func(req *entity.CommitRequest) bool
	height        uint64
	db            db.Repository
	nodeCache     *voteslog.FileVotesLog
	noAutoCommit  map[uint64]struct{}
	timeout       uint64
	precommitDone pendingPrecommit
}

type pendingPrecommit struct {
	done map[uint64]chan struct{}
	mu   sync.RWMutex
}

func newPendingPrecommit() pendingPrecommit {
	return pendingPrecommit{
		done: make(map[uint64]chan struct{}),
	}
}

func (p *pendingPrecommit) add(height uint64) {
	p.mu.Lock()
	p.done[height] = make(chan struct{})
	p.mu.Unlock()
}

func (p *pendingPrecommit) remove(height uint64) {
	p.mu.RLock()
	ch := p.done[height]
	p.mu.RUnlock()
	close(ch)

	p.mu.Lock()
	delete(p.done, height)
	p.mu.Unlock()
}

func (p *pendingPrecommit) wait(height uint64) chan struct{} {
	p.mu.RLock()
	waitch := p.done[height]
	p.mu.RUnlock()

	return waitch
}

func (p *pendingPrecommit) signalToChan(height uint64) {
	p.mu.RLock()
	ch := p.done[height]
	p.mu.RUnlock()

	select {
	case ch <- struct{}{}:
	default:

	}
}

func NewCommitter(d db.Repository, nodeCache *voteslog.FileVotesLog,
	proposeHook func(req *entity.ProposeRequest) bool,
	commitHook func(req *entity.CommitRequest) bool,
	timeout uint64) *Committer {
	return &Committer{
		proposeHook:   proposeHook,
		precommitHook: nil,
		commitHook:    commitHook,
		db:            d,
		nodeCache:     nodeCache,
		noAutoCommit:  make(map[uint64]struct{}),
		timeout:       timeout,
		precommitDone: newPendingPrecommit(),
	}
}

func (c *Committer) Height() uint64 {
	return c.height
}

func (c *Committer) Propose(_ context.Context, req *entity.ProposeRequest) (*entity.CohortResponse, error) {
	var response *entity.CohortResponse
	if atomic.LoadUint64(&c.height) > req.Height {
		return &entity.CohortResponse{ResponseType: entity.ResponseTypeNack, Height: atomic.LoadUint64(&c.height)}, nil
	}

	if c.proposeHook(req) {
		log.Infof("received: %s=%s\n", req.Key, string(req.Value))
		c.nodeCache.Set(req.Height, req.Key, req.Value)
		response = &entity.CohortResponse{ResponseType: entity.ResponseTypeAck, Height: req.Height}
	} else {
		response = &entity.CohortResponse{ResponseType: entity.ResponseTypeNack, Height: req.Height}
	}

	return response, nil
}

func (c *Committer) Precommit(ctx context.Context, index uint64, votes []*entity.Vote) (*entity.CohortResponse, error) {
	c.precommitDone.add(index)

	go func(ctx context.Context) {
		deadline := time.After(time.Duration(c.timeout) * time.Millisecond)
	ForLoop:
		for {
			select {
			case <-c.precommitDone.wait(index):
				break ForLoop
			case <-deadline:
				c.precommitDone.remove(index)
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
	c.precommitDone.signalToChan(atomic.LoadUint64(&c.height))

	c.noAutoCommit[req.Height] = struct{}{}
	if req.IsRollback {
		c.rollback()
	}

	var response *entity.CohortResponse
	if req.Height < atomic.LoadUint64(&c.height) {
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
		fmt.Println("ack cohort", atomic.LoadUint64(&c.height))
		atomic.AddUint64(&c.height, 1)
	}
	return response, nil
}

func (c *Committer) rollback() {
	c.nodeCache.Delete(c.height)
}
