package commitalgo

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/dto"
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
	proposeHook   func(req *dto.ProposeRequest) bool
	precommitHook func(height uint64) bool
	commitHook    func(req *dto.CommitRequest) bool
	height        uint64
	db            db.Repository
	vlog          voteslog.Log
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

func NewCommitter(d db.Repository, vlog voteslog.Log,
	proposeHook func(req *dto.ProposeRequest) bool,
	commitHook func(req *dto.CommitRequest) bool,
	timeout uint64) *Committer {
	return &Committer{
		proposeHook:   proposeHook,
		precommitHook: nil,
		commitHook:    commitHook,
		db:            d,
		vlog:          vlog,
		noAutoCommit:  make(map[uint64]struct{}),
		timeout:       timeout,
		precommitDone: newPendingPrecommit(),
	}
}

func (c *Committer) Height() uint64 {
	return c.height
}

func (c *Committer) Propose(_ context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error) {
	var response *dto.CohortResponse
	if atomic.LoadUint64(&c.height) > req.Height {
		return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: atomic.LoadUint64(&c.height)}, nil
	}

	if c.proposeHook(req) {
		log.Infof("received: %s=%s\n", req.Key, string(req.Value))
		c.vlog.Set(req.Height, req.Key, req.Value)
		response = &dto.CohortResponse{ResponseType: dto.ResponseTypeAck, Height: req.Height}
	} else {
		response = &dto.CohortResponse{ResponseType: dto.ResponseTypeNack, Height: req.Height}
	}

	return response, nil
}

func (c *Committer) Precommit(ctx context.Context, index uint64, votes []*dto.Vote) (*dto.CohortResponse, error) {
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
					break ForLoop
				}
				c.Commit(ctx, &dto.CommitRequest{Height: index})
				log.Warn("committed without coordinator after timeout")
				break ForLoop
			}
		}
	}(ctx)
	for _, v := range votes {
		if !v.IsAccepted {
			log.Printf("Node %s is not accepted proposal with index %d\n", v.Node, index)
			return &dto.CohortResponse{ResponseType: dto.ResponseTypeNack}, nil
		}
	}

	return &dto.CohortResponse{ResponseType: dto.ResponseTypeAck}, nil
}

func isAllNodesAccepted(votes []*dto.Vote) bool {
	for _, v := range votes {
		if !v.IsAccepted {
			return false
		}
	}

	return true
}

func (c *Committer) Commit(ctx context.Context, req *dto.CommitRequest) (*dto.CohortResponse, error) {
	c.precommitDone.signalToChan(atomic.LoadUint64(&c.height))

	c.noAutoCommit[req.Height] = struct{}{}

	var response *dto.CohortResponse
	if req.Height < atomic.LoadUint64(&c.height) {
		return nil, status.Errorf(codes.AlreadyExists, "stale commit proposed by coordinator (got %d, but actual height is %d)", req.Height, c.height)
	}
	if c.commitHook(req) {
		log.Printf("Committing on height: %d\n", req.Height)
		key, value, ok := c.vlog.Get(req.Height)
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
	return response, nil
}
