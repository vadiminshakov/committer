package cohort

import (
	"context"
	"errors"
	"github.com/openzipkin/zipkin-go"
	"github.com/vadiminshakov/committer/algorithm"
	"github.com/vadiminshakov/committer/entity"
	pb "github.com/vadiminshakov/committer/proto"
)

type Cohort struct {
	committer  algorithm.Committer
	Tracer     *zipkin.Tracer
	CommitType mode
	height     uint64
}

func (c *Cohort) Height() uint64 {
	return c.height
}

func (c *Cohort) Propose(ctx context.Context, req *entity.ProposeRequest) (*entity.Response, error) {
	return c.committer.Propose(ctx, req)
}

func (s *Cohort) Precommit(ctx context.Context, req *PrecommitRequest) (*entity.Response, error) {
	if s.CommitType != THREE_PHASE {
		return nil, errors.New("precommit is allowed for 3PC mode only")
	}

	return s.committer.Precommit(ctx, req.Index, req.Votes)
}

func (c *Cohort) Commit(ctx context.Context, req *entity.CommitRequest) (resp *pb.Response, err error) {
	return c.Commit(ctx, req)
}
