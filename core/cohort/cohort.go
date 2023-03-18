package cohort

import (
	"context"
	"errors"
	"github.com/openzipkin/zipkin-go"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo"
	"github.com/vadiminshakov/committer/core/entity"
)

type Cohort interface {
	Propose(ctx context.Context, req *entity.ProposeRequest) (*entity.CohortResponse, error)
	Precommit(ctx context.Context, index uint64, votes []*entity.Vote) (*entity.CohortResponse, error)
	Commit(ctx context.Context, in *entity.CommitRequest) (*entity.CohortResponse, error)
	Height() uint64
}

type CohortImpl struct {
	committer  *commitalgo.Committer
	tracer     *zipkin.Tracer
	commitType Mode
	height     uint64
}

func NewCohort(
	tracer *zipkin.Tracer,
	committer *commitalgo.Committer,
	commitType Mode) *CohortImpl {
	return &CohortImpl{
		committer:  committer,
		tracer:     tracer,
		commitType: commitType,
	}
}

func (c *CohortImpl) Height() uint64 {
	return c.committer.Height()
}

func (c *CohortImpl) Propose(ctx context.Context, req *entity.ProposeRequest) (*entity.CohortResponse, error) {
	return c.committer.Propose(ctx, req)
}

func (s *CohortImpl) Precommit(ctx context.Context, index uint64, votes []*entity.Vote) (*entity.CohortResponse, error) {
	if s.commitType != THREE_PHASE {
		return nil, errors.New("precommit is allowed for 3PC mode only")
	}

	return s.committer.Precommit(ctx, index, votes)
}

func (c *CohortImpl) Commit(ctx context.Context, in *entity.CommitRequest) (resp *entity.CohortResponse, err error) {
	return c.committer.Commit(ctx, in)
}
