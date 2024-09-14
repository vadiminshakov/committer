package cohort

import (
	"context"
	"errors"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo"
	"github.com/vadiminshakov/committer/core/dto"
)

type Cohort interface {
	Propose(ctx context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error)
	Precommit(ctx context.Context, index uint64, votes []*dto.Vote) (*dto.CohortResponse, error)
	Commit(ctx context.Context, in *dto.CommitRequest) (*dto.CohortResponse, error)
	Height() uint64
}

type CohortImpl struct {
	committer  *commitalgo.Committer
	commitType Mode
	height     uint64
}

func NewCohort(
	committer *commitalgo.Committer,
	commitType Mode) *CohortImpl {
	return &CohortImpl{
		committer:  committer,
		commitType: commitType,
	}
}

func (c *CohortImpl) Height() uint64 {
	return c.committer.Height()
}

func (c *CohortImpl) Propose(ctx context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error) {
	return c.committer.Propose(ctx, req)
}

func (s *CohortImpl) Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error) {
	if s.commitType != THREE_PHASE {
		return nil, errors.New("precommit is allowed for 3PC mode only")
	}

	return s.committer.Precommit(ctx, index)
}

func (c *CohortImpl) Commit(ctx context.Context, in *dto.CommitRequest) (resp *dto.CohortResponse, err error) {
	return c.committer.Commit(ctx, in)
}
