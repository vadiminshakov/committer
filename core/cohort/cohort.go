package cohort

import (
	"context"
	"errors"

	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/dto"
)

type Mode string

const THREE_PHASE Mode = "three-phase"

// Committer defines the interface for commit algorithms
//
//go:generate mockgen -destination=../../../mocks/mock_committer.go -package=mocks . Committer
type Committer interface {
	Height() uint64
	Propose(ctx context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error)
	Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error)
	Commit(ctx context.Context, req *dto.CommitRequest) (*dto.CohortResponse, error)
	Abort(ctx context.Context, req *dto.AbortRequest) (*dto.CohortResponse, error)
	RegisterHook(hook hooks.Hook)
}

type CohortImpl struct {
	committer  Committer
	commitType Mode
}

func NewCohort(
	committer Committer,
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

func (c *CohortImpl) Abort(ctx context.Context, req *dto.AbortRequest) (*dto.CohortResponse, error) {
	return c.committer.Abort(ctx, req)
}
