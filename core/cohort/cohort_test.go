package cohort

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/committer/mocks"
	"go.uber.org/mock/gomock"
)

func TestNewCohort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)

	// test creating 2PC cohort
	cohort := NewCohort(mockCommitter, "two-phase")
	require.NotNil(t, cohort)
	require.Equal(t, Mode("two-phase"), cohort.commitType)

	// test creating 3PC cohort
	cohort3PC := NewCohort(mockCommitter, THREE_PHASE)
	require.NotNil(t, cohort3PC)
	require.Equal(t, THREE_PHASE, cohort3PC.commitType)
}

func TestCohort_Height(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)
	cohort := NewCohort(mockCommitter, "two-phase")

	// expect Height to be called and return 5
	mockCommitter.EXPECT().Height().Return(uint64(5))

	height := cohort.Height()
	require.Equal(t, uint64(5), height)
}

func TestCohort_Propose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)
	cohort := NewCohort(mockCommitter, "two-phase")

	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	expectedResp := &dto.CohortResponse{
		ResponseType: dto.ResponseTypeAck,
		Height:       0,
	}

	// expect Propose to be called and return success
	mockCommitter.EXPECT().Propose(ctx, proposeReq).Return(expectedResp, nil)

	resp, err := cohort.Propose(ctx, proposeReq)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)
	require.Equal(t, uint64(0), resp.Height)
}

func TestCohort_Propose_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)
	cohort := NewCohort(mockCommitter, "two-phase")

	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	expectedErr := fmt.Errorf("propose failed")

	// expect Propose to be called and return error
	mockCommitter.EXPECT().Propose(ctx, proposeReq).Return(nil, expectedErr)

	resp, err := cohort.Propose(ctx, proposeReq)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, expectedErr, err)
}

func TestCohort_Precommit_TwoPhase(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)
	cohort := NewCohort(mockCommitter, "two-phase")

	ctx := context.Background()

	// test precommit in 2PC mode (should fail)
	_, err := cohort.Precommit(ctx, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "precommit is allowed for 3PC mode only")
}

func TestCohort_Precommit_ThreePhase(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)
	cohort := NewCohort(mockCommitter, THREE_PHASE)

	ctx := context.Background()

	expectedResp := &dto.CohortResponse{
		ResponseType: dto.ResponseTypeAck,
	}

	// expect Precommit to be called and return success
	mockCommitter.EXPECT().Precommit(ctx, uint64(0)).Return(expectedResp, nil)

	// test precommit in 3PC mode (should succeed)
	resp, err := cohort.Precommit(ctx, 0)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)
}

func TestCohort_Precommit_ThreePhase_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)
	cohort := NewCohort(mockCommitter, THREE_PHASE)

	ctx := context.Background()
	expectedErr := fmt.Errorf("precommit failed")

	// expect Precommit to be called and return error
	mockCommitter.EXPECT().Precommit(ctx, uint64(0)).Return(nil, expectedErr)

	resp, err := cohort.Precommit(ctx, 0)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, expectedErr, err)
}

func TestCohort_Commit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)
	cohort := NewCohort(mockCommitter, "two-phase")

	ctx := context.Background()
	commitReq := &dto.CommitRequest{Height: 0}

	expectedResp := &dto.CohortResponse{
		ResponseType: dto.ResponseTypeAck,
	}

	// expect Commit to be called and return success
	mockCommitter.EXPECT().Commit(ctx, commitReq).Return(expectedResp, nil)

	resp, err := cohort.Commit(ctx, commitReq)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)
}

func TestCohort_Commit_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)
	cohort := NewCohort(mockCommitter, "two-phase")

	ctx := context.Background()
	commitReq := &dto.CommitRequest{Height: 0}
	expectedErr := fmt.Errorf("commit failed")

	// expect Commit to be called and return error
	mockCommitter.EXPECT().Commit(ctx, commitReq).Return(nil, expectedErr)

	resp, err := cohort.Commit(ctx, commitReq)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, expectedErr, err)
}

func TestCohort_Commit_Nack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)
	cohort := NewCohort(mockCommitter, "two-phase")

	ctx := context.Background()
	commitReq := &dto.CommitRequest{Height: 0}

	expectedResp := &dto.CohortResponse{
		ResponseType: dto.ResponseTypeNack,
	}

	// expect Commit to be called and return NACK
	mockCommitter.EXPECT().Commit(ctx, commitReq).Return(expectedResp, nil)

	resp, err := cohort.Commit(ctx, commitReq)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, dto.ResponseTypeNack, resp.ResponseType)
}

func TestCohort_ModeValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCommitter := mocks.NewMockCommitter(ctrl)

	// test different modes
	modes := []Mode{"two-phase", THREE_PHASE, "custom-mode"}

	for _, mode := range modes {
		cohort := NewCohort(mockCommitter, mode)
		require.NotNil(t, cohort)
		require.Equal(t, mode, cohort.commitType)
	}
}

