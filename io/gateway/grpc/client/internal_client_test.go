package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeInternalCommitAPIClient struct {
	proposeFn   func(context.Context, *proto.ProposeRequest) (*proto.Response, error)
	precommitFn func(context.Context, *proto.PrecommitRequest) (*proto.Response, error)
	commitFn    func(context.Context, *proto.CommitRequest) (*proto.Response, error)
	abortFn     func(context.Context, *proto.AbortRequest) (*proto.Response, error)
	decisionFn  func(context.Context, *proto.DecisionRequest) (*proto.DecisionResponse, error)
}

func (f *fakeInternalCommitAPIClient) Propose(ctx context.Context, req *proto.ProposeRequest, _ ...grpc.CallOption) (*proto.Response, error) {
	if f.proposeFn == nil {
		return nil, fmt.Errorf("unexpected Propose RPC")
	}
	return f.proposeFn(ctx, req)
}

func (f *fakeInternalCommitAPIClient) Precommit(ctx context.Context, req *proto.PrecommitRequest, _ ...grpc.CallOption) (*proto.Response, error) {
	if f.precommitFn == nil {
		return nil, fmt.Errorf("unexpected Precommit RPC")
	}
	return f.precommitFn(ctx, req)
}

func (f *fakeInternalCommitAPIClient) Commit(ctx context.Context, req *proto.CommitRequest, _ ...grpc.CallOption) (*proto.Response, error) {
	if f.commitFn == nil {
		return nil, fmt.Errorf("unexpected Commit RPC")
	}
	return f.commitFn(ctx, req)
}

func (f *fakeInternalCommitAPIClient) Abort(ctx context.Context, req *proto.AbortRequest, _ ...grpc.CallOption) (*proto.Response, error) {
	if f.abortFn == nil {
		return nil, fmt.Errorf("unexpected Abort RPC")
	}
	return f.abortFn(ctx, req)
}

func (f *fakeInternalCommitAPIClient) Decision(ctx context.Context, req *proto.DecisionRequest, _ ...grpc.CallOption) (*proto.DecisionResponse, error) {
	if f.decisionFn == nil {
		return nil, fmt.Errorf("unexpected Decision RPC")
	}
	return f.decisionFn(ctx, req)
}

func TestCohortClientAddr(t *testing.T) {
	t.Parallel()

	client := &CohortClient{addr: "localhost:3001"}
	require.Equal(t, "localhost:3001", client.Addr())
}

func TestCohortClientMapsProposalAndReply(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		protocol dto.Protocol
		want     proto.CommitType
	}{
		{name: "two phase", protocol: dto.ProtocolTwoPhase, want: proto.CommitType_TWO_PHASE_COMMIT},
		{name: "three phase", protocol: dto.ProtocolThreePhase, want: proto.CommitType_THREE_PHASE_COMMIT},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			adapter := &CohortClient{rpc: &fakeInternalCommitAPIClient{
				proposeFn: func(_ context.Context, req *proto.ProposeRequest) (*proto.Response, error) {
					require.Equal(t, "account", req.Key)
					require.Equal(t, []byte("open"), req.Value)
					require.Equal(t, uint64(11), req.Index)
					require.Equal(t, tt.want, req.CommitType)
					return &proto.Response{Type: proto.Type_ACK, Index: 12}, nil
				},
			}}

			reply, err := adapter.Propose(context.Background(), dto.Proposal{
				Height:   11,
				Protocol: tt.protocol,
				Transaction: dto.Transaction{
					Key:   "account",
					Value: []byte("open"),
				},
			})
			require.NoError(t, err)
			require.Equal(t, dto.ParticipantReply{Accepted: true, Height: 12}, reply)
		})
	}
}

func TestCohortClientNormalizesNACK(t *testing.T) {
	t.Parallel()

	adapter := &CohortClient{rpc: &fakeInternalCommitAPIClient{
		precommitFn: func(_ context.Context, req *proto.PrecommitRequest) (*proto.Response, error) {
			require.Equal(t, uint64(7), req.Index)
			return &proto.Response{Type: proto.Type_NACK, Index: 6}, nil
		},
	}}

	reply, err := adapter.Precommit(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, dto.ParticipantReply{Accepted: false, Height: 6}, reply)
}

func TestCohortClientRejectsEmptyProtocolResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(*CohortClient) (dto.ParticipantReply, error)
		rpc  *fakeInternalCommitAPIClient
	}{
		{
			name: "propose",
			call: func(adapter *CohortClient) (dto.ParticipantReply, error) {
				return adapter.Propose(context.Background(), dto.Proposal{
					Height:   7,
					Protocol: dto.ProtocolTwoPhase,
				})
			},
			rpc: &fakeInternalCommitAPIClient{
				proposeFn: func(context.Context, *proto.ProposeRequest) (*proto.Response, error) {
					return nil, nil
				},
			},
		},
		{
			name: "precommit",
			call: func(adapter *CohortClient) (dto.ParticipantReply, error) {
				return adapter.Precommit(context.Background(), 7)
			},
			rpc: &fakeInternalCommitAPIClient{
				precommitFn: func(context.Context, *proto.PrecommitRequest) (*proto.Response, error) {
					return nil, nil
				},
			},
		},
		{
			name: "decide",
			call: func(adapter *CohortClient) (dto.ParticipantReply, error) {
				return adapter.ApplyFinalDecision(context.Background(), dto.FinalDecision{
					Height:  7,
					Outcome: dto.OutcomeCommit,
				})
			},
			rpc: &fakeInternalCommitAPIClient{
				commitFn: func(context.Context, *proto.CommitRequest) (*proto.Response, error) {
					return nil, nil
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := &CohortClient{rpc: tt.rpc}
			reply, err := tt.call(adapter)
			require.Equal(t, dto.ParticipantReply{}, reply)
			require.ErrorContains(t, err, "participant protocol returned an empty response")
		})
	}
}

func TestCohortClientRoutesFinalDecisions(t *testing.T) {
	t.Parallel()

	operations := make([]string, 0, 3)
	adapter := &CohortClient{rpc: &fakeInternalCommitAPIClient{
		commitFn: func(_ context.Context, req *proto.CommitRequest) (*proto.Response, error) {
			operations = append(operations, "commit")
			require.Contains(t, []uint64{3, 4}, req.Index)
			return &proto.Response{Type: proto.Type_ACK}, nil
		},
		abortFn: func(_ context.Context, req *proto.AbortRequest) (*proto.Response, error) {
			operations = append(operations, "abort")
			require.Equal(t, uint64(5), req.Height)
			require.NotEmpty(t, req.Reason)
			return &proto.Response{Type: proto.Type_ACK}, nil
		},
	}}

	for _, decision := range []dto.FinalDecision{
		{Height: 3, Outcome: dto.OutcomeCommit},
		{Height: 4, Outcome: dto.OutcomeCommit, RequirePrecommit: true},
		{Height: 5, Outcome: dto.OutcomeAbort},
	} {
		reply, err := adapter.ApplyFinalDecision(context.Background(), decision)
		require.NoError(t, err)
		require.True(t, reply.Accepted)
	}

	require.Equal(t, []string{"commit", "commit", "abort"}, operations)
}

func TestCohortClientRejectsInvalidDomainValuesAndWrapsRPCError(t *testing.T) {
	t.Parallel()

	adapter := &CohortClient{rpc: &fakeInternalCommitAPIClient{
		proposeFn: func(context.Context, *proto.ProposeRequest) (*proto.Response, error) {
			return nil, status.Error(codes.Unavailable, "cohort offline")
		},
	}}

	_, err := adapter.Propose(context.Background(), dto.Proposal{Height: 9, Protocol: dto.ProtocolTwoPhase})
	require.ErrorContains(t, err, "participant propose at height 9")
	require.ErrorContains(t, err, "cohort offline")

	_, err = adapter.Propose(context.Background(), dto.Proposal{Protocol: dto.Protocol(99)})
	require.ErrorContains(t, err, "unsupported participant protocol")

	_, err = adapter.ApplyFinalDecision(context.Background(), dto.FinalDecision{Height: 9, Outcome: dto.OutcomeUnknown})
	require.ErrorContains(t, err, "outcome 0 is not final")
}

func TestCoordinatorClientMapsOutcomesAndErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		resp    *proto.DecisionResponse
		err     error
		want    dto.Outcome
		wantErr string
	}{
		{name: "commit", resp: &proto.DecisionResponse{Outcome: proto.Outcome_OUTCOME_COMMIT}, want: dto.OutcomeCommit},
		{name: "abort", resp: &proto.DecisionResponse{Outcome: proto.Outcome_OUTCOME_ABORT}, want: dto.OutcomeAbort},
		{name: "unknown", resp: &proto.DecisionResponse{Outcome: proto.Outcome_OUTCOME_UNKNOWN}, want: dto.OutcomeUnknown},
		{name: "empty response", want: dto.OutcomeUnknown, wantErr: "empty response"},
		{name: "rpc error", err: status.Error(codes.Unavailable, "coordinator offline"), want: dto.OutcomeUnknown, wantErr: "request decision at height 17"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &CoordinatorClient{rpc: &fakeInternalCommitAPIClient{
				decisionFn: func(_ context.Context, req *proto.DecisionRequest) (*proto.DecisionResponse, error) {
					require.Equal(t, uint64(17), req.Height)
					return tt.resp, tt.err
				},
			}}

			outcome, err := client.Decision(context.Background(), 17)
			require.Equal(t, tt.want, outcome)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
			require.NoError(t, client.Close())
			require.NoError(t, client.Close())
		})
	}
}
