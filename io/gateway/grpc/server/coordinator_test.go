package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	corecoordinator "github.com/vadiminshakov/committer/core/coordinator"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type coordinatorStub struct {
	broadcast func(context.Context, dto.BroadcastRequest) (*dto.BroadcastResponse, error)
}

func (s coordinatorStub) Broadcast(ctx context.Context, request dto.BroadcastRequest) (*dto.BroadcastResponse, error) {
	return s.broadcast(ctx, request)
}

func (coordinatorStub) Decision(uint64) dto.Outcome { return dto.OutcomeUnknown }

func TestPutMapsCommittedTransactionHeight(t *testing.T) {
	server := &Server{coordinator: coordinatorStub{broadcast: func(_ context.Context, request dto.BroadcastRequest) (*dto.BroadcastResponse, error) {
		require.Equal(t, dto.BroadcastRequest{Key: "key", Value: []byte("value")}, request)

		return &dto.BroadcastResponse{Type: dto.ResponseTypeAck, Height: 7}, nil
	}}}

	response, err := server.Put(context.Background(), &proto.Entry{Key: "key", Value: []byte("value")})
	require.NoError(t, err)
	require.Equal(t, proto.Type_ACK, response.Type)
	require.Equal(t, uint64(7), response.Index)
}

func TestPutMapsCommittedNotAppliedToGRPCStatus(t *testing.T) {
	server := &Server{coordinator: coordinatorStub{broadcast: func(context.Context, dto.BroadcastRequest) (*dto.BroadcastResponse, error) {
		return &dto.BroadcastResponse{Type: dto.ResponseTypeNack, Height: 3}, &corecoordinator.CommittedNotAppliedError{Height: 3}
	}}}

	response, err := server.Put(context.Background(), &proto.Entry{Key: "key"})
	require.Nil(t, response)
	require.Equal(t, codes.Internal, status.Code(err))
	require.ErrorContains(t, err, "committed but not applied")
}

func TestCoordinatorErrorStatusMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "canceled", err: fmt.Errorf("vote: %w", context.Canceled), code: codes.Canceled},
		{name: "deadline", err: fmt.Errorf("vote: %w", context.DeadlineExceeded), code: codes.DeadlineExceeded},
		{name: "invalid transaction", err: corecoordinator.ErrInvalidTransaction, code: codes.InvalidArgument},
		{name: "not ready", err: corecoordinator.ErrCoordinatorNotReady, code: codes.FailedPrecondition},
		{name: "propose vote rejected", err: corecoordinator.ErrProposeVote, code: codes.FailedPrecondition},
		{name: "precommit vote rejected", err: corecoordinator.ErrPrecommitVote, code: codes.FailedPrecondition},
		{name: "internal", err: errors.New("journal unavailable"), code: codes.Internal},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.code, status.Code(coordinatorErrorToStatus(test.err)))
		})
	}
}
