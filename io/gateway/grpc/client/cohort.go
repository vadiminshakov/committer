package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/vadiminshakov/committer/v2/core/dto"
	"github.com/vadiminshakov/committer/v2/io/gateway/grpc/proto"
	"google.golang.org/grpc"
)

type CohortClient struct {
	addr      string
	conn      *grpc.ClientConn
	rpc       proto.InternalCommitAPIClient
	closeOnce sync.Once
	closeErr  error
}

const coordinatorAbortReason = "coordinator decided abort"

const (
	commitOperation = "commit"
	abortOperation  = "abort"
)

func NewCohortClient(addr string) (*CohortClient, error) {
	conn, err := createConnection(addr)
	if err != nil {
		return nil, fmt.Errorf("connect cohort %q: %w", addr, err)
	}

	return &CohortClient{
		addr: addr,
		conn: conn,
		rpc:  proto.NewInternalCommitAPIClient(conn),
	}, nil
}

func (client *CohortClient) Addr() string {
	return client.addr
}

func (client *CohortClient) Propose(ctx context.Context, proposal dto.Proposal) (dto.ParticipantReply, error) {
	commitType, err := commitTypeToProto(proposal.Protocol)
	if err != nil {
		return dto.ParticipantReply{}, err
	}

	resp, err := client.rpc.Propose(ctx, &proto.ProposeRequest{
		Key:        proposal.Transaction.Key,
		Value:      append([]byte(nil), proposal.Transaction.Value...),
		CommitType: commitType,
		Index:      proposal.Height,
	})
	if err != nil {
		return dto.ParticipantReply{}, participantRPCError("propose", proposal.Height, err)
	}

	return participantReplyFromProto(resp)
}

func (client *CohortClient) Precommit(ctx context.Context, height uint64) (dto.ParticipantReply, error) {
	resp, err := client.rpc.Precommit(ctx, &proto.PrecommitRequest{Index: height})
	if err != nil {
		return dto.ParticipantReply{}, participantRPCError("precommit", height, err)
	}

	return participantReplyFromProto(resp)
}

func (client *CohortClient) ApplyFinalDecision(
	ctx context.Context,
	decision dto.FinalDecision,
) (dto.ParticipantReply, error) {
	var (
		resp      *proto.Response
		err       error
		operation string
	)

	switch decision.Outcome {
	case dto.OutcomeCommit:
		operation = commitOperation
		resp, err = client.rpc.Commit(ctx, &proto.CommitRequest{Index: decision.Height})
	case dto.OutcomeAbort:
		operation = abortOperation
		resp, err = client.rpc.Abort(ctx, &proto.AbortRequest{
			Height: decision.Height,
			Reason: coordinatorAbortReason,
		})
	default:
		return dto.ParticipantReply{}, fmt.Errorf(
			"deliver decision at height %d: outcome %d is not final",
			decision.Height,
			decision.Outcome,
		)
	}

	if err != nil {
		return dto.ParticipantReply{}, participantRPCError(operation, decision.Height, err)
	}

	return participantReplyFromProto(resp)
}

func (client *CohortClient) Close() error {
	client.closeOnce.Do(func() {
		if client.conn != nil {
			client.closeErr = client.conn.Close()
		}
	})

	return client.closeErr
}

func commitTypeToProto(protocol dto.Protocol) (proto.CommitType, error) {
	switch protocol {
	case dto.ProtocolTwoPhase:
		return proto.CommitType_TWO_PHASE_COMMIT, nil
	case dto.ProtocolThreePhase:
		return proto.CommitType_THREE_PHASE_COMMIT, nil
	default:
		return proto.CommitType_TWO_PHASE_COMMIT, fmt.Errorf("unsupported participant protocol %d", protocol)
	}
}

func participantReplyFromProto(resp *proto.Response) (dto.ParticipantReply, error) {
	if resp == nil {
		return dto.ParticipantReply{}, errors.New("participant protocol returned an empty response")
	}

	return dto.ParticipantReply{
		Accepted: resp.Type == proto.Type_ACK,
		Height:   resp.Index,
		Reason:   resp.Reason,
	}, nil
}

func participantRPCError(operation string, height uint64, err error) error {
	return fmt.Errorf("participant %s at height %d: %w", operation, height, err)
}
