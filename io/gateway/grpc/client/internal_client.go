package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"google.golang.org/grpc"
)

const coordinatorAbortReason = "coordinator decided abort"

// CohortClient is the production coordinator-to-cohort client. It
// translates the coordinator domain protocol to protobuf and owns its gRPC
// connection.
type CohortClient struct {
	addr      string
	conn      *grpc.ClientConn
	rpc       proto.InternalCommitAPIClient
	closeOnce sync.Once
	closeErr  error
}

// NewCohortClient connects a cohort participant. The returned client
// must be closed by its owner.
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

// Addr returns the cohort network address used by this client.
func (client *CohortClient) Addr() string {
	return client.addr
}

// Propose asks the cohort to vote on a transaction at a coordinator-assigned
// height.
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

// Precommit asks a 3PC cohort to enter its committable phase.
func (client *CohortClient) Precommit(ctx context.Context, height uint64) (dto.ParticipantReply, error) {
	resp, err := client.rpc.Precommit(ctx, &proto.PrecommitRequest{Index: height})
	if err != nil {
		return dto.ParticipantReply{}, participantRPCError("precommit", height, err)
	}
	return participantReplyFromProto(resp)
}

// ApplyFinalDecision delivers a durable final decision through the ordinary protocol RPC.
// A recovered 3PC decision is preceded by PRECOMMIT in cohort delivery; the
// adapter never bypasses the participant FSM.
func (client *CohortClient) ApplyFinalDecision(ctx context.Context, decision dto.FinalDecision) (dto.ParticipantReply, error) {
	var (
		resp *proto.Response
		err  error
		op   string
	)

	switch decision.Outcome {
	case dto.OutcomeCommit:
		op = "commit"
		resp, err = client.rpc.Commit(ctx, &proto.CommitRequest{Index: decision.Height})
	case dto.OutcomeAbort:
		op = "abort"
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
		return dto.ParticipantReply{}, participantRPCError(op, decision.Height, err)
	}
	return participantReplyFromProto(resp)
}

// Close releases the cohort's gRPC connection. It is safe to call
// repeatedly.
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
		return dto.ParticipantReply{}, fmt.Errorf("participant protocol returned an empty response")
	}
	return dto.ParticipantReply{
		Accepted: resp.Type == proto.Type_ACK,
		Height:   resp.Index,
	}, nil
}

func participantRPCError(operation string, height uint64, err error) error {
	return fmt.Errorf("participant %s at height %d: %w", operation, height, err)
}

// CoordinatorClient is the cohort-side adapter used to ask a coordinator for a
// durable transaction outcome. It owns the underlying gRPC connection.
type CoordinatorClient struct {
	conn      *grpc.ClientConn
	rpc       proto.InternalCommitAPIClient
	closeOnce sync.Once
	closeErr  error
}

// NewCoordinatorClient connects the cohort-side termination protocol to a
// coordinator. The returned adapter must be closed by its owner.
func NewCoordinatorClient(addr string) (*CoordinatorClient, error) {
	conn, err := createConnection(addr)
	if err != nil {
		return nil, fmt.Errorf("connect coordinator %q: %w", addr, err)
	}

	return &CoordinatorClient{
		conn: conn,
		rpc:  proto.NewInternalCommitAPIClient(conn),
	}, nil
}

// Decision returns the coordinator's recorded outcome for height.
func (client *CoordinatorClient) Decision(ctx context.Context, height uint64) (dto.Outcome, error) {
	resp, err := client.rpc.Decision(ctx, &proto.DecisionRequest{Height: height})
	if err != nil {
		return dto.OutcomeUnknown, fmt.Errorf("request decision at height %d: %w", height, err)
	}
	if resp == nil {
		return dto.OutcomeUnknown, fmt.Errorf("request decision at height %d: empty response", height)
	}

	switch resp.Outcome {
	case proto.Outcome_OUTCOME_COMMIT:
		return dto.OutcomeCommit, nil
	case proto.Outcome_OUTCOME_ABORT:
		return dto.OutcomeAbort, nil
	default:
		return dto.OutcomeUnknown, nil
	}
}

// Close releases the gRPC connection. It is safe to call repeatedly.
func (client *CoordinatorClient) Close() error {
	client.closeOnce.Do(func() {
		if client.conn != nil {
			client.closeErr = client.conn.Close()
		}
	})
	return client.closeErr
}
