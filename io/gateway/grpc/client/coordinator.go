package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/vadiminshakov/committer/v2/core/dto"
	"github.com/vadiminshakov/committer/v2/io/gateway/grpc/proto"
	"google.golang.org/grpc"
)

type CoordinatorClient struct {
	conn      *grpc.ClientConn
	rpc       proto.InternalCommitAPIClient
	closeOnce sync.Once
	closeErr  error
}

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

func (client *CoordinatorClient) Close() error {
	client.closeOnce.Do(func() {
		if client.conn != nil {
			client.closeErr = client.conn.Close()
		}
	})

	return client.closeErr
}
