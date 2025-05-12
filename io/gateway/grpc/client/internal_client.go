package client

import (
	"context"

	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
)

// InternalCommitClient provides access to the internal API of the consensus protocol
type InternalCommitClient struct {
	Connection proto.InternalCommitAPIClient
}

// NewInternalClient creates an instance of the internal API client.
// 'addr' - the network address of the node (host + port).
func NewInternalClient(addr string) (*InternalCommitClient, error) {
	conn, err := createConnection(addr)
	if err != nil {
		return nil, err
	}
	return &InternalCommitClient{Connection: proto.NewInternalCommitAPIClient(conn)}, nil
}

// Propose sends a propose request to the internal commit API
func (client *InternalCommitClient) Propose(ctx context.Context, req *proto.ProposeRequest) (*proto.Response, error) {
	return client.Connection.Propose(ctx, req)
}

// Precommit sends a precommit request to the internal commit API
func (client *InternalCommitClient) Precommit(ctx context.Context, req *proto.PrecommitRequest) (*proto.Response, error) {
	return client.Connection.Precommit(ctx, req)
}

// Commit sends a commit request to the internal commit API
func (client *InternalCommitClient) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.Response, error) {
	return client.Connection.Commit(ctx, req)
}
