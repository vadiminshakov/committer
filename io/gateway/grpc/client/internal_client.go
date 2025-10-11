package client

import (
	"context"

	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
)

// InternalCommitClient provides access to the internal API of the atomic commit protocol
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

// Abort sends an abort request to the internal commit API
func (client *InternalCommitClient) Abort(ctx context.Context, req *dto.AbortRequest) (*proto.Response, error) {
	protoReq := &proto.AbortRequest{
		Height: req.Height,
		Reason: req.Reason,
	}

	log.Infof("Sending abort request for height %d with reason: %s", req.Height, req.Reason)
	return client.Connection.Abort(ctx, protoReq)
}
