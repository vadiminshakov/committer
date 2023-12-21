package client

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

type CommitClient struct {
	Connection proto.CommitClient
}

// New creates instance of peer client.
// 'addr' is a coordinator network address (host + port).
func New(addr string) (*CommitClient, error) {
	var (
		conn *grpc.ClientConn
		err  error
	)

	connParams := grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay: 100 * time.Millisecond,
			MaxDelay:  10 * time.Second,
		},
		MinConnectTimeout: 200 * time.Millisecond,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	conn, err = grpc.DialContext(ctx, addr, grpc.WithConnectParams(connParams), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}
	return &CommitClient{Connection: proto.NewCommitClient(conn)}, nil
}

func (client *CommitClient) Propose(ctx context.Context, req *proto.ProposeRequest) (*proto.Response, error) {
	return client.Connection.Propose(ctx, req)
}

func (client *CommitClient) Precommit(ctx context.Context, req *proto.PrecommitRequest) (*proto.Response, error) {
	return client.Connection.Precommit(ctx, req)
}

func (client *CommitClient) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.Response, error) {
	return client.Connection.Commit(ctx, req)
}

// Put sends key/value pair to peer (it should be a coordinator).
// The coordinator reaches consensus and all peers commit the value.
func (client *CommitClient) Put(ctx context.Context, key string, value []byte) (*proto.Response, error) {
	return client.Connection.Put(ctx, &proto.Entry{Key: key, Value: value})
}

// NodeInfo gets info about current node height.
func (client *CommitClient) NodeInfo(ctx context.Context) (*proto.Info, error) {
	return client.Connection.NodeInfo(ctx, &empty.Empty{})
}

// Get queries value of specific key
func (client *CommitClient) Get(ctx context.Context, key string) (*proto.Value, error) {
	return client.Connection.Get(ctx, &proto.Msg{Key: key})
}
