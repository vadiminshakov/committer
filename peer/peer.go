package peer

import (
	"context"
	"github.com/pkg/errors"
	pb "github.com/vadiminshakov/committer/proto"
	"google.golang.org/grpc"
)

type CommitClient struct {
	Connection pb.CommitClient
}

// New creates instance of peer client.
// 'addr' is a coordinator network address (host + port).
func New(addr string) (*CommitClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}
	return &CommitClient{Connection: pb.NewCommitClient(conn)}, nil
}

func (client *CommitClient) Propose(ctx context.Context, req *pb.ProposeRequest) (*pb.Response, error) {
	return client.Connection.Propose(ctx, req)
}

func (client *CommitClient) Precommit(ctx context.Context, req *pb.PrecommitRequest) (*pb.Response, error) {
	return client.Connection.Precommit(ctx, req)
}

func (client *CommitClient) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.Response, error) {
	return client.Connection.Commit(ctx, req)
}

// Put sends key/value pair to peer (it should be a coordinator).
// The coordinator reaches consensus and all peers commit the value.
func (client *CommitClient) Put(ctx context.Context, key string, value []byte) (*pb.Response, error) {
	return client.Connection.Put(ctx, &pb.Entry{Key: key, Value: value})
}

// Get queries value of specific key
func (client *CommitClient) Get(ctx context.Context, key string) (*pb.Value, error) {
	return client.Connection.Get(ctx, &pb.Msg{Key: key})
}
