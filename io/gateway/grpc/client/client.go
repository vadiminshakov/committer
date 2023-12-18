package client

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/openzipkin/zipkin-go"
	zipkingrpc "github.com/openzipkin/zipkin-go/middleware/grpc"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

type CommitClient struct {
	Connection proto.CommitClient
	Tracer     *zipkin.Tracer
}

// New creates instance of peer client.
// 'addr' is a coordinator network address (host + port).
func New(addr string, tracer *zipkin.Tracer) (*CommitClient, error) {
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
	if tracer != nil {
		conn, err = grpc.DialContext(ctx, addr, grpc.WithConnectParams(connParams), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(zipkingrpc.NewClientHandler(tracer)))
	} else {
		conn, err = grpc.DialContext(ctx, addr, grpc.WithConnectParams(connParams), grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}
	return &CommitClient{Connection: proto.NewCommitClient(conn), Tracer: tracer}, nil
}

func (client *CommitClient) Propose(ctx context.Context, req *proto.ProposeRequest) (*proto.Response, error) {
	var span zipkin.Span
	if client.Tracer != nil {
		span, ctx = client.Tracer.StartSpanFromContext(ctx, "Propose")
		defer span.Finish()
	}
	return client.Connection.Propose(ctx, req)
}

func (client *CommitClient) Precommit(ctx context.Context, req *proto.PrecommitRequest) (*proto.Response, error) {
	var span zipkin.Span
	if client.Tracer != nil {
		span, ctx = client.Tracer.StartSpanFromContext(ctx, "Precommit")
		defer span.Finish()
	}
	return client.Connection.Precommit(ctx, req)
}

func (client *CommitClient) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.Response, error) {
	var span zipkin.Span
	if client.Tracer != nil {
		span, ctx = client.Tracer.StartSpanFromContext(ctx, "Commit")
		defer span.Finish()
	}
	return client.Connection.Commit(ctx, req)
}

// Put sends key/value pair to peer (it should be a coordinator).
// The coordinator reaches consensus and all peers commit the value.
func (client *CommitClient) Put(ctx context.Context, key string, value []byte) (*proto.Response, error) {
	var span zipkin.Span
	if client.Tracer != nil {
		span, ctx = client.Tracer.StartSpanFromContext(ctx, "Put")
		defer span.Finish()
	}
	return client.Connection.Put(ctx, &proto.Entry{Key: key, Value: value})
}

// NodeInfo gets info about current node height.
func (client *CommitClient) NodeInfo(ctx context.Context) (*proto.Info, error) {
	var span zipkin.Span
	if client.Tracer != nil {
		span, ctx = client.Tracer.StartSpanFromContext(ctx, "NodeInfo")
		defer span.Finish()
	}
	return client.Connection.NodeInfo(ctx, &empty.Empty{})
}

// Get queries value of specific key
func (client *CommitClient) Get(ctx context.Context, key string) (*proto.Value, error) {
	var span zipkin.Span
	if client.Tracer != nil {
		span, ctx = client.Tracer.StartSpanFromContext(ctx, "Get")
		defer span.Finish()
	}
	return client.Connection.Get(ctx, &proto.Msg{Key: key})
}
