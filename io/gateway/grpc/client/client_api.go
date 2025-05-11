package client

import (
	"context"

	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ClientAPIClient provides access to the client API
type ClientAPIClient struct {
	Connection proto.ClientAPIClient
}

// NewClientAPI creates an instance of the client API client.
// 'addr' - the network address of the coordinator (host + port).
func NewClientAPI(addr string) (*ClientAPIClient, error) {
	conn, err := createConnection(addr)
	if err != nil {
		return nil, err
	}
	return &ClientAPIClient{Connection: proto.NewClientAPIClient(conn)}, nil
}

// Put sends a put request to the client API
func (client *ClientAPIClient) Put(ctx context.Context, key string, value []byte) (*proto.Response, error) {
	return client.Connection.Put(ctx, &proto.Entry{Key: key, Value: value})
}

// NodeInfo gets the current height of the node.
func (client *ClientAPIClient) NodeInfo(ctx context.Context) (*proto.Info, error) {
	return client.Connection.NodeInfo(ctx, &emptypb.Empty{})
}

// Get gets the value by the specified key
func (client *ClientAPIClient) Get(ctx context.Context, key string) (*proto.Value, error) {
	return client.Connection.Get(ctx, &proto.Msg{Key: key})
}
