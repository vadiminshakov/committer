// Package client provides gRPC client implementations for communicating with committer nodes.
//
// This package contains both internal client for node-to-node communication
// and external client API for application integration.
package client

import (
	"context"
	"fmt"

	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ClientAPIClient provides access to the client API.
type ClientAPIClient struct {
	rpc  proto.ClientAPIClient
	conn *grpc.ClientConn
}

// NewClientAPI creates an instance of the client API client.
// The addr parameter should be the network address of the coordinator (host + port).
func NewClientAPI(addr string) (*ClientAPIClient, error) {
	conn, err := createConnection(addr)
	if err != nil {
		return nil, err
	}

	return &ClientAPIClient{rpc: proto.NewClientAPIClient(conn), conn: conn}, nil
}

// Put sends a put request to the client API.
func (client *ClientAPIClient) Put(ctx context.Context, key string, value []byte) (*proto.Response, error) {
	resp, err := client.rpc.Put(ctx, &proto.Entry{Key: key, Value: value})
	if err != nil {
		return nil, fmt.Errorf("put key %q: %w", key, err)
	}

	return resp, nil
}

// NodeInfo gets the current height of the node.
func (client *ClientAPIClient) NodeInfo(ctx context.Context) (*proto.Info, error) {
	resp, err := client.rpc.NodeInfo(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("get node info: %w", err)
	}

	return resp, nil
}

// Get gets the value by the specified key.
func (client *ClientAPIClient) Get(ctx context.Context, key string) (*proto.Value, error) {
	resp, err := client.rpc.Get(ctx, &proto.Msg{Key: key})
	if err != nil {
		return nil, fmt.Errorf("get key %q: %w", key, err)
	}

	return resp, nil
}

// Close releases the client connection.
func (client *ClientAPIClient) Close() error {
	if client.conn == nil {
		return nil
	}

	return client.conn.Close()
}
