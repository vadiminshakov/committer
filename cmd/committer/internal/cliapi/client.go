package cliapi

import (
	"context"
	"fmt"

	"github.com/vadiminshakov/committer/v2/cmd/committer/internal/cliapi/pb"
	"github.com/vadiminshakov/committer/v2/core/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Client calls a node's CLI API.
type Client struct {
	rpc  pb.CLIClient
	conn *grpc.ClientConn
}

// Dial connects lazily to a node started with -cli.
func Dial(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}

	return &Client{rpc: pb.NewCLIClient(conn), conn: conn}, nil
}

// Commit runs one transaction on a coordinator and returns its height. An
// error wrapping coordinator.ErrAborted means durably aborted; other failures are gRPC
// status errors, and a transport error or deadline leaves the outcome unknown.
func (c *Client) Commit(ctx context.Context, key string, value []byte) (uint64, error) {
	resp, err := c.rpc.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	if err != nil {
		switch status.Code(err) {
		case codes.Aborted:
			return 0, fmt.Errorf("%w: %s", coordinator.ErrAborted, status.Convert(err).Message())
		case codes.InvalidArgument:
			return 0, fmt.Errorf("%w: %s", coordinator.ErrInvalidTransaction, status.Convert(err).Message())
		default:
			return 0, err //nolint:wrapcheck // callers inspect the gRPC status
		}
	}

	return resp.GetHeight(), nil
}

// Get reads a committed value from a cohort.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.rpc.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, err //nolint:wrapcheck // callers inspect the gRPC status
	}

	return resp.GetValue(), nil
}

// Close releases the connection.
func (c *Client) Close() error {
	return c.conn.Close() //nolint:wrapcheck // nothing to add
}
