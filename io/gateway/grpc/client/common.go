package client

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// createConnection creates a gRPC connection to the specified address
func createConnection(addr string) (*grpc.ClientConn, error) {
	connParams := grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay: 100 * time.Millisecond,
			MaxDelay:  10 * time.Second,
		},
		MinConnectTimeout: 200 * time.Millisecond,
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	conn, err := grpc.DialContext(ctx, addr, grpc.WithConnectParams(connParams), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}

	return conn, nil
}
