package client

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// gRPC dial timing.
const (
	dialBaseDelay         = 100 * time.Millisecond
	dialMaxDelay          = 10 * time.Second
	dialMinConnectTimeout = 200 * time.Millisecond
	dialTimeout           = 10 * time.Second
)

func createConnection(addr string) (*grpc.ClientConn, error) {
	connParams := grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay: dialBaseDelay,
			MaxDelay:  dialMaxDelay,
		},
		MinConnectTimeout: dialMinConnectTimeout,
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithConnectParams(connParams),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}

	return conn, nil
}
