package server

import (
	"context"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// CoordinatorCheck intercepts InternalCommitAPI RPCs and restricts them to the configured coordinator.
// ClientAPI methods (Get, NodeInfo) are not affected.
func CoordinatorCheck(ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {

	if !strings.HasPrefix(info.FullMethod, "/schema.InternalCommitAPI/") {
		return handler(ctx, req)
	}

	serv := info.Server.(*Server)
	if serv.Config.Coordinator == "" {
		return handler(ctx, req)
	}

	peerinfo, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Internal, "failed to retrieve peer info")
	}

	peerHost, _, err := net.SplitHostPort(peerinfo.Addr.String())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse peer address: %v", err)
	}

	coordHost, _, err := net.SplitHostPort(serv.Config.Coordinator)
	if err != nil {
		coordHost = serv.Config.Coordinator
	}

	if peerHost == coordHost {
		return handler(ctx, req)
	}

	ips, err := net.LookupHost(coordHost)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "host %s is not the coordinator", peerHost)
	}

	for _, ip := range ips {
		if peerHost == ip {
			return handler(ctx, req)
		}
	}

	return nil, status.Errorf(codes.PermissionDenied, "host %s is not the coordinator", peerHost)
}
