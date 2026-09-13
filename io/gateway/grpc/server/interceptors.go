package server

import (
	"context"
	"net"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// CoordinatorCheck intercepts InternalCommitAPI RPCs and restricts them to the configured coordinator.
// ClientAPI methods (Get, NodeInfo) are not affected.
func CoordinatorCheck(ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (any, error) {
	if !strings.HasPrefix(info.FullMethod, "/schema.InternalCommitAPI/") {
		return handler(ctx, req)
	}

	serv, valid := info.Server.(*Server)

	if !valid {
		return nil, status.Errorf(codes.Internal, "unexpected server type %T", info.Server)
	}

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

	if slices.Contains(ips, peerHost) {
		return handler(ctx, req)
	}

	return nil, status.Errorf(codes.PermissionDenied, "host %s is not the coordinator", peerHost)
}
