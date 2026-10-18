package server

import (
	"context"
	"net"
	"slices"
	"strings"

	"github.com/vadiminshakov/committer/v2/io/gateway/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// internalCommitAPIPrefix prefixes the full method names of the internal
// commit API.
var internalCommitAPIPrefix = "/" + proto.InternalCommitAPI_ServiceDesc.ServiceName + "/"

// coordinatorCheck restricts internal commit API RPCs to the configured
// coordinator's host.
func (s *Server) coordinatorCheck(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if s.coordinatorAddr == "" || !strings.HasPrefix(info.FullMethod, internalCommitAPIPrefix) {
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

	coordHost, _, err := net.SplitHostPort(s.coordinatorAddr)
	if err != nil {
		coordHost = s.coordinatorAddr
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
