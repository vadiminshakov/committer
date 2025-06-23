package server

import (
	"context"
	"net"

	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	status "google.golang.org/grpc/status"
)

// WhiteListChecker intercepts RPC and checks that the caller is whitelisted.
func WhiteListChecker(ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {
	peerinfo, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Internal, "failed to retrieve peer info")
	}

	host, _, err := net.SplitHostPort(peerinfo.Addr.String())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	serv := info.Server.(*Server)
	if !includes(serv.Config.Whitelist, host) {
		return nil, status.Errorf(codes.PermissionDenied, "host %s is not in whitelist", host)
	}

	// Calls the handler
	h, err := handler(ctx, req)

	return h, err
}

// includes checks that the 'arr' includes 'value'
func includes(arr []string, value string) bool {
	for i := range arr {
		if arr[i] == value {
			return true
		}
	}
	return false
}